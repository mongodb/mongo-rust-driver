use std::ptr;
use windows_sys::Win32::{
    Foundation::{SEC_E_OK, SEC_I_CONTINUE_NEEDED},
    Security::{
        Authentication::Identity::{
            AcquireCredentialsHandleW,
            DecryptMessage,
            DeleteSecurityContext,
            EncryptMessage,
            FreeCredentialsHandle,
            InitializeSecurityContextW,
            QueryContextAttributesW,
            SecBuffer,
            SecBufferDesc,
            SecPkgContext_Sizes,
            ISC_REQ_ALLOCATE_MEMORY,
            ISC_REQ_MUTUAL_AUTH,
            SECBUFFER_DATA,
            SECBUFFER_PADDING,
            SECBUFFER_STREAM,
            SECBUFFER_TOKEN,
            SECBUFFER_VERSION,
            SECPKG_ATTR_SIZES,
            SECPKG_CRED_OUTBOUND,
            SECQOP_WRAP_NO_ENCRYPT,
            SECURITY_NETWORK_DREP,
        },
        Credentials::SecHandle,
    },
    System::Rpc::{SEC_WINNT_AUTH_IDENTITY_UNICODE, SEC_WINNT_AUTH_IDENTITY_W},
};

use crate::{
    client::{
        auth::{
            sasl::{SaslContinue, SaslResponse, SaslStart},
            Credential,
            GSSAPI_STR,
        },
        options::ServerApi,
    },
    cmap::Connection,
    error::{Error, Result},
};

pub(super) async fn authenticate_stream(
    conn: &mut Connection,
    credential: &Credential,
    server_api: Option<&ServerApi>,
    service_principal: String,
    source: &str,
) -> Result<()> {
    let user_principal = credential.username.clone();
    let password = credential.password.clone();
    let (mut authenticator, initial_token) =
        SspiAuthenticator::init(user_principal, password, service_principal).await?;

    let command = SaslStart::new(
        source.to_string(),
        crate::client::auth::AuthMechanism::Gssapi,
        initial_token,
        server_api.cloned(),
    )
    .into_command()?;

    let response_doc = conn.send_message(command).await?;
    let sasl_response =
        SaslResponse::parse(GSSAPI_STR, response_doc.auth_response_body(GSSAPI_STR)?)?;

    let mut conversation_id = Some(sasl_response.conversation_id);
    let mut payload = sasl_response.payload;

    for _ in 0..10 {
        let challenge = payload.as_slice();
        let output_token = authenticator.step(challenge).await?;

        let token = output_token.unwrap_or(vec![]);
        let command = SaslContinue::new(
            source.to_string(),
            conversation_id.clone().unwrap(),
            token,
            server_api.cloned(),
        )
        .into_command();

        let response_doc = conn.send_message(command).await?;
        let sasl_response =
            SaslResponse::parse(GSSAPI_STR, response_doc.auth_response_body(GSSAPI_STR)?)?;

        conversation_id = Some(sasl_response.conversation_id);
        payload = sasl_response.payload;

        if sasl_response.done {
            return Ok(());
        }

        if authenticator.is_complete() {
            break;
        }
    }

    let output_token = authenticator.do_unwrap_wrap(payload.as_slice())?;
    let command = SaslContinue::new(
        source.to_string(),
        conversation_id.unwrap(),
        output_token,
        server_api.cloned(),
    )
    .into_command();

    let response_doc = conn.send_message(command).await?;
    let sasl_response =
        SaslResponse::parse(GSSAPI_STR, response_doc.auth_response_body(GSSAPI_STR)?)?;

    if sasl_response.done {
        Ok(())
    } else {
        Err(Error::authentication_error(
            GSSAPI_STR,
            "GSSAPI authentication failed after 10 attempts",
        ))
    }
}

struct SspiAuthenticator {
    cred_handle: Option<SecHandle>,
    ctx_handle: Option<SecHandle>,
    have_cred: bool,
    have_context: bool,
    auth_complete: bool,
    name_token: Vec<u16>,
    user_plus_realm: String,
}

impl SspiAuthenticator {
    async fn init(
        user_principal: Option<String>,
        password: Option<String>,
        service_principal: String,
    ) -> Result<(Self, Vec<u8>)> {
        let user_plus_realm = user_principal.clone().unwrap_or_default();

        let name_token_string = service_principal.clone();
        let name_token: Vec<u16> = name_token_string
            .encode_utf16()
            .chain(std::iter::once(0))
            .collect();

        let mut authenticator = Self {
            cred_handle: None,
            ctx_handle: None,
            have_cred: false,
            have_context: false,
            auth_complete: false,
            name_token,
            user_plus_realm,
        };

        let initial_token = authenticator
            .acquire_credentials_and_init(user_principal, password)
            .await?;
        Ok((authenticator, initial_token))
    }

    async fn acquire_credentials_and_init(
        &mut self,
        user_principal: Option<String>,
        password: Option<String>,
    ) -> Result<Vec<u8>> {
        unsafe {
            let mut cred_handle = SecHandle::default();
            let mut expiry: i64 = 0;

            let mut auth_identity = SEC_WINNT_AUTH_IDENTITY_W::default();
            auth_identity.Flags = SEC_WINNT_AUTH_IDENTITY_UNICODE;

            let username_wide: Vec<u16>;
            let domain_wide: Vec<u16>;
            let password_wide: Vec<u16>;

            if let Some(user_principal) = &user_principal {
                if let Some(at_pos) = user_principal.find('@') {
                    let username = &user_principal[..at_pos];
                    let domain = &user_principal[at_pos + 1..];

                    username_wide = username.encode_utf16().chain(std::iter::once(0)).collect();
                    domain_wide = domain.encode_utf16().chain(std::iter::once(0)).collect();

                    auth_identity.User = username_wide.as_ptr() as *mut u16;
                    auth_identity.UserLength = username.len() as u32;
                    auth_identity.Domain = domain_wide.as_ptr() as *mut u16;
                    auth_identity.DomainLength = domain.len() as u32;

                    if let Some(password) = &password {
                        password_wide = password.encode_utf16().chain(std::iter::once(0)).collect();
                        auth_identity.Password = password_wide.as_ptr() as *mut u16;
                        auth_identity.PasswordLength = password.len() as u32;
                    }
                }
            }

            let package_name: Vec<u16> = "kerberos\0".encode_utf16().collect();
            let result = AcquireCredentialsHandleW(
                std::ptr::null(),
                package_name.as_ptr(),
                SECPKG_CRED_OUTBOUND,
                ptr::null_mut(),
                if password.is_some() {
                    &auth_identity as *const _ as *const _
                } else {
                    ptr::null()
                },
                None,
                ptr::null_mut(),
                &mut cred_handle,
                &mut expiry,
            );

            if result != SEC_E_OK {
                return Err(Error::authentication_error(
                    GSSAPI_STR,
                    &format!("Failed to acquire credentials handle: {:?}", result),
                ));
            }

            self.cred_handle = Some(cred_handle);
            self.have_cred = true;

            let initial_token = self.initialize_security_context(&[])?;
            Ok(initial_token)
        }
    }

    async fn step(&mut self, challenge: &[u8]) -> Result<Option<Vec<u8>>> {
        if self.auth_complete {
            return Ok(None);
        }

        let token = self.initialize_security_context(challenge)?;
        Ok(Some(token))
    }

    fn initialize_security_context(&mut self, input_token: &[u8]) -> Result<Vec<u8>> {
        unsafe {
            let mut ctx_handle = if self.have_context {
                self.ctx_handle.unwrap()
            } else {
                SecHandle::default()
            };

            let mut input_buffer = SecBuffer {
                cbBuffer: input_token.len() as u32,
                BufferType: SECBUFFER_TOKEN,
                pvBuffer: if input_token.is_empty() {
                    ptr::null_mut()
                } else {
                    input_token.as_ptr() as *mut _
                },
            };

            let input_buffer_desc = SecBufferDesc {
                ulVersion: SECBUFFER_VERSION,
                cBuffers: 1,
                pBuffers: &mut input_buffer,
            };

            let mut output_buffer = SecBuffer {
                cbBuffer: 0,
                BufferType: SECBUFFER_TOKEN,
                pvBuffer: ptr::null_mut(),
            };

            let mut output_buffer_desc = SecBufferDesc {
                ulVersion: SECBUFFER_VERSION,
                cBuffers: 1,
                pBuffers: &mut output_buffer,
            };

            let mut context_attr = 0u32;

            let result = InitializeSecurityContextW(
                &self.cred_handle.unwrap(),
                if self.have_context {
                    &ctx_handle
                } else {
                    ptr::null()
                },
                self.name_token.as_ptr(),
                ISC_REQ_ALLOCATE_MEMORY | ISC_REQ_MUTUAL_AUTH,
                0,
                SECURITY_NETWORK_DREP,
                if self.have_context {
                    &input_buffer_desc
                } else {
                    ptr::null()
                },
                0,
                &mut ctx_handle,
                &mut output_buffer_desc,
                &mut context_attr,
                ptr::null_mut(),
            );

            self.ctx_handle = Some(ctx_handle);
            self.have_context = true;

            match result {
                SEC_E_OK => {
                    self.auth_complete = true;
                }
                SEC_I_CONTINUE_NEEDED => {}
                _ => {
                    return Err(Error::authentication_error(
                        GSSAPI_STR,
                        &format!("InitializeSecurityContext failed: {:?}", result),
                    ));
                }
            }

            let token = if output_buffer.pvBuffer.is_null() || output_buffer.cbBuffer == 0 {
                Vec::new()
            } else {
                let token_slice = std::slice::from_raw_parts(
                    output_buffer.pvBuffer as *const u8,
                    output_buffer.cbBuffer as usize,
                );
                token_slice.to_vec()
            };

            Ok(token)
        }
    }

    fn do_unwrap_wrap(&mut self, payload: &[u8]) -> Result<Vec<u8>> {
        unsafe {
            let mut message = payload.to_vec();

            let mut wrap_bufs = [
                SecBuffer {
                    cbBuffer: message.len() as u32,
                    BufferType: SECBUFFER_STREAM,
                    pvBuffer: message.as_mut_ptr() as *mut _,
                },
                SecBuffer {
                    cbBuffer: 0,
                    BufferType: SECBUFFER_DATA,
                    pvBuffer: ptr::null_mut(),
                },
            ];

            let mut wrap_buf_desc = SecBufferDesc {
                ulVersion: SECBUFFER_VERSION,
                cBuffers: 2,
                pBuffers: wrap_bufs.as_mut_ptr(),
            };

            let result = DecryptMessage(
                &self.ctx_handle.unwrap(),
                &mut wrap_buf_desc,
                0,
                ptr::null_mut(),
            );
            if result != SEC_E_OK {
                return Err(Error::authentication_error(
                    GSSAPI_STR,
                    &format!("DecryptMessage failed: {:?}", result),
                ));
            }

            if wrap_bufs[1].cbBuffer < 4 {
                return Err(Error::authentication_error(
                    GSSAPI_STR,
                    "Server message is too short",
                ));
            }

            let data_ptr = wrap_bufs[1].pvBuffer as *const u8;
            let first_byte = *data_ptr;
            if (first_byte & 1) == 0 {
                return Err(Error::authentication_error(
                    GSSAPI_STR,
                    "Server does not support the required security layer",
                ));
            }

            let mut sizes = SecPkgContext_Sizes::default();
            let result = QueryContextAttributesW(
                &self.ctx_handle.unwrap(),
                SECPKG_ATTR_SIZES,
                &mut sizes as *mut _ as *mut _,
            );
            if result != SEC_E_OK {
                return Err(Error::authentication_error(
                    GSSAPI_STR,
                    &format!("QueryContextAttributes failed: {:?}", result),
                ));
            }

            let user_plus_realm = &self.user_plus_realm;
            let plaintext_message_size = 4 + user_plus_realm.len();
            let total_size = sizes.cbSecurityTrailer as usize
                + plaintext_message_size
                + sizes.cbBlockSize as usize;
            let mut message_buf = vec![0u8; total_size];

            let plaintext_start = sizes.cbSecurityTrailer as usize;
            message_buf[plaintext_start] = 1;
            message_buf[plaintext_start + 1] = 0;
            message_buf[plaintext_start + 2] = 0;
            message_buf[plaintext_start + 3] = 0;
            message_buf[plaintext_start + 4..plaintext_start + 4 + user_plus_realm.len()]
                .copy_from_slice(user_plus_realm.as_bytes());

            let mut encrypt_bufs = [
                SecBuffer {
                    cbBuffer: sizes.cbSecurityTrailer,
                    BufferType: SECBUFFER_TOKEN,
                    pvBuffer: message_buf.as_mut_ptr() as *mut _,
                },
                SecBuffer {
                    cbBuffer: plaintext_message_size as u32,
                    BufferType: SECBUFFER_DATA,
                    pvBuffer: message_buf
                        .as_mut_ptr()
                        .add(sizes.cbSecurityTrailer as usize)
                        as *mut _,
                },
                SecBuffer {
                    cbBuffer: sizes.cbBlockSize,
                    BufferType: SECBUFFER_PADDING,
                    pvBuffer: message_buf
                        .as_mut_ptr()
                        .add(plaintext_start + plaintext_message_size)
                        as *mut _,
                },
            ];

            let mut encrypt_buf_desc = SecBufferDesc {
                ulVersion: SECBUFFER_VERSION,
                cBuffers: 3,
                pBuffers: encrypt_bufs.as_mut_ptr(),
            };

            let result = EncryptMessage(
                &self.ctx_handle.unwrap(),
                SECQOP_WRAP_NO_ENCRYPT,
                &mut encrypt_buf_desc,
                0,
            );
            if result != SEC_E_OK {
                return Err(Error::authentication_error(
                    GSSAPI_STR,
                    &format!("EncryptMessage failed: {:?}", result),
                ));
            }

            let total_len =
                encrypt_bufs[0].cbBuffer + encrypt_bufs[1].cbBuffer + encrypt_bufs[2].cbBuffer;
            let mut result_buf = Vec::with_capacity(total_len as usize);

            let buf0_slice = std::slice::from_raw_parts(
                encrypt_bufs[0].pvBuffer as *const u8,
                encrypt_bufs[0].cbBuffer as usize,
            );
            result_buf.extend_from_slice(buf0_slice);

            let buf1_slice = std::slice::from_raw_parts(
                encrypt_bufs[1].pvBuffer as *const u8,
                encrypt_bufs[1].cbBuffer as usize,
            );
            result_buf.extend_from_slice(buf1_slice);

            let buf2_slice = std::slice::from_raw_parts(
                encrypt_bufs[2].pvBuffer as *const u8,
                encrypt_bufs[2].cbBuffer as usize,
            );
            result_buf.extend_from_slice(buf2_slice);

            Ok(result_buf)
        }
    }

    fn is_complete(&self) -> bool {
        self.auth_complete
    }
}

impl Drop for SspiAuthenticator {
    fn drop(&mut self) {
        unsafe {
            if let Some(ctx) = &self.ctx_handle {
                let _ = DeleteSecurityContext(ctx);
            }
            if let Some(cred) = &self.cred_handle {
                let _ = FreeCredentialsHandle(cred);
            }
        }
    }
}
