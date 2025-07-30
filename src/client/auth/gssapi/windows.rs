use sspi::{
    AcquireCredentialsHandleResult, BufferType, ClientRequestFlags, CredentialsBuffers,
    DataRepresentation, InitializeSecurityContextResult, Kerberos, KerberosConfig, SecurityBuffer,
    SecurityStatus, Sspi, SspiImpl, Username,
};

use crate::{
    client::{
        auth::{
            sasl::{SaslContinue, SaslResponse, SaslStart},
            Credential, GSSAPI_STR,
        },
        options::ServerApi,
    },
    cmap::Connection,
    error::{Error, Result},
    options::ResolverConfig,
};

pub(super) async fn authenticate_stream(
    conn: &mut Connection,
    credential: &Credential,
    server_api: Option<&ServerApi>,
    service_principal: String,
    source: &str,
) -> Result<()> {
    // Configuration
    let kerberos_config = KerberosConfig::new("", "".to_string());
    let mut kerberos = Kerberos::new_client_from_config(kerberos_config).map_err(|e| {
        Error::authentication_error(
            GSSAPI_STR,
            &format!("Failed to create SSPI Kerberos client: {e}"),
        )
    })?;

    // Acquire Creds
    let mut acq_creds_handle_result = get_cred_handle(&mut kerberos, credential)?;

    let mut conversation_id = None;
    let mut input_token: Vec<u8> = vec![];

    loop {
        let (output_token, status) = step(
            &mut kerberos,
            &mut acq_creds_handle_result.credentials_handle,
            input_token.as_slice(),
            service_principal.clone(),
        )?;
        if status == SecurityStatus::ContinueNeeded || status == SecurityStatus::Ok {
            let command = if conversation_id.is_none() {
                SaslStart::new(
                    source.to_string(),
                    crate::client::auth::AuthMechanism::Gssapi,
                    output_token,
                    server_api.cloned(),
                )
                .into_command()?
            } else {
                SaslContinue::new(
                    source.to_string(),
                    conversation_id.clone().unwrap(),
                    output_token,
                    server_api.cloned(),
                )
                .into_command()
            };

            let response_doc = conn.send_message(command).await?;
            let sasl_response =
                SaslResponse::parse(GSSAPI_STR, response_doc.auth_response_body(GSSAPI_STR)?)?;

            conversation_id = Some(sasl_response.conversation_id);
            input_token = sasl_response.payload;

            if sasl_response.done {
                return Ok(());
            }
        } else {
            return Err(Error::authentication_error(
                GSSAPI_STR,
                &format!("problem authenticating; status = {status:?}"),
            ));
        }
    }
}

fn get_cred_handle(
    kerberos: &mut Kerberos,
    credential: &Credential,
) -> Result<AcquireCredentialsHandleResult<Option<CredentialsBuffers>>> {
    let mut acq_creds_handle_result = kerberos
        .acquire_credentials_handle()
        .with_credential_use(sspi::CredentialUse::Outbound);

    let mut auth_data: Option<sspi::Credentials> = None;
    if let Some(pwd) = credential.password.clone() {
        if let Some(username) = credential.username.as_ref() {
            let identity = sspi::AuthIdentity {
                username: Username::parse(username).map_err(|e| {
                    Error::authentication_error(
                        GSSAPI_STR,
                        &format!("Failed to parse user principal: {e}"),
                    )
                })?,
                password: pwd.into(),
            };

            auth_data = Some(identity.into());
        } else {
            return Err(Error::authentication_error(
                GSSAPI_STR,
                "Username required but not specified",
            ));
        }
    }

    if let Some(auth_data) = auth_data.as_ref() {
        acq_creds_handle_result = acq_creds_handle_result.with_auth_data(auth_data);
    }

    acq_creds_handle_result.execute(kerberos).map_err(|e| {
        Error::authentication_error(
            GSSAPI_STR,
            &format!("Failed to acquire credentials handle: {e:?}"),
        )
    })
}

fn step_helper(
    kerberos: &mut Kerberos,
    cred_handle: &mut <Kerberos as SspiImpl>::CredentialsHandle,
    input_buffer: &mut [SecurityBuffer],
    output_buffer: &mut [SecurityBuffer],
    service_principal: String,
) -> Result<InitializeSecurityContextResult> {
    let mut builder = kerberos
        .initialize_security_context()
        .with_credentials_handle(cred_handle)
        .with_context_requirements(ClientRequestFlags::MUTUAL_AUTH)
        .with_target_data_representation(DataRepresentation::Native)
        .with_target_name(&service_principal)
        .with_input(input_buffer)
        .with_output(output_buffer);

    let result = kerberos
        .initialize_security_context_impl(&mut builder)
        .map_err(|e| {
            Error::authentication_error(
                GSSAPI_STR,
                &format!("Failed to initialize security context: {e}"),
            )
        })?
        .resolve_to_result()
        .map_err(|e| {
            Error::authentication_error(GSSAPI_STR, &format!("Failed to resolve to result: {e}"))
        });

    result
}

fn step(
    kerberos: &mut Kerberos,
    cred_handle: &mut <Kerberos as SspiImpl>::CredentialsHandle,
    input_token: &[u8],
    service_principal: String,
) -> Result<(Vec<u8>, SecurityStatus)> {
    let mut secure_input_buffer =
        vec![SecurityBuffer::new(input_token.to_vec(), BufferType::Token)];
    let mut secure_output_buffer = vec![SecurityBuffer::new(Vec::new(), BufferType::Token)];
    match step_helper(
        kerberos,
        cred_handle,
        &mut secure_input_buffer,
        &mut secure_output_buffer,
        service_principal,
    ) {
        Ok(result) => {
            let output_buffer = secure_output_buffer[0].to_owned();
            Ok((output_buffer.buffer, result.status))
        }
        Err(e) => Err(Error::authentication_error(
            GSSAPI_STR,
            &format!("error stepping: {e:?}"),
        )),
    }
}
