use std::{convert::TryInto, path::PathBuf};

use bson::{Document, RawDocument, RawDocumentBuf};
use futures_util::{stream, TryStreamExt};
use mongocrypt::ctx::{Ctx, State};
use rayon::ThreadPool;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{oneshot, Mutex},
};

use crate::{
    client::{options::ServerAddress, WeakClient},
    cmap::options::StreamOptions,
    error::{Error, ErrorKind, Result},
    operation::{RawOutput, RunCommand},
    runtime::AsyncStream,
    Client,
    Namespace,
};

use super::options::KmsProvidersTlsOptions;

#[derive(Debug)]
pub(crate) struct CryptExecutor {
    key_vault_client: WeakClient,
    key_vault_namespace: Namespace,
    tls_options: Option<KmsProvidersTlsOptions>,
    crypto_threads: ThreadPool,
    mongocryptd: Option<Mongocryptd>,
    metadata_client: Option<WeakClient>,
}

#[derive(Debug)]
struct Mongocryptd {
    opts: MongocryptdOptions,
    client: Mutex<Client>,
}

impl CryptExecutor {
    pub(crate) fn new_explicit(
        key_vault_client: WeakClient,
        key_vault_namespace: Namespace,
        tls_options: Option<KmsProvidersTlsOptions>,
    ) -> Result<Self> {
        let num_cpus = std::thread::available_parallelism()?.get();
        let crypto_threads = rayon::ThreadPoolBuilder::new()
            .num_threads(num_cpus)
            .build()
            .map_err(|e| Error::internal(format!("could not initialize thread pool: {}", e)))?;
        Ok(Self {
            key_vault_client,
            key_vault_namespace,
            tls_options,
            crypto_threads,
            mongocryptd: None,
            metadata_client: None,
        })
    }

    pub(crate) async fn new_implicit(
        key_vault_client: WeakClient,
        key_vault_namespace: Namespace,
        tls_options: Option<KmsProvidersTlsOptions>,
        mongocryptd_opts: Option<MongocryptdOptions>,
        metadata_client: Option<WeakClient>,
    ) -> Result<Self> {
        let mongocryptd = match mongocryptd_opts {
            Some(opts) => {
                let client = Mutex::new(opts.spawn().await?);
                Some(Mongocryptd { opts, client })
            }
            None => None,
        };
        let mut exec = Self::new_explicit(key_vault_client, key_vault_namespace, tls_options)?;
        exec.mongocryptd = mongocryptd;
        exec.metadata_client = metadata_client;
        Ok(exec)
    }

    pub(crate) async fn run_ctx(&self, ctx: Ctx, db: Option<&str>) -> Result<RawDocumentBuf> {
        let mut result = None;
        // This needs to be a `Result` so that the `Ctx` can be temporarily owned by the processing
        // thread for crypto finalization.  An `Option` would also work here, but `Result` means we
        // can return a helpful error if things get into a broken state rather than panicing.
        let mut ctx = Ok(ctx);
        loop {
            let state = result_ref(&ctx)?.state()?;
            match state {
                State::NeedMongoCollinfo => {
                    let ctx = result_mut(&mut ctx)?;
                    let filter = raw_to_doc(ctx.mongo_op()?)?;
                    let metadata_client = self
                        .metadata_client
                        .as_ref()
                        .and_then(|w| w.upgrade())
                        .ok_or_else(|| {
                        Error::internal("metadata_client required for NeedMongoCollinfo state")
                    })?;
                    let db = metadata_client.database(db.as_ref().ok_or_else(|| {
                        Error::internal("db required for NeedMongoCollinfo state")
                    })?);
                    let mut cursor = db.list_collections(filter, None).await?;
                    if cursor.advance().await? {
                        ctx.mongo_feed(cursor.current())?;
                    }
                    ctx.mongo_done()?;
                }
                State::NeedMongoMarkings => {
                    let ctx = result_mut(&mut ctx)?;
                    let command = ctx.mongo_op()?.to_raw_document_buf();
                    let db = db.as_ref().ok_or_else(|| {
                        Error::internal("db required for NeedMongoMarkings state")
                    })?;
                    let op = RawOutput(RunCommand::new_raw(db.to_string(), command, None, None)?);
                    let mongocryptd = self
                        .mongocryptd
                        .as_ref()
                        .ok_or_else(|| Error::internal("mongocryptd client not found"))?;
                    let mut mongocryptd_client = mongocryptd.client.lock().await;
                    let mut result = mongocryptd_client.execute_operation(op.clone(), None).await;
                    if let Err(e) = &result {
                        if matches!(&*e.kind, ErrorKind::ServerSelection { .. }) {
                            *mongocryptd_client = mongocryptd.opts.spawn().await?;
                            let new_result = mongocryptd_client.execute_operation(op, None).await;
                            if new_result.is_ok() {
                                result = new_result;
                            }
                        }
                    }
                    let response = result?;
                    ctx.mongo_feed(response.raw_body())?;
                    ctx.mongo_done()?;
                }
                State::NeedMongoKeys => {
                    let ctx = result_mut(&mut ctx)?;
                    let filter = raw_to_doc(ctx.mongo_op()?)?;
                    let kv_ns = &self.key_vault_namespace;
                    let kv_client = self
                        .key_vault_client
                        .upgrade()
                        .ok_or_else(|| Error::internal("key vault client dropped"))?;
                    let kv_coll = kv_client
                        .database(&kv_ns.db)
                        .collection::<RawDocumentBuf>(&kv_ns.coll);
                    let mut cursor = kv_coll.find(filter, None).await?;
                    while cursor.advance().await? {
                        ctx.mongo_feed(cursor.current())?;
                    }
                    ctx.mongo_done()?;
                }
                State::NeedKms => {
                    let ctx = result_mut(&mut ctx)?;
                    let scope = ctx.kms_scope();
                    let mut kms_ctxen: Vec<Result<_>> = vec![];
                    while let Some(kms_ctx) = scope.next_kms_ctx() {
                        kms_ctxen.push(Ok(kms_ctx));
                    }
                    stream::iter(kms_ctxen)
                        .try_for_each_concurrent(None, |mut kms_ctx| async move {
                            let endpoint = kms_ctx.endpoint()?;
                            let addr = ServerAddress::parse(endpoint)?;
                            let provider = kms_ctx.kms_provider()?;
                            let tls_options = self
                                .tls_options
                                .as_ref()
                                .and_then(|tls| tls.get(&provider))
                                .cloned()
                                .unwrap_or_default();
                            let mut stream = AsyncStream::connect(
                                StreamOptions::builder()
                                    .address(addr)
                                    .tls_options(tls_options)
                                    .build(),
                            )
                            .await?;
                            stream.write_all(kms_ctx.message()?).await?;
                            let mut buf = vec![0];
                            while kms_ctx.bytes_needed() > 0 {
                                let buf_size = kms_ctx.bytes_needed().try_into().map_err(|e| {
                                    Error::internal(format!("buffer size overflow: {}", e))
                                })?;
                                buf.resize(buf_size, 0);
                                let count = stream.read(&mut buf).await?;
                                kms_ctx.feed(&buf[0..count])?;
                            }
                            Ok(())
                        })
                        .await?;
                }
                State::NeedKmsCredentials => todo!("RUST-1314"),
                State::Ready => {
                    let (tx, rx) = oneshot::channel();
                    let mut thread_ctx = std::mem::replace(
                        &mut ctx,
                        Err(Error::internal("crypto context not present")),
                    )?;
                    self.crypto_threads.spawn(move || {
                        let result = thread_ctx.finalize().map(|doc| doc.to_owned());
                        let _ = tx.send((thread_ctx, result));
                    });
                    let (ctx_again, output) = rx
                        .await
                        .map_err(|_| Error::internal("crypto thread dropped"))?;
                    ctx = Ok(ctx_again);
                    result = Some(output?);
                }
                State::Done => break,
                s => return Err(Error::internal(format!("unhandled state {:?}", s))),
            }
        }
        match result {
            Some(doc) => Ok(doc),
            None => Err(Error::internal("libmongocrypt terminated without output")),
        }
    }
}

#[derive(Debug)]
pub(crate) struct MongocryptdOptions {
    pub(crate) spawn_path: Option<PathBuf>,
    pub(crate) spawn_args: Vec<String>,
    pub(crate) uri: Option<String>,
}

impl MongocryptdOptions {
    async fn spawn(&self) -> Result<Client> {
        use std::process::{Command, Stdio};

        let which_path;
        let bin_path = match &self.spawn_path {
            Some(s) => s,
            None => {
                which_path = which::which("mongocryptd")
                    .map_err(|e| Error::invalid_argument(format!("{}", e)))?;
                &which_path
            }
        };
        let mut args: Vec<&str> = vec![];
        let mut has_idle = false;
        for arg in &self.spawn_args {
            has_idle |= arg.starts_with("--idleShutdownTimeoutSecs");
            args.push(arg);
        }
        if !has_idle {
            args.push("--idleShutdownTimeoutSecs=60");
        }
        Command::new(bin_path)
            .args(&args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;

        let uri = self.uri.as_deref().unwrap_or("mongodb://localhost:27020");
        let mut options = crate::options::ClientOptions::parse_uri(uri, None).await?;
        options.server_selection_timeout = Some(std::time::Duration::from_millis(10_000));
        Client::with_options(options)
    }
}

fn result_ref<T>(r: &Result<T>) -> Result<&T> {
    r.as_ref().map_err(Error::clone)
}

fn result_mut<T>(r: &mut Result<T>) -> Result<&mut T> {
    r.as_mut().map_err(|e| e.clone())
}

fn raw_to_doc(raw: &RawDocument) -> Result<Document> {
    raw.try_into()
        .map_err(|e| Error::internal(format!("could not parse raw document: {}", e)))
}
