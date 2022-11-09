use std::{
    convert::TryInto,
    ops::DerefMut,
    path::{Path, PathBuf},
};

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
    error::{Error, Result},
    operation::{RawOutput, RunCommand},
    runtime::{AsyncStream, Process, TlsConfig},
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
    mongocryptd_client: Option<Client>,
    metadata_client: Option<WeakClient>,
}

impl CryptExecutor {
    pub(crate) fn new_explicit(
        key_vault_client: WeakClient,
        key_vault_namespace: Namespace,
        tls_options: Option<KmsProvidersTlsOptions>,
    ) -> Result<Self> {
        // TODO RUST-1492: Replace num_cpus with std::thread::available_parallelism.
        let crypto_threads = rayon::ThreadPoolBuilder::new()
            .num_threads(num_cpus::get())
            .build()
            .map_err(|e| Error::internal(format!("could not initialize thread pool: {}", e)))?;
        Ok(Self {
            key_vault_client,
            key_vault_namespace,
            tls_options,
            crypto_threads,
            mongocryptd: None,
            mongocryptd_client: None,
            metadata_client: None,
        })
    }

    pub(crate) async fn new_implicit(
        key_vault_client: WeakClient,
        key_vault_namespace: Namespace,
        tls_options: Option<KmsProvidersTlsOptions>,
        mongocryptd_opts: Option<MongocryptdOptions>,
        mongocryptd_client: Option<Client>,
        metadata_client: Option<WeakClient>,
    ) -> Result<Self> {
        let mongocryptd = match mongocryptd_opts {
            Some(opts) => Some(Mongocryptd::new(opts).await?),
            None => None,
        };
        let mut exec = Self::new_explicit(key_vault_client, key_vault_namespace, tls_options)?;
        exec.mongocryptd = mongocryptd;
        exec.mongocryptd_client = mongocryptd_client;
        exec.metadata_client = metadata_client;
        Ok(exec)
    }

    #[cfg(test)]
    pub(crate) fn mongocryptd_spawned(&self) -> bool {
        self.mongocryptd.is_some()
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
                    let mongocryptd_client = self.mongocryptd_client.as_ref().ok_or_else(|| {
                        Error::invalid_argument("this operation requires mongocryptd")
                    })?;
                    let result = mongocryptd_client.execute_operation(op.clone(), None).await;
                    let response = match result {
                        Ok(r) => r,
                        Err(e) if e.is_server_selection_error() => {
                            if let Some(mongocryptd) = &self.mongocryptd {
                                mongocryptd.respawn().await?;
                                match mongocryptd_client.execute_operation(op, None).await {
                                    Ok(r) => r,
                                    Err(new_e) if !new_e.is_server_selection_error() => {
                                        return Err(new_e)
                                    }
                                    Err(_) => return Err(e),
                                }
                            } else {
                                return Err(e);
                            }
                        }
                        Err(e) => return Err(e),
                    };
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
                            let mut stream =
                                AsyncStream::connect(addr, Some(&TlsConfig::new(tls_options)?))
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
                State::NeedKmsCredentials => {
                    // TODO(RUST-1314, RUST-1417): support fetching KMS credentials.
                    return Err(Error::internal("KMS credentials are not yet supported"));
                }
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
struct Mongocryptd {
    opts: MongocryptdOptions,
    child: Mutex<Result<Process>>,
}

impl Mongocryptd {
    async fn new(opts: MongocryptdOptions) -> Result<Self> {
        let child = Mutex::new(Ok(Self::spawn(&opts)?));
        Ok(Self { opts, child })
    }

    async fn respawn(&self) -> Result<()> {
        let mut child = match self.child.try_lock() {
            Ok(l) => l,
            _ => {
                // Another respawn is in progress.  Lock to wait for it.
                return unit_err(&*self.child.lock().await);
            }
        };
        let new_child = Self::spawn(&self.opts);
        if new_child.is_ok() {
            if let Ok(mut old_child) = std::mem::replace(child.deref_mut(), new_child) {
                crate::runtime::spawn(async move {
                    let _ = old_child.kill();
                    let _ = old_child.wait().await;
                });
            }
        } else {
            *child = new_child;
        }
        unit_err(&*child)
    }

    fn spawn(opts: &MongocryptdOptions) -> Result<Process> {
        let bin_path = match &opts.spawn_path {
            Some(s) => s,
            None => Path::new("mongocryptd"),
        };
        let mut args: Vec<&str> = vec![];
        let mut has_idle = false;
        for arg in &opts.spawn_args {
            has_idle |= arg.starts_with("--idleShutdownTimeoutSecs");
            args.push(arg);
        }
        if !has_idle {
            args.push("--idleShutdownTimeoutSecs=60");
        }
        Process::spawn(bin_path, &args)
    }
}

fn unit_err<T>(r: &Result<T>) -> Result<()> {
    match r {
        Ok(_) => Ok(()),
        Err(e) => Err(e.clone()),
    }
}

#[derive(Debug)]
pub(crate) struct MongocryptdOptions {
    pub(crate) spawn_path: Option<PathBuf>,
    pub(crate) spawn_args: Vec<String>,
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
