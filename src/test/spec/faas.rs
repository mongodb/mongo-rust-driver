use std::env;

use crate::Client;

type Result<T> = anyhow::Result<T>;

struct TempVars {
    restore: Vec<(&'static str, Option<std::ffi::OsString>)>,
}

impl TempVars {
    #[must_use]
    fn set(vars: &[(&'static str, &str)]) -> TempVars {
        let mut restore = vec![];
        for (name, value) in vars {
            restore.push((*name, env::var_os(name)));
            env::set_var(name, value);
        }

        Self { restore }
    }
}

impl Drop for TempVars {
    fn drop(&mut self) {
        for (name, value) in self.restore.drain(..) {
            if let Some(v) = value {
                env::set_var(name, v);
            } else {
                env::remove_var(name);
            }
        }
    }
}

async fn check_faas_handshake(vars: &[(&'static str, &str)]) -> Result<()> {
    let _tv = TempVars::set(vars);

    let client = Client::test_builder().build().await;
    client.list_database_names().await?;

    Ok(())
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn valid_aws() -> Result<()> {
    check_faas_handshake(&[
        ("AWS_EXECUTION_ENV", "AWS_Lambda_java8"),
        ("AWS_REGION", "us-east-2"),
        ("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "1024"),
    ])
    .await
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn valid_azure() -> Result<()> {
    check_faas_handshake(&[("FUNCTIONS_WORKER_RUNTIME", "node")]).await
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn valid_gcp() -> Result<()> {
    check_faas_handshake(&[
        ("K_SERVICE", "servicename"),
        ("FUNCTION_MEMORY_MB", "1024"),
        ("FUNCTION_TIMEOUT_SEC", "60"),
        ("FUNCTION_REGION", "us-central1"),
    ])
    .await
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn valid_vercel() -> Result<()> {
    check_faas_handshake(&[
        ("VERCEL", "1"),
        ("VERCEL_URL", "*.vercel.app"),
        ("VERCEL_REGION", "cdg1"),
    ])
    .await
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn invalid_multiple_providers() -> Result<()> {
    check_faas_handshake(&[
        ("AWS_EXECUTION_ENV", "AWS_Lambda_java8"),
        ("FUNCTIONS_WORKER_RUNTIME", "node"),
    ])
    .await
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn invalid_long_string() -> Result<()> {
    check_faas_handshake(&[
        ("AWS_EXECUTION_ENV", "AWS_Lambda_java8"),
        ("AWS_REGION", &"a".repeat(512)),
    ])
    .await
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn invalid_wrong_type() -> Result<()> {
    check_faas_handshake(&[
        ("AWS_EXECUTION_ENV", "AWS_Lambda_java8"),
        ("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "big"),
    ])
    .await
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn invalid_aws_not_lambda() -> Result<()> {
    check_faas_handshake(&[("AWS_EXECUTION_ENV", "EC2")]).await
}
