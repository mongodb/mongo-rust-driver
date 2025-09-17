use std::env;

use crate::bson::rawdoc;

use crate::{
    cmap::establish::handshake::{
        ClientMetadata,
        FaasEnvironmentName,
        RuntimeEnvironment,
        BASE_CLIENT_METADATA,
    },
    Client,
};

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

async fn check_faas_handshake(
    vars: &[(&'static str, &str)],
    expected: &ClientMetadata,
) -> Result<()> {
    let _tv = TempVars::set(vars);

    let client = Client::for_test().await;
    client.list_database_names().await?;
    #[allow(clippy::incompatible_msrv)]
    let metadata = crate::cmap::establish::handshake::TEST_METADATA
        .get()
        .unwrap();
    assert_eq!(expected, metadata);

    Ok(())
}

#[tokio::test]
async fn valid_aws() -> Result<()> {
    check_faas_handshake(
        &[
            ("AWS_EXECUTION_ENV", "AWS_Lambda_java8"),
            ("AWS_REGION", "us-east-2"),
            ("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "1024"),
        ],
        &ClientMetadata {
            env: Some(RuntimeEnvironment {
                name: Some(FaasEnvironmentName::AwsLambda),
                runtime: Some("AWS_Lambda_java8".to_string()),
                memory_mb: Some(1024),
                region: Some("us-east-2".to_string()),
                ..RuntimeEnvironment::UNSET
            }),
            ..BASE_CLIENT_METADATA.clone()
        },
    )
    .await
}

#[tokio::test]
async fn valid_azure() -> Result<()> {
    check_faas_handshake(
        &[("FUNCTIONS_WORKER_RUNTIME", "node")],
        &ClientMetadata {
            env: Some(RuntimeEnvironment {
                name: Some(FaasEnvironmentName::AzureFunc),
                runtime: Some("node".to_string()),
                ..RuntimeEnvironment::UNSET
            }),
            ..BASE_CLIENT_METADATA.clone()
        },
    )
    .await
}

#[tokio::test]
async fn valid_gcp() -> Result<()> {
    check_faas_handshake(
        &[
            ("K_SERVICE", "servicename"),
            ("FUNCTION_MEMORY_MB", "1024"),
            ("FUNCTION_TIMEOUT_SEC", "60"),
            ("FUNCTION_REGION", "us-central1"),
        ],
        &ClientMetadata {
            env: Some(RuntimeEnvironment {
                name: Some(FaasEnvironmentName::GcpFunc),
                memory_mb: Some(1024),
                timeout_sec: Some(60),
                region: Some("us-central1".to_string()),
                ..RuntimeEnvironment::UNSET
            }),
            ..BASE_CLIENT_METADATA.clone()
        },
    )
    .await
}

#[tokio::test]
async fn valid_vercel() -> Result<()> {
    check_faas_handshake(
        &[
            ("VERCEL", "1"),
            ("VERCEL_URL", "*.vercel.app"),
            ("VERCEL_REGION", "cdg1"),
        ],
        &ClientMetadata {
            env: Some(RuntimeEnvironment {
                name: Some(FaasEnvironmentName::Vercel),
                region: Some("cdg1".to_string()),
                ..RuntimeEnvironment::UNSET
            }),
            ..BASE_CLIENT_METADATA.clone()
        },
    )
    .await
}

#[tokio::test]
async fn invalid_multiple_providers() -> Result<()> {
    check_faas_handshake(
        &[
            ("AWS_EXECUTION_ENV", "AWS_Lambda_java8"),
            ("FUNCTIONS_WORKER_RUNTIME", "node"),
        ],
        &BASE_CLIENT_METADATA,
    )
    .await
}

#[tokio::test]
async fn invalid_long_string() -> Result<()> {
    check_faas_handshake(
        &[
            ("AWS_EXECUTION_ENV", "AWS_Lambda_java8"),
            ("AWS_REGION", &"a".repeat(512)),
        ],
        &ClientMetadata {
            env: Some(RuntimeEnvironment {
                name: Some(FaasEnvironmentName::AwsLambda),
                ..RuntimeEnvironment::UNSET
            }),
            ..BASE_CLIENT_METADATA.clone()
        },
    )
    .await
}

#[tokio::test]
async fn invalid_wrong_type() -> Result<()> {
    check_faas_handshake(
        &[
            ("AWS_EXECUTION_ENV", "AWS_Lambda_java8"),
            ("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "big"),
        ],
        &ClientMetadata {
            env: Some(RuntimeEnvironment {
                name: Some(FaasEnvironmentName::AwsLambda),
                runtime: Some("AWS_Lambda_java8".to_string()),
                ..RuntimeEnvironment::UNSET
            }),
            ..BASE_CLIENT_METADATA.clone()
        },
    )
    .await
}

#[tokio::test]
async fn invalid_aws_not_lambda() -> Result<()> {
    check_faas_handshake(&[("AWS_EXECUTION_ENV", "EC2")], &BASE_CLIENT_METADATA).await
}

#[tokio::test]
async fn valid_container_and_faas() -> Result<()> {
    check_faas_handshake(
        &[
            ("AWS_EXECUTION_ENV", "AWS_Lambda_java8"),
            ("AWS_REGION", "us-east-2"),
            ("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "1024"),
            ("KUBERNETES_SERVICE_HOST", "1"),
        ],
        &ClientMetadata {
            env: Some(RuntimeEnvironment {
                name: Some(FaasEnvironmentName::AwsLambda),
                runtime: Some("AWS_Lambda_java8".to_string()),
                region: Some("us-east-2".to_string()),
                memory_mb: Some(1024),
                container: Some(rawdoc! { "orchestrator": "kubernetes"}),
                ..RuntimeEnvironment::UNSET
            }),
            ..BASE_CLIENT_METADATA.clone()
        },
    )
    .await
}
