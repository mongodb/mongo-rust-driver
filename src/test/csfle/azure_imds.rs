use std::time::Instant;

use crate::{bson::rawdoc, client::csfle::state_machine::azure::ExecutorState};

use super::{Result, AZURE_IMDS_MOCK_PORT};

// Prose test 18. Azure IMDS Credentials
#[tokio::test]
async fn azure_imds() -> Result<()> {
    let mut azure_exec = ExecutorState::new()?;
    azure_exec.test_host = Some(("localhost", *AZURE_IMDS_MOCK_PORT));

    // Case 1: Success
    {
        let now = Instant::now();
        let token = azure_exec.get_token().await?;
        assert_eq!(token, rawdoc! { "accessToken": "magic-cookie" });
        let cached = azure_exec.take_cached().await.expect("cached token");
        assert_eq!(cached.server_response.expires_in, "70");
        assert_eq!(cached.server_response.resource, "https://vault.azure.net");
        assert!((65..75).contains(&cached.expire_time.duration_since(now).as_secs()));
    }

    // Case 2: Empty JSON
    {
        azure_exec.test_param = Some("case=empty-json");
        let result = azure_exec.get_token().await;
        assert!(result.is_err(), "expected err got {result:?}");
        assert!(result.unwrap_err().is_auth_error());
    }

    // Case 3: Bad JSON
    {
        azure_exec.test_param = Some("case=bad-json");
        let result = azure_exec.get_token().await;
        assert!(result.is_err(), "expected err got {result:?}");
        assert!(result.unwrap_err().is_auth_error());
    }

    // Case 4: HTTP 404
    {
        azure_exec.test_param = Some("case=404");
        let result = azure_exec.get_token().await;
        assert!(result.is_err(), "expected err got {result:?}");
        assert!(result.unwrap_err().is_auth_error());
    }

    // Case 5: HTTP 500
    {
        azure_exec.test_param = Some("case=500");
        let result = azure_exec.get_token().await;
        assert!(result.is_err(), "expected err got {result:?}");
        assert!(result.unwrap_err().is_auth_error());
    }

    // Case 6: Slow Response
    {
        azure_exec.test_param = Some("case=slow");
        let result = azure_exec.get_token().await;
        assert!(result.is_err(), "expected err got {result:?}");
        assert!(result.unwrap_err().is_auth_error());
    }

    Ok(())
}
