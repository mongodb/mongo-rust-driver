#![allow(dead_code)]
#![cfg(all(not(feature = "sync"), not(feature = "tokio-sync")))]

// START TRANSACTIONS EXAMPLE
use mongodb::{
    bson::{doc, Document},
    error::{Result, TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT},
    options::{Acknowledgment, ReadConcern, TransactionOptions, WriteConcern},
    ClientSession,
};

async fn update_employee_info(session: &mut ClientSession) -> Result<()> {
    let transaction_options = TransactionOptions::builder()
        .read_concern(ReadConcern::snapshot())
        .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
        .build();
    session.start_transaction(transaction_options).await?;

    execute_transaction_with_retry(session).await
}

async fn execute_transaction_with_retry(session: &mut ClientSession) -> Result<()> {
    while let Err(err) = execute_employee_info_transaction(session).await {
        println!("Transaction aborted. Error returned during transaction.");
        if err.contains_label(TRANSIENT_TRANSACTION_ERROR) {
            println!("Encountered TransientTransactionError, retrying transaction.");
            continue;
        } else {
            return Err(err);
        }
    }
    Ok(())
}

async fn execute_employee_info_transaction(session: &mut ClientSession) -> Result<()> {
    let client = session.client();
    let employees = client.database("hr").collection::<Document>("employees");
    let events = client
        .database("reporting")
        .collection::<Document>("events");

    employees
        .update_one_with_session(
            doc! { "employee": 3 },
            doc! { "$set": { "status": "Inactive" } },
            None,
            session,
        )
        .await?;
    events
        .insert_one_with_session(
            doc! { "employee": 3, "status": { "new": "Inactive", "old": "Active" } },
            None,
            session,
        )
        .await?;

    commit_with_retry(session).await
}

async fn commit_with_retry(session: &mut ClientSession) -> Result<()> {
    while let Err(err) = session.commit_transaction().await {
        if err.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT) {
            println!("Encountered UnknownTransactionCommitResult, retrying commit operation.");
            continue;
        } else {
            println!("Encountered non-retryable error during commit.");
            return Err(err);
        }
    }
    println!("Transaction committed.");
    Ok(())
}
// END TRANSACTIONS EXAMPLE
