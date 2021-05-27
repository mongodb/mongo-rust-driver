#![allow(dead_code)]

extern crate mongodb;

use futures::Future;
use mongodb::{
    bson::{doc, Document},
    error::{Result, TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT},
    options::{Acknowledgment, TransactionOptions, WriteConcern},
    ClientSession,
};

async fn execute_transaction(session: &mut ClientSession) -> Result<()> {
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

async fn execute_transaction_with_retry<F, G>(
    execute_transaction: F,
    session: &mut ClientSession,
) -> Result<()>
where
    F: Fn(&mut ClientSession) -> G,
    G: Future<Output = Result<()>>,
{
    while let Err(err) = execute_transaction(session).await {
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

async fn commit_with_retry(session: &mut ClientSession) -> Result<()> {
    loop {
        match session.commit_transaction().await {
            Ok(()) => {
                println!("Transaction committed.");
                return Ok(());
            }
            Err(err) => {
                if err.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT) {
                    println!(
                        "Encountered UnknownTransactionCommitResult, retrying commit operation."
                    );
                    continue;
                } else {
                    println!("Encountered non-retryable error during commit.");
                    return Err(err);
                }
            }
        }
    }
}

async fn update_employee_info(session: &mut ClientSession) -> Result<()> {
    // TODO RUST-824 add snapshot read concern
    let transaction_options = TransactionOptions::builder()
        .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
        .build();
    session.start_transaction(transaction_options).await?;

    execute_transaction_with_retry(execute_transaction, session).await
}
