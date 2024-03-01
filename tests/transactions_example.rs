// START TRANSACTIONS EXAMPLE
use mongodb::{
    bson::{doc, Document},
    error::{Result, TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT},
    options::{
        Acknowledgment,
        ReadConcern,
        ReadPreference,
        SelectionCriteria,
        TransactionOptions,
        WriteConcern,
    },
    Client,
    ClientSession,
};

#[tokio::main]
async fn main() -> Result<()> {
    let uri = std::env::var("MONGODB_URI").expect("MONGODB_URI must be set");
    let client = Client::with_uri_str(uri).await?;

    let mut session = client.start_session().await?;
    run_transaction_with_retry(&mut session).await
}

async fn run_transaction_with_retry(session: &mut ClientSession) -> Result<()> {
    loop {
        match execute_transaction(session).await {
            Ok(()) => {
                println!("Transaction succeeded.");
                return Ok(());
            }
            Err(error) => {
                if error.contains_label(TRANSIENT_TRANSACTION_ERROR) {
                    println!("TransientTransactionError, retrying transaction...");
                    continue;
                } else {
                    session.abort_transaction().await?;
                    return Err(error);
                }
            }
        }
    }
}

async fn execute_transaction(session: &mut ClientSession) -> Result<()> {
    let transaction_options = TransactionOptions::builder()
        .read_concern(ReadConcern::snapshot())
        .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
        .selection_criteria(SelectionCriteria::ReadPreference(ReadPreference::Primary))
        .build();
    session.start_transaction(transaction_options).await?;

    let client = session.client();
    let employees = client.database("hr").collection::<Document>("employees");
    let events = client.database("reporting").collection("events");

    employees
        .update_one(
            doc! { "employee": 3 },
            doc! { "$set": { "status": "Inactive" } }
        )
        .session(&mut *session)
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
    loop {
        match session.commit_transaction().await {
            Ok(()) => {
                println!("Transaction committed.");
                return Ok(());
            }
            Err(error) => {
                if error.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT) {
                    println!("UnknownTransactionCommitResult, retrying commit operation...");
                    continue;
                } else {
                    println!("Error during commit.");
                    return Err(error);
                }
            }
        }
    }
}
// END TRANSACTIONS EXAMPLE
