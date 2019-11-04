mod write;

use mongodb::{event::command::CommandStartedEvent, Collection};

use crate::util::EventClient;

fn run_operation_with_events(
    name: &str,
    command_name: &str,
    function: impl FnOnce(Collection),
) -> Vec<CommandStartedEvent> {
    let client = EventClient::new();
    let collection = client.database(name).collection(name);
    function(collection);
    let events: Vec<_> = client
        .events
        .write()
        .unwrap()
        .drain(..)
        .filter(|event| event.command_name == command_name)
        .collect();
    assert_eq!(events.len(), 1);
    events
}
