#![no_main]
use libfuzzer_sys::fuzz_target;
use mongodb::cmap::conn::wire::{header::Header, message::Message};

fuzz_target!(|data: &[u8]| {
    if data.len() < Header::LENGTH {
        return;
    }
    if let Ok(header) = Header::from_slice(data) {
        let data = &data[Header::LENGTH..];
        if let Ok(message) = Message::read_from_slice(data, header) {
            let _ = message;
        }
    }
});
