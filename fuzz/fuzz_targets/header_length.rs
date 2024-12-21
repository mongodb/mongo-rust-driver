#![no_main]
use libfuzzer_sys::fuzz_target;
use mongodb::cmap::conn::wire::{
    header::{Header, OpCode},
    message::Message,
};

fuzz_target!(|data: &[u8]| {
    if data.len() < Header::LENGTH {
        return;
    }
    if let Ok(mut header) = Header::from_slice(data) {
        // read_from_slice will adjust the data for the header length, this first check will
        // almost always have length mismatches, but length mismatches are a possible attack
        // vector.
        // This will also often have the wrong opcode, but that is also a possible attack vector.
        if let Ok(message) = Message::read_from_slice(data, header.clone()) {
            let _ = message;
        }
        // try again with the header.length set to the data length and the header.opcode ==
        // OpCode::Message to catch other attack vectors.
        header.length = data.len() as i32 - Header::LENGTH as i32;
        header.op_code = OpCode::Message;
        if let Ok(message) = Message::read_from_slice(data, header) {
            let _ = message;
        }
    }
});
