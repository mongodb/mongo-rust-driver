#![no_main]
use libfuzzer_sys::fuzz_target;
use mongodb::cmap::conn::wire::header::Header;

fuzz_target!(|data: &[u8]| {
    if data.len() < Header::LENGTH {
        return;
    }
    if let Ok(header) = Header::from_slice(data) {
        if header.length < 0 {
            panic!("Negative length detected: {}", header.length);
        }
        if header.length as usize > std::usize::MAX - Header::LENGTH {
            panic!("Integer overflow detected in length calculation");
        }
        let total_size = Header::LENGTH + header.length as usize;
        if total_size > data.len() {
            return;
        }
    }
});
