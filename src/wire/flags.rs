pub const RESPONSE_FLAGS_LENGTH: i32 = 32 / 8;

bitflags! {
    pub struct ResponseFlags: i32 {
        const CURSOR_NOT_FOUND  = 0b00000001;
        const QUERY_FAILURE     = 0b00000010;
        const AWAIT_CAPABLE     = 0b00001000;
    }
}

pub const QUERY_FLAGS_LENGTH: i32 = 32 / 8;

bitflags! {
    pub struct QueryFlags: i32 {
        const TAILABLE_CURSOR   = 0b00000010;
        const SLAVE_OK          = 0b00000100;
        const OPLOG_RELAY       = 0b00001000;
        const NO_CURSOR_TIMEOUT = 0b00010000;
        const AWAIT_DATA        = 0b00100000;
        const EXHAUST           = 0b01000000;
        const PARTIAL           = 0b10000000;
    }
}
