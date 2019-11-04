use lazy_static::lazy_static;

#[allow(dead_code)]
mod spec;
mod util;

use util::TestClient;

lazy_static! {
    static ref CLIENT: TestClient = TestClient::new();
}
