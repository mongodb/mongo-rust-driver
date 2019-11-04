use lazy_static::lazy_static;

mod util;
mod write_concern;

use util::TestClient;

lazy_static! {
    static ref CLIENT: TestClient = TestClient::new();
}
