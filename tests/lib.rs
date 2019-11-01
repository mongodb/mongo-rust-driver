use lazy_static::lazy_static;

mod read_concern;
mod util;

use util::TestClient;

lazy_static! {
    static ref CLIENT: TestClient = TestClient::new();
}
