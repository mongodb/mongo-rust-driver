use lazy_static::lazy_static;

mod util;

use util::TestClient;

lazy_static! {
    static ref CLIENT: TestClient = TestClient::new();
}
