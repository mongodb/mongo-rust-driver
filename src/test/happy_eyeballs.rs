use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::runtime::stream::tcp_connect;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static CONTROL: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 10036);
const SLOW_V4: u8 = 4;
const SLOW_V6: u8 = 6;

async fn happy_request(payload: u8) -> (SocketAddr, SocketAddr) {
    let mut control = tcp_connect(vec![CONTROL], None).await.unwrap();
    control.write_u8(payload).await.unwrap();
    let resp = control.read_u8().await.unwrap();
    assert_eq!(resp, 1);
    let v4_port = control.read_u16().await.unwrap();
    let v6_port = control.read_u16().await.unwrap();
    (
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), v4_port),
        SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), v6_port),
    )
}

#[tokio::test]
async fn slow_ipv4() {
    let (v4_addr, v6_addr) = happy_request(SLOW_V4).await;
    let mut conn = tcp_connect(vec![v4_addr, v6_addr], None).await.unwrap();
    assert!(conn.peer_addr().unwrap().is_ipv6());
    let data = conn.read_u8().await.unwrap();
    assert_eq!(data, 6);
}

#[tokio::test]
async fn slow_ipv6() {
    let (v4_addr, v6_addr) = happy_request(SLOW_V6).await;
    let mut conn = tcp_connect(vec![v4_addr, v6_addr], None).await.unwrap();
    assert!(conn.peer_addr().unwrap().is_ipv4());
    let data = conn.read_u8().await.unwrap();
    assert_eq!(data, 4);
}
