use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::runtime::stream::tcp_connect;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type Result<T> = anyhow::Result<T>;

static CONTROL: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 10036);
const SLOW_V4: u8 = 4;
const SLOW_V6: u8 = 6;

async fn happy_request(payload: u8) -> Result<(SocketAddr, SocketAddr)> {
    let mut control = tcp_connect(vec![CONTROL.clone()]).await?;
    control.write_u8(payload).await?;
    let resp = control.read_u8().await?;
    assert_eq!(resp, 1);
    let v4_port = control.read_u16().await?;
    let v6_port = control.read_u16().await?;
    Ok((
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), v4_port),
        SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), v6_port),
    ))
}

#[tokio::test]
async fn slow_ipv4() -> Result<()> {
    let (v4_addr, v6_addr) = happy_request(SLOW_V4).await?;
    let mut conn = tcp_connect(vec![v4_addr, v6_addr]).await?;
    assert!(conn.peer_addr()?.is_ipv6());
    let data = conn.read_u8().await?;
    assert_eq!(data, 6);

    Ok(())
}

#[tokio::test]
async fn slow_ipv6() -> Result<()> {
    let (v4_addr, v6_addr) = happy_request(SLOW_V6).await?;
    let mut conn = tcp_connect(vec![v4_addr, v6_addr]).await?;
    assert!(conn.peer_addr()?.is_ipv4());
    let data = conn.read_u8().await?;
    assert_eq!(data, 4);

    Ok(())
}
