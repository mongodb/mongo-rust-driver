use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::runtime::stream::tcp_connect;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type Result<T> = anyhow::Result<T>;

static CONTROL: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 10036);
static IPV4: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 10037);
static IPV6: SocketAddr = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 10038);

#[tokio::test]
async fn slow_ipv4() -> Result<()> {
    let mut control = tcp_connect(vec![CONTROL.clone()]).await?;
    control.write_u8(4).await?;
    let resp = control.read_u8().await?;
    assert_eq!(resp, 1);

    let mut conn = tcp_connect(vec![IPV4.clone(), IPV6.clone()]).await?;
    assert!(conn.peer_addr()?.is_ipv6());
    let data = conn.read_u8().await?;
    assert_eq!(data, 6);

    Ok(())
}
