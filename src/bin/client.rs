use std::{
    env,
    sync::{Arc, LazyLock},
};

use anyhow::Context;
use pb::tunnel_client::TunnelClient;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    select,
    sync::{Mutex, mpsc},
    task::JoinSet,
};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{Streaming, transport::Channel};
use tracing::{error, info};

use crate::pb::{Connect, Packet, Port, TunnelInfo, tunnel_info::Type};

pub mod pb {
    tonic::include_proto!("tunnel");
}

const PORTS: &[u32] = &[8080, 8006];
static HOST: LazyLock<String> =
    LazyLock::new(|| env::var("HOST").unwrap_or("127.0.0.1".to_string()));

type SharedTunnel = Arc<Mutex<TunnelClient<Channel>>>;

async fn handle_port(client: SharedTunnel, port: u32) -> anyhow::Result<()> {
    let mut listen_stream = client
        .lock()
        .await
        .listen(Port { port })
        .await?
        .into_inner();
    let mut j = JoinSet::new();
    while let Some(c) = listen_stream.next().await {
        match c {
            Err(e) => {
                error!(error = ?e, "server error");
            }
            Ok(c) => {
                info!(uuid = c.uuid, "received connection request");
                let client = client.clone();
                j.spawn(async move {
                    if let Err(e) = handle_connect(client, c, port).await {
                        error!(error = ?e, "tunnel error");
                    }
                });
            }
        }
    }
    j.join_all().await;
    Ok(())
}

async fn handle_connect(client: SharedTunnel, connect: Connect, _port: u32) -> anyhow::Result<()> {
    let (send, recv) = mpsc::channel(128);
    let addr = format!("{}:8000", *HOST);
    let (tcp_read, tcp_write) = TcpStream::connect(&addr).await?.into_split();
    info!(address = addr, "connected");
    let stream = ReceiverStream::from(recv);

    let server_stream = client
        .lock()
        .await
        .create_tunnel(stream)
        .await?
        .into_inner();

    info!(address = addr, "tunnel established");

    let pipe1 = {
        let send = send.clone();
        async move { pipe_to_proxy(tcp_read, send).await }
    };

    let pipe2 = pipe_to_connection(tcp_write, server_stream);

    send.send(TunnelInfo {
        r#type: Some(Type::Connect(connect.clone())),
    })
    .await?;
    info!(uuid = connect.uuid, "sent connection header");
    info!(uuid = connect.uuid, "started forwarding");
    select! {
        r = pipe1 => r?,
        r = pipe2 => r?
    }
    Ok(())
}

async fn pipe_to_proxy(
    mut tcp_read: OwnedReadHalf,
    send: mpsc::Sender<TunnelInfo>,
) -> anyhow::Result<()> {
    let mut buf = [0; 1024];
    loop {
        let bytes_read = tcp_read.read(&mut buf).await?;
        let data = &buf[..bytes_read];
        if bytes_read == 0 {
            info!(address = ?tcp_read.peer_addr(), "EOF reached");
            break;
        }
        send.send(TunnelInfo {
            r#type: Some(Type::Packet(Packet { data: data.into() })),
        })
        .await?;
    }
    Ok(())
}

async fn pipe_to_connection(
    mut tcp_send: OwnedWriteHalf,
    mut recv: Streaming<Packet>,
) -> anyhow::Result<()> {
    while let Some(p) = recv.next().await {
        let data = p?.data;
        tcp_send.write_all(&data).await?;
    }
    info!(address = ?tcp_send.peer_addr(), "EOF reached");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber)?;

    let server = env::var("SERVER_ADDR").context("SERVER_ADDR not set")?;
    let client = Arc::new(Mutex::new(TunnelClient::connect(server).await?));
    let mut j = JoinSet::new();
    for p in PORTS {
        let client = client.clone();
        j.spawn(handle_port(client, *p));
    }
    j.join_all().await;
    Ok(())
}
