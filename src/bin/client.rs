use std::sync::Arc;

use pb::tunnel_client::TunnelClient;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
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

const PORTS: &[u32] = &[8080];

type SharedTunnel = Arc<Mutex<TunnelClient<Channel>>>;

async fn handle_port(client: SharedTunnel, port: u32) -> anyhow::Result<()> {
    let mut listen_stream = client
        .lock()
        .await
        .listen(Port { port })
        .await?
        .into_inner();
    while let Some(c) = listen_stream.next().await {
        match c {
            Err(e) => {
                error!("Proxy failed to listen to port {port}: {e}");
            }
            Ok(c) => {
                let client = client.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connect(client, &c.uuid, port).await {
                        error!("{e}");
                    }
                });
            }
        }
    }
    Ok(())
}

async fn handle_connect(client: SharedTunnel, uuid: &str, port: u32) -> anyhow::Result<()> {
    let (send, recv) = mpsc::channel(128);
    let (tcp_read, tcp_write) = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await?
        .into_split();
    let stream = ReceiverStream::from(recv);

    let temp = client
        .lock()
        .await
        .create_tunnel(stream)
        .await?
        .into_inner();

    send.send(TunnelInfo {
        r#type: Some(Type::Connect(Connect {
            uuid: uuid.to_string(),
        })),
    })
    .await?;
    Ok(())
}

async fn pipe_to_proxy(
    mut tcp_read: OwnedReadHalf,
    send: mpsc::Sender<TunnelInfo>,
) -> anyhow::Result<()> {
    let mut buf = [0; 1024];
    let mut tcp_read = BufReader::new(tcp_read);
    loop {
        let bytes_read = tcp_read.read(&mut buf).await?;
        let data = &buf[..bytes_read];
        if bytes_read == 0 {
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
    let mut tcp_send = BufWriter::new(tcp_send);
    while let Some(p) = recv.next().await {
        let data = p?.data;
        tcp_send.write_all(&data).await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber)?;

    let client = Arc::new(Mutex::new(
        TunnelClient::connect("http://127.0.0.1:50051").await?,
    ));
    let mut j = JoinSet::new();
    for p in PORTS {
        j.spawn({
            let client = client.clone();
            async move {
                let temp = client.lock().await.listen(Port { port: *p }).await;
                match temp {
                    Err(_e) => (),
                    Ok(s) => {
                        let mut s = s.into_inner();
                        while let Some(Ok(c)) = s.next().await {
                            tokio::spawn({
                                let client = client.clone();
                                let (send, recv) = mpsc::channel::<TunnelInfo>(128);
                                let stream = ReceiverStream::from(recv);
                                async move {
                                    let temp = client.lock().await.create_tunnel(stream).await;
                                    match temp {
                                        Err(_e) => {}
                                        Ok(r) => {
                                            let tcp =
                                                TcpStream::connect(format!("127.0.0.1:8000")).await;
                                            match tcp {
                                                Err(_e) => {}
                                                Ok(t) => {
                                                    let (mut tcp_read, mut tcp_write) =
                                                        t.into_split();
                                                    let _ = send
                                                        .send(TunnelInfo {
                                                            r#type: Some(Type::Connect(c)),
                                                        })
                                                        .await;
                                                    let mut r = r.into_inner();

                                                    tokio::spawn({
                                                        let send = send.clone();
                                                        async move {
                                                            let mut buf = [0; 1024];
                                                            loop {
                                                                let temp =
                                                                    tcp_read.read(&mut buf).await;
                                                                match temp {
                                                                    Ok(0) => break,
                                                                    Err(_e) => break,
                                                                    Ok(n) => {
                                                                        info!("received {n} bytes from external server");
                                                                        let data = &buf[..n];
                                                                        match send.send(TunnelInfo {
                                                                            r#type: Some(
                                                                                Type::Packet(
                                                                                    Packet {
                                                                                        data: data
                                                                                            .into(),
                                                                                    },
                                                                                ),
                                                                            ),
                                                                        })
                                                                        .await {
                                                                            Err(_e) => break,
                                                                            Ok(_)=> continue
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    });
                                                    while let Some(Ok(p)) = r.next().await {
                                                        info!(
                                                            "received {} bytes from proxy server",
                                                            p.data.len()
                                                        );
                                                        match tcp_write.write_all(&p.data).await {
                                                            Err(_e) => break,
                                                            Ok(_) => continue,
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            }
        });
    }
    j.join_all().await;
    Ok(())
}
