use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Context;
use anyhow::anyhow;
use anyhow::bail;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::transport::Server;
use tonic::{Response, Status, Streaming};
use tracing::warn;
use tracing::{error, info};
use tunnel::tunnel_server::Tunnel;
use uuid::Uuid;

use crate::tunnel::tunnel_info::Type;
use crate::tunnel::{Connect, Packet, TunnelInfo};

pub mod tunnel {
    tonic::include_proto!("tunnel");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("tunnel_descriptor");
}

type ConnectionMap = Arc<Mutex<HashMap<Uuid, TcpStream>>>;

#[derive(Debug, Default, Clone)]
pub struct MyTunnel {
    connection_map: ConnectionMap,
}

impl MyTunnel {
    async fn handle_listen(
        c_map: ConnectionMap,
        port: u32,
        send: Sender<Result<Connect, Status>>,
    ) -> anyhow::Result<()> {
        let listener = TcpListener::bind(format!("0.0.0.0:{port}")).await?;
        info!(port, "listening");

        let mut active_connections = Vec::new();
        loop {
            let c_map = c_map.clone();
            tokio::select! {
                _ = send.closed() => {
                    warn!(port, "broken stream with agent");
                    break;
                },
                r = listener.accept() => {
                    match r {
                        Err(e) => {
                            error!(port, error = ?e, "failed to accept connection");
                        }
                        Ok((tcp_stream, addr)) => {
                            info!(address = ?addr, "connection incoming");
                            let uuid = Uuid::new_v4();
                            c_map.lock().await.insert(uuid, tcp_stream);
                            active_connections.push(uuid);
                            tokio::spawn({
                                let send = send.clone();
                                async move {
                                    if let Err(e) = send
                                        .send(Ok(Connect {
                                            uuid: uuid.to_string(),
                                        }))
                                        .await
                                    {
                                        error!(error = ?e, "broken stream with agent");
                                    }
                                }
                            });
                        }
                    }
                }
            }
        }
        let mut c_map = c_map.lock().await;
        for c in active_connections {
            c_map.remove(&c);
        }
        Ok(())
    }

    async fn handle_tunnel_create(
        c_map: ConnectionMap,
        send: Sender<Result<Packet, Status>>,
        mut recv: Streaming<TunnelInfo>,
    ) -> anyhow::Result<()> {
        let Some(Ok(TunnelInfo {
            r#type: Some(Type::Connect(c)),
        })) = recv.next().await
        else {
            bail!("invalid first message, connection info missing");
        };
        let uuid =
            Uuid::parse_str(&c.uuid).with_context(|| format!("failed to parse uuid {}", c.uuid))?;
        let tcp_stream = c_map
            .lock()
            .await
            .remove(&uuid)
            .context(format!("connection with uuid {uuid} does not exist"))?;
        let (mut tcp_read, mut tcp_write) = tcp_stream.into_split();
        let read_task = async move {
            let mut buf = [0; 1024];
            loop {
                match tcp_read.read(&mut buf).await {
                    Ok(0) => {
                        info!("connection {uuid} EOF reached, ending connection");
                        break;
                    }
                    Ok(n) => {
                        let data = &buf[..n];
                        send.send(Ok(Packet { data: data.into() }))
                            .await
                            .with_context(|| format!("connection {uuid} broken tunnel"))?;
                    }
                    Err(e) => bail!(
                        anyhow!(e).context(format!("connection {uuid} failed to read tcp stream"))
                    ),
                };
            }
            Ok(())
        };
        let write_task = async move {
            while let Some(r) = recv.next().await {
                match r {
                    Ok(TunnelInfo {
                        r#type: Some(Type::Packet(p)),
                    }) => {
                        let data = p.data;
                        tcp_write.write_all(&data).await.with_context(|| {
                            format!("connection {uuid} error writing to tcp stream")
                        })?;
                    }
                    Ok(TunnelInfo {
                        r#type: Some(Type::Connect(_)),
                    }) => {
                        bail!(
                            "connection {uuid} invalid message from tunnel, expected packet, received connect"
                        );
                    }
                    Ok(TunnelInfo { r#type: None }) => {
                        bail!("connection {uuid} missing message from tunnel");
                    }
                    Err(e) => bail!(anyhow!(e)),
                }
            }
            Ok(())
        };
        select! {
            r = read_task => r?,
            r = write_task => r?
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl Tunnel for MyTunnel {
    type ListenStream = Pin<Box<dyn Stream<Item = Result<tunnel::Connect, Status>> + Send>>;

    async fn listen(
        &self,
        request: tonic::Request<tunnel::Port>,
    ) -> std::result::Result<tonic::Response<Self::ListenStream>, tonic::Status> {
        let port = request.into_inner().port;

        let (send, recv) = mpsc::channel::<Result<tunnel::Connect, Status>>(128);
        let res = ReceiverStream::new(recv);

        let c_map = self.connection_map.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::handle_listen(c_map, port, send).await {
                error!(port, error = ?e , "listen error");
            }
        });

        Ok(Response::new(Box::pin(res)))
    }

    type CreateTunnelStream =
        Pin<Box<dyn Stream<Item = std::result::Result<tunnel::Packet, Status>> + Send>>;

    async fn create_tunnel(
        &self,
        request: tonic::Request<tonic::Streaming<tunnel::TunnelInfo>>,
    ) -> std::result::Result<tonic::Response<Self::CreateTunnelStream>, tonic::Status> {
        info!("received tunnel creation request");
        let (send, recv) = mpsc::channel(128);
        let ret = ReceiverStream::from(recv);
        let c_map = self.connection_map.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::handle_tunnel_create(c_map, send, request.into_inner()).await {
                error!(error = ?e, "tunnel error");
            }
        });
        Ok(Response::new(Box::pin(ret)))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber)?;

    let service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(tunnel::FILE_DESCRIPTOR_SET)
        .build_v1()?;
    let addr = "0.0.0.0:50051".parse()?;
    let tunnel_service = MyTunnel {
        connection_map: Arc::new(Mutex::new(HashMap::new())),
    };

    Server::builder()
        .add_service(service)
        .add_service(tunnel::tunnel_server::TunnelServer::new(tunnel_service))
        .serve(addr)
        .await?;

    Ok(())
}
