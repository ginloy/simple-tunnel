use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt, stdout};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::transport::Server;
use tonic::{Response, Status};
use tracing::{error, info};
use tunnel::tunnel_server::Tunnel;
use uuid::Uuid;

use crate::tunnel::tunnel_info::Type;
use crate::tunnel::{Connect, Packet};

pub mod tunnel {
    tonic::include_proto!("tunnel");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("tunnel_descriptor");
}

#[derive(Debug, Default, Clone)]
pub struct MyTunnel {
    connection_map: Arc<Mutex<HashMap<Uuid, tokio::net::TcpStream>>>,
}

#[tonic::async_trait]
impl Tunnel for MyTunnel {
    type ListenStream = Pin<Box<dyn Stream<Item = Result<tunnel::Connect, Status>> + Send>>;

    async fn listen(
        &self,
        request: tonic::Request<tunnel::Port>,
    ) -> std::result::Result<tonic::Response<Self::ListenStream>, tonic::Status> {
        let port = request.into_inner().port;
        let listener = TcpListener::bind(format!("0.0.0.0:{port}")).await?;
        info!("listening to port {port}");

        let (send, recv) = mpsc::channel::<Result<tunnel::Connect, Status>>(128);
        let res = ReceiverStream::new(recv);
        let c_map = self.connection_map.clone();

        tokio::spawn(async move {
            loop {
                let test = listener.accept().await;
                match test {
                    Err(e) => {
                        error!("listener accept error: {e}");
                        let _ = send.send(Err(Status::from_error(Box::new(e)))).await;
                        break;
                    }
                    Ok((s, a)) => {
                        let uuid = Uuid::new_v4();
                        info!(
                            "request connection to port {port} from {a:?}, connection with uuid {uuid} created"
                        );
                        c_map.lock().await.insert(uuid, s);
                        if let Err(e) = send
                            .send(Ok(Connect {
                                uuid: uuid.to_string(),
                            }))
                            .await
                        {
                            error!("failed to send connection info uuid {uuid} to client");
                            break;
                        }

                        // Cleanup task
                        let c_map = c_map.clone();
                        tokio::spawn(async move {
                            let mut ticker = tokio::time::interval(Duration::from_secs(1));
                            loop {
                                ticker.tick().await;
                                let c_map = c_map.try_lock();
                                if let Ok(mut m) = c_map {
                                    match m.get(&uuid) {
                                        None => break,
                                        Some(s) => {
                                            let mut buf = [0; 128];
                                            if s.peek(&mut buf).await.is_ok_and(|e| e == 0) {
                                                m.remove(&uuid);
                                            }
                                        }
                                    }
                                }
                            }
                        });
                    }
                }
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
        tokio::spawn({
            let c_map = self.connection_map.clone();
            async move {
                let mut stream_from_client = request.into_inner();
                let head = stream_from_client.message().await;
                match head {
                    Ok(Some(x)) => match x.r#type {
                        Some(Type::Connect(c)) => {
                            info!("client stream established");
                            let uuid = Uuid::parse_str(&c.uuid);
                            match uuid {
                                Err(e) => {
                                    error!("invalid uuid {} provided", c.uuid);
                                    send.send(Err(Status::invalid_argument(format!(
                                        "invalid uuid {} provided",
                                        c.uuid
                                    ))))
                                    .await;
                                }
                                Ok(i) => {
                                    let tcp_stream = c_map.lock().await.remove(&i);
                                    match tcp_stream {
                                        None => {
                                            let msg =
                                                format!("connection with uuid {i} does not exist");
                                            error!(msg);
                                            send.send(Err(Status::invalid_argument(msg))).await;
                                        }
                                        Some(tcp_stream) => {
                                            let (mut tcp_recv, mut tcp_write) =
                                                tcp_stream.into_split();

                                            tokio::spawn(async move {
                                                let mut buf = [0; 1024];
                                                loop {
                                                    let temp = tcp_recv.read(&mut buf).await;
                                                    match temp {
                                                        Ok(0) => break,
                                                        Err(_e) => break,
                                                        Ok(n) => {
                                                            info!(
                                                                "received {n} bytes from external connection"
                                                            );
                                                            let data = &buf[..n];
                                                            stdout().write_all(data).await;
                                                            if let Err(e) = send
                                                                .send(Ok(Packet {
                                                                    data: data.into(),
                                                                }))
                                                                .await
                                                            {
                                                                error!(
                                                                    "failed to send data to client, closing connection: {e}"
                                                                );
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                            });
                                            while let Some(Ok(x)) = stream_from_client.next().await
                                            {
                                                match x.r#type {
                                                    Some(Type::Packet(p)) => {
                                                        info!(
                                                            "received {} bytes from client",
                                                            &p.data.len()
                                                        );
                                                        if let Err(e) = {
                                                            stdout().write_all(&p.data).await;
                                                            tcp_write.write_all(&p.data).await
                                                        } {
                                                            error!(
                                                                "failed to send data to external connection, closing connections: {e}"
                                                            );
                                                            break;
                                                        }
                                                    }
                                                    _ => break,
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    },
                    _ => {}
                }
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
    let addr = "127.0.0.1:50051".parse()?;
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
