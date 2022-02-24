// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::task::JoinHandle;

struct Proxy {
    proxy_addr: SocketAddr,
    server_addr: SocketAddr,
}

impl Proxy {
    fn new(proxy_addr: &str, server_addr: &str) -> Self {
        Self {
            proxy_addr: proxy_addr.to_socket_addrs().unwrap().next().unwrap(),
            server_addr: server_addr.to_socket_addrs().unwrap().next().unwrap(),
        }
    }

    fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let proxy_socket_rx = Arc::new(UdpSocket::bind(self.proxy_addr).await.unwrap());
            let proxy_socket_tx = proxy_socket_rx.clone();
            let (outgoing_queue_tx, mut outgoing_queue_rx) =
                tokio::sync::mpsc::channel::<(_, Vec<u8>)>(1_000);

            tokio::spawn(async move {
                loop {
                    let (dest_addr, msg) = outgoing_queue_rx.recv().await.unwrap();
                    proxy_socket_tx.send_to(&msg, dest_addr).await;
                }
            });
            let mut buf = [0; 64 * 1024];

            loop {
                let (len, src_addr) = proxy_socket_rx.recv_from(&mut buf).await.unwrap();
                let msg = buf[..len].to_vec();
                let server_addr = self.server_addr.clone();
                let outgoing_queue_tx = outgoing_queue_tx.clone();

                tokio::spawn(async move {
                    let mut buf = [0; 64 * 1024];
                    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
                    socket.send_to(&msg, server_addr).await.unwrap();

                    if let Ok(Ok((len, _))) =
                        tokio::time::timeout(Duration::from_secs(60), socket.recv_from(&mut buf)).await
                    {
                        let msg = buf[..len].to_vec();
                        outgoing_queue_tx.send((src_addr, msg)).await.unwrap();
                    }
                });
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use tokio::net::UdpSocket;
    use tokio::task::JoinHandle;

    use super::*;

    fn echo_server(addr: String) -> JoinHandle<()> {
        tokio::spawn(async {
            let socket = UdpSocket::bind(addr).await.unwrap();
            let mut buf = [0; 1_000];
            loop {
                let (len, src_addr) = socket.recv_from(&mut buf).await.unwrap();
                let response = format!("Hello, {}!", std::str::from_utf8(&buf[..len]).unwrap());
                socket.send_to(response.as_bytes(), src_addr).await.unwrap();
            }
        })
    }

    #[tokio::test]
    async fn test_echo_server() {
        let server = echo_server("127.0.0.1:22222".to_string());

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket.send_to(b"World", "127.0.0.1:22222").await.unwrap();

        let mut buf = [0; 1_000];
        let (len, _) = socket.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..len], "Hello, World!".as_bytes());
    }

    #[tokio::test]
    async fn test_proxy() {
        let server = echo_server("127.0.0.1:33333".to_string());
        let proxy = Proxy::new("127.0.0.1:11111", "127.0.0.1:33333").spawn();

        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket.send_to(b"World", "127.0.0.1:11111").await.unwrap();

        let mut buf = [0; 1_000];
        let (len, _) = socket.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..len], "Hello, World!".as_bytes());
    }
}
