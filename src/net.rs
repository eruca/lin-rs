
use std::io::ErrorKind;
use std::net::SocketAddr;

use core::net::UdpSocket;
use core::reactor::Handle;
use futures::{Future, IntoFuture, Poll, Async};
use futures::stream::Stream;
use net2::UdpBuilder;
use net2::unix::UnixUdpBuilderExt;

use self::super::LinError;

const MAX_LINE_LEN: usize = 8 * 1024 * 1024;

#[derive(Debug)]
pub struct Collector {
    socket: UdpSocket,
    local: Vec<u8>,
}

impl Collector {
    pub fn bind(addr: &str, handle: &Handle) -> Collector {
        let addr = addr.parse::<SocketAddr>().unwrap();
        let builder = UdpBuilder::new_v4().expect("UdpBuilder not initial");
        let std_socket = builder.reuse_address(true)
            .expect("REUSE_ADDRESS not support in this os")
            .reuse_port(true)
            .expect("REUSE_PORT not support in this os")
            .bind(addr)
            .expect("Bind Error");
        let socket = UdpSocket::from_socket(std_socket, handle).unwrap();
        let mut vec = Vec::new();
        vec.resize(MAX_LINE_LEN, 0);
        Collector {
            socket: socket,
            local: vec,
        }
    }
}

impl Stream for Collector {
    type Item = Vec<u8>;
    type Error = LinError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if let Async::NotReady = self.socket.poll_read() {
            return Ok(Async::NotReady);
        }

        let inner = match self.socket.recv_from(&mut self.local) {
            Err(err) => {
                if err.kind() == ErrorKind::WouldBlock {
                    return Ok(Async::NotReady);
                }
                Err(err)
            }
            inner => inner,
        };

        inner.into_future()
            .map_err(|err| Into::<LinError>::into(err))
            .map(|(size, remote)| {
                debug!("receive {} byte from {}", size, remote);
                Some(Vec::from(&self.local[..size]))
            })
            .poll()
    }
}
