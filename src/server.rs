use crate::common::*;
use crate::{KvEngine, Result};
use serde_json::Deserializer;
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};

pub struct KvServer<E: KvEngine> {
    engine: E,
}

impl<E: KvEngine> KvServer<E> {
    pub fn new(engine: E) -> Self {
        KvServer { engine }
    }
    pub fn run<A: ToSocketAddrs>(mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) = self.serve(stream) {
                        error!("Error on serving client: {}", e);
                    }
                }
                Err(e) => error!("connection failed: {}", e),
            }
        }
        Ok(())
    }
    pub fn serve(&mut self, tcp: TcpStream) -> Result<()> {
        let peer_addr = tcp.peer_addr()?;
        let reader = BufReader::new(&tcp);
        let mut writer = BufWriter::new(&tcp);
        let req_reader = Deserializer::from_reader(reader).into_iter::<Request>();

        macro_rules! send_rsp {
            ($rsp:expr) => {{
                let rsp = $rsp;
                serde_json::to_writer(&mut writer, &rsp)?;
                writer.flush()?;
                debug!("Response sent to {}: {:?}", peer_addr, rsp);
            };};
        }
        for req in req_reader {
            let req = req?;
            debug!("Receive request from {}: {:?}", peer_addr, req);
            match req {
                Request::Get { key } => send_rsp!(match self.engine.get(key) {
                    Ok(value) => GetResponse::Ok(value),
                    Err(e) => GetResponse::Err(format!("{}", e)),
                }),
                Request::Set { key, value } => send_rsp!(match self.engine.set(key, value) {
                    Ok(_) => SetResponse::Ok(()),
                    Err(e) => SetResponse::Err(format!("{}", e)),
                }),
                Request::Remove { key } => send_rsp!(match self.engine.remove(key) {
                    Ok(_) => RemoveResponse::Ok(()),
                    Err(e) => RemoveResponse::Err(format!("{}", e)),
                }),
            }
        }
        Ok(())
    }
}
