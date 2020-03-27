use crate::common::*;
use crate::{KvError, Result};
use serde::Deserialize;
use serde_json::de::IoRead;
use serde_json::Deserializer;
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpStream, ToSocketAddrs};

pub struct KvClient {
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl KvClient {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let tcp_in_stream = TcpStream::connect(addr)?;
        let tcp_out_stream = tcp_in_stream.try_clone()?;

        Ok(KvClient {
            reader: Deserializer::from_reader(BufReader::new(tcp_in_stream)),
            writer: BufWriter::new(tcp_out_stream),
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        serde_json::to_writer(&mut self.writer, &Request::Get { key })?;
        self.writer.flush()?;
        let rsp = GetResponse::deserialize(&mut self.reader)?;
        match rsp {
            GetResponse::Ok(value) => Ok(value),
            GetResponse::Err(msg) => Err(KvError::StringError(msg)),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Set { key, value })?;
        self.writer.flush()?;
        let rsp = SetResponse::deserialize(&mut self.reader)?;
        match rsp {
            SetResponse::Ok(_) => Ok(()),
            SetResponse::Err(msg) => Err(KvError::StringError(msg)),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Remove { key })?;
        self.writer.flush()?;
        let rsp = RemoveResponse::deserialize(&mut self.reader)?;
        match rsp {
            RemoveResponse::Ok(_) => Ok(()),
            RemoveResponse::Err(msg) => Err(KvError::StringError(msg)),
        }
    }
}
