use log::debug;
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc,
};

use crate::codec::Message;
use std::io::Result;

pub struct BufferedDiameterParser<'s, R: 's, const MAXSIZE: usize> {
    buffer: [u8; MAXSIZE],
    buffer_size: usize,
    offset: usize,
    reader: &'s mut R,
}

impl<'s, R: tokio::io::AsyncRead + Unpin + 's, const MAXSIZE: usize>
    BufferedDiameterParser<'s, R, MAXSIZE>
{
    pub fn new(reader: &'s mut R) -> BufferedDiameterParser<'s, R, MAXSIZE> {
        Self {
            buffer: [0; MAXSIZE],
            buffer_size: 0,
            offset: 0,
            reader,
        }
    }
    pub async fn parse(&mut self) -> Result<Message> {
        loop {
            let decode_res = Message::decode(&self.buffer[self.offset..self.buffer_size], MAXSIZE)?;
            if let Some(r) = decode_res {
                self.offset += r.total_message_size();
                if self.offset == self.buffer_size {
                    self.offset = 0;
                    self.buffer_size = 0;
                }
                break Ok(r);
            }
            if self.offset != self.buffer_size {
                self.buffer.copy_within(self.offset..self.buffer_size, 0);
            }
            self.buffer_size -= self.offset;
            self.offset = 0;
            let read_res = self
                .reader
                .read(&mut self.buffer[self.buffer_size..])
                .await?;
            if read_res == 0 {
                return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
            }
            self.buffer_size += read_res;
        }
    }
}

pub struct BufferedSender {
    tx: mpsc::Sender<Vec<u8>>,
}

impl BufferedSender {
    pub fn new<W: AsyncWrite + Unpin + Send + 'static>(mut writer: W) -> Self {
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(1024);
        tokio::spawn(async move {
            while let Some(data) = rx.recv().await {
                    if let Err(e) = writer.write_all(&data).await {
                        println!("{:?}", e);
                        break;
                    }
            }
            debug!("buffered sender terminating")
        });
        Self { tx }
    }
    pub fn send(&self, data: Vec<u8>) -> Result<()> {
        match self.tx.try_send(data) {
            Ok(_) => Ok(()),
            Err(e) => match e {
                mpsc::error::TrySendError::Full(_) => Ok(()),
                mpsc::error::TrySendError::Closed(_) => {
                    Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof))
                }
            },
        }
    }
}
