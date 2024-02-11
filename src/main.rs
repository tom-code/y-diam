use std::sync::Arc;
use std::io::Result;
use codec::Message;
use codec_asyn::{BufferedDiameterParser, BufferedSender};
use log::{debug, warn};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

mod codec;
mod codec_asyn;
mod dconst;



struct ClientConfigBuilder<'a> {
    remote_address: &'a str,
    local_identity: &'a str,
    local_realm: &'a str,
    handler: Box<dyn MessageHandler + Sync + Send>
}
impl<'a> ClientConfigBuilder<'a> {
    fn new(h: Box<dyn MessageHandler + Sync + Send>) -> Self {
        Self {
            remote_address: "127.0.0.1:3868",
            local_identity: "warpanel.bridge.enterprise.com",
            local_realm: "bridge.enterprise.com",
            handler: h

        }
    }
    fn remote_address(mut self, address: &'a str) -> Self {
        self.remote_address = address;
        self
    }
    fn local_identity(mut self, local_identity: &'a str) -> Self {
        self.local_identity = local_identity;
        self
    }
    fn local_realm(mut self, local_realm: &'a str) -> Self {
        self.local_realm = local_realm;
        self
    }
    fn build(self) -> Arc<Client> {
        Arc::new(Client {
            remote_address: self.remote_address.to_owned(),
            local_identity: self.local_identity.to_owned(),
            local_realm: self.local_realm.to_owned(),
            sender: tokio::sync::RwLock::new(None),
            handler: self.handler
        })
    }
}


struct Client {
    remote_address: String,
    local_identity: String,
    local_realm: String,
    sender: tokio::sync::RwLock<Option<BufferedSender>>,
    handler: Box<dyn MessageHandler + Sync + Send>
}


trait MessageHandler {
    fn handle(&self, msg: Message, con: Arc<Client>);
}
struct NoopHandler{}
impl MessageHandler for NoopHandler {
    fn handle(&self, msg: Message, con: Arc<Client>) {
    }
}

impl Client {
    async fn send_cer(&self, w: &mut (impl tokio::io::AsyncWrite + Unpin)) -> Result<()> {
        let msg = codec::Message {
            flags: codec::MSG_FLAG_REQUEST,
            command: dconst::CMD_CER,
            application: 0,
            e2e: 0,
            h2h: 0,
            body: Vec::with_capacity(0),
        };
        let buffer = msg.encode_with_builder(|body| {
            body.avp_write_string(dconst::AVP_ORIGIN_HOST, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY, &self.local_identity);
            body.avp_write_string(dconst::AVP_ORIGIN_REALM, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY, &self.local_realm);
        });
        w.write_all(&buffer).await?;
        Ok(())
    }
    async fn send_dwa(&self, msg: Message) -> Result<()> {
        let msg = codec::Message {
            flags: 0,
            command: dconst::CMD_DWR,
            application: 0,
            e2e: msg.e2e,
            h2h: msg.h2h,
            body: Vec::with_capacity(0),
        };
        let buffer = msg.encode_with_builder(|body| {
            body.avp_write_string(dconst::AVP_ORIGIN_HOST, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY, &self.local_identity);
            body.avp_write_string(dconst::AVP_ORIGIN_REALM, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY, &self.local_realm);
            body.avp_write_u32(dconst::AVP_RESULT_CODE, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY, 2001);
        });
        let lsender = self.sender.read().await;
        if let Some(sender) = lsender.as_ref() {
            sender.send(buffer)?;
        }
        Ok(())
    }

    async fn process_messages<'a, R>(self: &Arc<Self>, parser: &mut BufferedDiameterParser<'a, R, {1024*100}>)
           where R : AsyncRead + Unpin {
        while let Ok(m) = parser.parse().await {
            debug!("incoming message: {:?}", m);
            self.handler.handle(m, self.clone());
            //if m.command == dconst::CMD_DWR {
            //    self.send_dwa(m).await;
           // }
        }
    }

    fn connect_i(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                let res = tokio::net::TcpStream::connect(self.remote_address.clone()).await;
                let stream = match res {
                    Err(e) => {
                        log::warn!("connect error {} {}", self.remote_address, e);
                        tokio::time::sleep(core::time::Duration::from_secs(1)).await;
                        continue;
                    }
                    Ok(stream) => stream,
                };
                let (mut rd, mut wr) = tokio::io::split(stream);
                let mut parser: BufferedDiameterParser<'_, _, {1024*100}> = BufferedDiameterParser::new(&mut rd);
                if let Err(e) = self.send_cer(&mut wr).await {
                    warn!("cer send failed: {:?}", e);
                    continue;
                }
                let cer_response = match parser.parse().await {
                    Ok(resp) => resp,
                    Err(e) => {
                        warn!("can't decode cer response {:?}", e);
                        continue
                    }
                };
                if cer_response.command != dconst::CMD_CER {
                    warn!("unexpected command instead of cer response {}", cer_response.command);
                    continue
                }
                let sender = BufferedSender::new(wr);
                {
                    let mut slocked = self.sender.write().await;
                    *slocked = Some(sender);
                }
                debug!("connected");
                self.process_messages(&mut parser).await;
                //while let Ok(m) = parser.parse().await {
                //}
                {
                    let mut slocked = self.sender.write().await;
                    *slocked = None;
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        });
    }
    fn connect(self: &Arc<Self>) {
        let me = self.clone();
        me.connect_i();
    }
}

struct H1 {
    name: String,
    connection: Option<Arc<Client>>
}
impl MessageHandler for H1 {
    fn handle(&self, msg: Message, con: Arc<Client>) {
        println!("1 {}", self.name);
        println!("2 {}", self.name);

    }
}
fn main() {
    env_logger::Builder::new()
        .target(env_logger::Target::Stdout)
        .filter_level(log::LevelFilter::Trace)
        .format_timestamp(Some(env_logger::TimestampPrecision::Millis))
        .init();



    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let mut h = Box::new(H1{
            name: "aaa".to_owned(),
            connection: None
        });

        let c = ClientConfigBuilder::new(h)
            .remote_address("127.0.0.1:3868")
            .build();

        c.connect();
        println!("connect out");
        tokio::time::sleep(tokio::time::Duration::from_secs(1000)).await;
    });
}
