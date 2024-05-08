use std::{collections::HashMap, sync::Arc};
use std::io::Result;
use codec::Message;
use codec_asyn::{BufferedDiameterParser, BufferedSender};
use log::{debug, warn};

use tokio::io::{AsyncRead, AsyncWriteExt};

mod codec;
mod codec_asyn;
mod dconst;

enum ApplicationId {
    Auth(u32),
    Acct(u32),
    VSAuth(u32, u32),
    VSAcct(u32, u32),
}

struct ClientConfigBuilder<'a, MH> {
    remote_address: &'a str,
    local_identity: &'a str,
    local_realm: &'a str,
    product_name: &'a str,
    vendor_id: u32,
    supported_vendor_id: Option<u32>,
    application_id: Vec<ApplicationId>,
    handler: MH
}
impl<'a, MH> ClientConfigBuilder<'a, MH> {
    fn new(h: MH) -> Self {
        Self {
            remote_address: "127.0.0.1:3868",
            local_identity: "warpanel.bridge.enterprise.com",
            local_realm: "bridge.enterprise.com",
            handler: h,
            product_name: "y-diameter",
            vendor_id: 0,
            supported_vendor_id: None,
            application_id: Vec::new()
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
    fn product_name(mut self, product_name: &'a str) -> Self {
        self.product_name = product_name;
        self
    }
    
    fn vendor_id(mut self, vendor_id: u32) -> Self {
        self.vendor_id = vendor_id;
        self
    }
    fn supported_vendor_id(mut self, vendor_id: u32) -> Self {
        self.supported_vendor_id = Some(vendor_id);
        self
    }
    fn auth_application_id(mut self, id: u32) -> Self {
        self.application_id.push(ApplicationId::Auth(id));
        self
    }
    fn acct_application_id(mut self, id: u32) -> Self {
        self.application_id.push(ApplicationId::Acct(id));
        self
    }
    fn vs_acct_application_id(mut self, vendor: u32, id: u32) -> Self {
        self.application_id.push(ApplicationId::VSAcct(vendor, id));
        self
    }
    fn vs_auth_application_id(mut self, vendor: u32, id: u32) -> Self {
        self.application_id.push(ApplicationId::VSAuth(vendor, id));
        self
    }
    fn build(self) -> Arc<Client<MH>> {
        Arc::new(Client {
            remote_address: self.remote_address.to_owned(),
            local_identity: self.local_identity.to_owned(),
            local_realm: self.local_realm.to_owned(),
            sender: std::sync::RwLock::new(None),
            handler: self.handler,
            contexts: RequestContexts::new(),
            product_name: self.product_name.to_owned(),
            vendor_id: self.vendor_id,
            supported_vendor_id: self.supported_vendor_id,
            application_id: self.application_id,
            connected_subscribers: std::sync::Mutex::new(Vec::new()),
            connected: std::sync::atomic::AtomicBool::new(false)
        })
    }
}

struct RequestContext {
    notificator: tokio::sync::oneshot::Sender<Message>
}
impl RequestContext {
    fn new(notificator: tokio::sync::oneshot::Sender<Message>) -> Self {
        Self {
            notificator,
        }
    }
}
struct RequestContexts {
    contexts: std::sync::Mutex<HashMap<u32, RequestContext>>,
}
impl RequestContexts {
    fn new() -> Self {
        Self {
            contexts: std::sync::Mutex::new(HashMap::new())
        }
    }
    fn add(&self, tid: u32, c: RequestContext) {
        let mut l = self.contexts.lock().unwrap();
        l.insert(tid, c);
    }
    fn get(&self, tid: u32) -> Option<RequestContext> {
        let mut l = self.contexts.lock().unwrap();
        l.remove(&tid)
    }
}

struct Client<MH> {
    remote_address: String,
    local_identity: String,
    local_realm: String,
    product_name: String,
    vendor_id: u32,
    supported_vendor_id: Option<u32>,
    application_id: Vec<ApplicationId>,

    sender: std::sync::RwLock<Option<BufferedSender>>,
    handler: MH,
    contexts: RequestContexts,

    connected_subscribers: std::sync::Mutex<Vec<tokio::sync::oneshot::Sender<()>>>,
    connected: std::sync::atomic::AtomicBool
}


trait MessageHandler {
    fn handle(&self, msg: Message, con: Arc<Client<Self>>) -> impl std::future::Future<Output = ()> + std::marker::Send where Self: Sized;
}
struct NoopHandler{}
impl MessageHandler for NoopHandler {
    async fn handle(&self, _msg: Message, _con: Arc<Client<Self>>) {
    }
}

impl<MH> Drop for Client<MH>  {
    fn drop(&mut self) {
        println!("dropped");
    }
}

impl<MH> Client<MH> where MH: MessageHandler+Send+Sync+'static {
    fn stop(&mut self) {
    }
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
            body.avp_write_string(dconst::AVP_PRODUCT_NAME, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY, &self.product_name);
            body.avp_write_u32(dconst::AVP_VENDOR_ID, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY, self.vendor_id);
            if let Some(svid) = self.supported_vendor_id {
                body.avp_write_u32(dconst::AVP_SUPPORTED_VENDOR_ID, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY, svid);
            }
            for appid in &self.application_id {
                match appid {
                    ApplicationId::Auth(id) => body.avp_write_u32(dconst::AVP_AUTH_APPLICATION_ID, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY, *id),
                    ApplicationId::Acct(id) => body.avp_write_u32(dconst::AVP_ACCT_APPLICATION_ID, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY, *id),
                    ApplicationId::VSAuth(vendor, id) => {
                        body.avp_start_group(dconst::AVP_VS_APPLICATION_ID, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY);
                        body.avp_write_u32(dconst::AVP_VENDOR_ID, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY, *vendor);
                        body.avp_write_u32(dconst::AVP_AUTH_APPLICATION_ID, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY, *id);
                        body.avp_end_group();
                    }
                    ApplicationId::VSAcct(vendor, id) => {
                        body.avp_start_group(dconst::AVP_VS_APPLICATION_ID, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY);
                        body.avp_write_u32(dconst::AVP_VENDOR_ID, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY, *vendor);
                        body.avp_write_u32(dconst::AVP_ACCT_APPLICATION_ID, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY, *id);
                        body.avp_end_group();
                    },
                }
            }
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
        self.send(buffer)
    }

    async fn process_messages<'a, R>(self: &Arc<Self>, parser: &mut BufferedDiameterParser<'a, R, {1024*100}>)
           where R : AsyncRead + Unpin {
        while let Ok(m) = parser.parse().await {
            debug!("incoming message: {:?}", m);
            if !m.is_request() {
                match self.contexts.get(m.h2h) {
                    None => {println!("ignoring unpexpected response {}", m.h2h)}
                    Some(ctx) => {let _ = ctx.notificator.send(m);}
                }
            } else if m.command == dconst::CMD_DWR {
                let _ = self.send_dwa(m).await;
            } else {
                self.handler.handle(m, self.clone()).await;
            }
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
                    let mut slocked = self.sender.write().unwrap();
                    *slocked = Some(sender);
                }
                debug!("connected");
                {
                    let mut lock = self.connected_subscribers.lock().unwrap();
                    while !lock.is_empty() {
                        if let Some(c) = lock.pop() {let _ = c.send(());}
                    }
                }
                self.process_messages(&mut parser).await;
                //while let Ok(m) = parser.parse().await {
                //}
                {
                    let mut slocked = self.sender.write().unwrap();
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
    async fn wait_connected(&self) {
        if self.connected.load(std::sync::atomic::Ordering::SeqCst) {
            return
        }
        let (not_tx, not_rx) = tokio::sync::oneshot::channel();
        {
            let mut lock = self.connected_subscribers.lock().unwrap();
            if self.connected.load(std::sync::atomic::Ordering::SeqCst) {
                return
            }
            lock.push(not_tx);
        }
        let _ = not_rx.await;
    }

    // send encoded message
    fn send(&self, buffer: Vec<u8>) -> Result<()> {
        if let  Some(slock) = self.sender.read().unwrap().as_ref() {
            slock.send(buffer)
        } else {
            Err(std::io::Error::from(std::io::ErrorKind::NotConnected))
        }
    }

    async fn send_req(&self, buffer: Vec<u8>, tid: u32) -> Result<Message> {
        let (not_tx, not_rx) = tokio::sync::oneshot::channel();
        self.contexts.add(tid, RequestContext::new(not_tx));
        self.send(buffer)?;
        match not_rx.await {
            Ok(rx) => Ok(rx),
            Err(_) => Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
        }
    }
}

struct H1 {
    name: String
}
impl MessageHandler for H1 {
    async fn handle(&self, msg: Message, con: Arc<Client<Self>>) {
        let msg = codec::Message {
            flags: codec::MSG_FLAG_PROXYABLE,
            command: msg.command,
            application: msg.application,
            e2e: msg.e2e,
            h2h: msg.h2h,
            body: Vec::with_capacity(0),
        };
        let buffer = msg.encode_with_builder(|body| {
            body.avp_write_u32(dconst::AVP_RESULT_CODE, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY, 2001);
            body.avp_write_string(dconst::AVP_ORIGIN_HOST, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY, "abc");
            body.avp_write_string(dconst::AVP_ORIGIN_REALM, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY, "cde");

        });

        if let Err(e) = con.send(buffer) {
            log::error!("send failed {:?}", e)
        }
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
        let h = H1{
            name: "aaa".to_owned()
        };

        {
            let c = ClientConfigBuilder::new(h)
                .remote_address("127.0.0.1:3868")
                .acct_application_id(1)
                .auth_application_id(2)
                .vs_auth_application_id(10, 3)
                .vs_acct_application_id(11, 4)
                .product_name("prod")
                .local_realm("realm1.com")
                .local_identity("me.realm.com")
                .build();
            c.connect();
            c.wait_connected().await;
            let msg = codec::Message {
                flags: codec::MSG_FLAG_REQUEST | codec::MSG_FLAG_PROXYABLE,
                command: dconst::CMD_AAR,
                application: 999,
                e2e: 10,
                h2h: 11,
                body: Vec::with_capacity(0),
            };
            let buffer = msg.encode_with_builder(|body| {
                body.avp_write_string(dconst::AVP_ORIGIN_HOST, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY, "abc");
                body.avp_write_string(dconst::AVP_ORIGIN_REALM, dconst::AVP_VENDOR_NONE, codec::AVP_FLAG_MANDATORY, "cde");
            });
            let resp = c.send_req(buffer, msg.h2h).await;
            println!("resp {:?}", resp);
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
        println!("block out");

        tokio::time::sleep(tokio::time::Duration::from_secs(1000)).await;
    });
}
