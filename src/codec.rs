use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Result;
use std::io::Seek;
use std::io::SeekFrom;
use std::str::from_utf8;
use tokio_util::bytes::Buf;

pub const MSG_FLAG_REQUEST: u8 = 0x80;
pub const MSG_FLAG_PROXYABLE: u8 = 0x40;

pub const AVP_FLAG_VENDOR: u8 = 0x80;
pub const AVP_FLAG_MANDATORY: u8 = 0x40;

/// Diameter message object with parsed header parameters and body as vector of bytes.
#[derive(Debug, Clone)]
pub struct Message {
    pub flags: u8,
    pub command: u32,
    pub application: u32,
    pub h2h: u32,
    pub e2e: u32,
    pub body: Vec<u8>,
}

impl Message {
    pub fn is_request(&self) -> bool {
        matches!(self.flags & MSG_FLAG_REQUEST, MSG_FLAG_REQUEST)
    }

    /// Encode diameter header
    pub fn encode_header(&self, body_size: usize) -> Vec<u8> {
        let mut vec = Vec::with_capacity(body_size + 32);
        let _ = vec.write_u8(1);
        let len = body_size + 20;
        vec.write_3byte_int(len as u32);
        let _ = vec.write_u8(self.flags);
        vec.write_3byte_int(self.command);
        let _ = vec.write_u32::<BigEndian>(self.application);
        let _ = vec.write_u32::<BigEndian>(self.h2h);
        let _ = vec.write_u32::<BigEndian>(self.e2e);
        vec
    }

    /// Encode full diameter message
    pub fn encode(&self) -> Vec<u8> {
        let mut vec = self.encode_header(self.body.len());
        vec.extend_from_slice(self.body.as_slice());
        vec
    }

    /// Encode diameter message. Ignore body member of message and call supplied closure,
    /// which allows user to add avps during encoding.
    pub fn encode_with_builder<F>(&self, avp_encoder: F) -> Vec<u8> where F: Fn(&mut AvpInnerGroupBuilder){
        let mut buffer = self.encode_header(1024);
        avp_encoder(&mut AvpInnerGroupBuilder::new(&mut buffer));
        Self::fix_size(&mut buffer);
        buffer
    }

    /// Correct size of encoded diamete message.
    /// This assumes that buffer.len() is correct total length of message.
    fn fix_size(buffer: &mut [u8]) {
        if buffer.len() < 20 {
            return
        }
        let size = buffer.len();
        buffer[1] = (size >> 16) as u8;
        buffer[2] = (size >> 8) as u8;
        buffer[3] = size as u8;
    }

    /// Total_message_size returns number of bytes message occupies when encoded.
    /// This is usefull to understand how much bytes decode function did use.
    pub fn total_message_size(&self) -> usize {
        self.body.len() + 20
    }

    /// Decode diameter message from supplied array of bytes
    ///
    /// # Return value
    ///
    /// This function returns:
    ///
    /// - [`Ok(Some(Message))`] if message was sucesfully parsed
    /// - [`Ok(None)`] if more data are needed to decode message
    /// - [`Err(Error)`] if there is unrecoverable error leading to unability to parse message
    ///
    pub fn decode(input: &[u8], maxsize: usize) -> Result<Option<Self>> {
        if input.len() < 20 {
            // not enough data
            return Ok(None);
        }
        let len = ((input[1] as u32) << 16 | (input[2] as u32) << 8 | (input[3] as u32)) as usize;
        if len > maxsize {
            // message too large
            return Err(std::io::Error::from(std::io::ErrorKind::OutOfMemory));
        }
        if input.len() < len {
            // we don't have full message yet
            return Ok(None);
        }
        let mut reader = Cursor::new(input);
        reader.advance(4);
        let flags = reader.read_u8()?;
        let command: u32 = (reader.read_u8()? as u32) << 16
            | (reader.read_u8()? as u32) << 8
            | (reader.read_u8()? as u32);
        let application = reader.read_u32::<BigEndian>()?;
        let h2h = reader.read_u32::<BigEndian>()?;
        let e2e = reader.read_u32::<BigEndian>()?;
        let mut body = vec![0; len - 20];
        reader.read_exact(&mut body)?;
        Ok(Some(Message {
            flags,
            command,
            application,
            e2e,
            h2h,
            body,
        }))
    }
}

trait DiamUtilInternal {
    fn write_3byte_int(&mut self, n: u32);
    fn write_avp_header(&mut self, code: u32, vendor: u32, flags: u8, len: u32);
}

impl DiamUtilInternal for Vec<u8> {
    fn write_3byte_int(&mut self, n: u32) {
        let _ = self.write_u8((n >> 16) as u8);
        let _ = self.write_u8((n >> 8) as u8);
        let _ = self.write_u8((n) as u8);
    }
    fn write_avp_header(&mut self, code: u32, vendor: u32, flags: u8, len: u32) {
        let _ = self.write_u32::<BigEndian>(code);
        let _ = self.write_u8(flags); // flags
        let l = if (flags & AVP_FLAG_VENDOR) == AVP_FLAG_VENDOR {
            len + 12
        } else {
            len + 8
        };
        self.write_3byte_int(l);
        if flags & AVP_FLAG_VENDOR == AVP_FLAG_VENDOR {
            let _ = self.write_u32::<BigEndian>(vendor);
        }
    }
}

pub trait DiamUtilAvp {
    fn avp_write_string(&mut self, code: u32, vendor: u32, flags: u8, value: &str);
    fn avp_write_u32(&mut self, code: u32, vendor: u32, flags: u8, value: u32);
    fn avp_write_raw(&mut self, code: u32, vendor: u32, flags: u8, value: &[u8]);
}

impl DiamUtilAvp for Vec<u8> {
    fn avp_write_string(&mut self, code: u32, vendor: u32, flags: u8, value: &str) {
        avp_write_string(self, code, vendor, flags, value)
    }
    fn avp_write_u32(&mut self, code: u32, vendor: u32, flags: u8, value: u32) {
        avp_write_u32(self, code, vendor, flags, value)
    }
    fn avp_write_raw(&mut self, code: u32, vendor: u32, flags: u8, value: &[u8]) {
        avp_write_raw(self, code, vendor, flags, value)
    }
}

pub struct AvpGroupBuilder(Vec<u8>);
impl AvpGroupBuilder {
    pub fn new() -> Self {
        Self(Vec::with_capacity(512))
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl Default for AvpGroupBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl From<AvpGroupBuilder> for Vec<u8> {
    fn from(value: AvpGroupBuilder) -> Self {
        value.0
    }
}

impl DiamUtilAvp for AvpGroupBuilder {
    fn avp_write_string(&mut self, code: u32, vendor: u32, flags: u8, value: &str) {
        avp_write_string(&mut self.0, code, vendor, flags, value)
    }
    fn avp_write_u32(&mut self, code: u32, vendor: u32, flags: u8, value: u32) {
        avp_write_u32(&mut self.0, code, vendor, flags, value)
    }
    fn avp_write_raw(&mut self, code: u32, vendor: u32, flags: u8, value: &[u8]) {
        avp_write_raw(&mut self.0, code, vendor, flags, value)
    }
}

pub struct AvpInnerGroupBuilder<'a> {
    pub buffer: &'a mut Vec<u8>,
    start_grp_stack: Vec<usize>
}

impl <'a>AvpInnerGroupBuilder<'a> {
    pub fn new(buffer: &'a mut Vec<u8>) -> Self {
        Self {
            buffer,
            start_grp_stack: Vec::with_capacity(4)
        }
    }
    pub fn avp_start_group(&mut self, code: u32, vendor: u32, flags: u8) {
        self.start_grp_stack.push(self.buffer.len());
        self.buffer.write_avp_header(code, vendor, flags, 0);
    }
    pub fn avp_end_group(&mut self) {
        let start_offset = match self.start_grp_stack.pop() {
            Some(v) => v,
            None => return
        };
        let len = self.buffer.len() - start_offset;
        self.buffer[start_offset+5] = (len >> 16) as u8;
        self.buffer[start_offset+6] = (len >> 8) as u8;
        self.buffer[start_offset+7] = len as u8;
    }

    pub fn avp_write_string(&mut self, code: u32, vendor: u32, flags: u8, value: &str) {
        avp_write_string(self.buffer, code, vendor, flags, value)
    }
    pub fn avp_write_u32(&mut self, code: u32, vendor: u32, flags: u8, value: u32) {
        avp_write_u32(self.buffer, code, vendor, flags, value)
    }
    pub fn avp_write_raw(&mut self, code: u32, vendor: u32, flags: u8, value: &[u8]) {
        avp_write_raw(self.buffer, code, vendor, flags, value)
    }
    pub fn append_raw_data(&mut self, value: &[u8]) {
        self.buffer.extend_from_slice(value)
    }
}
impl <'a> Drop for AvpInnerGroupBuilder<'a> {
    fn drop(&mut self) {
        while !self.start_grp_stack.is_empty() {
            self.avp_end_group()
        }
    }
}


const ZEROES: [u8; 3] = [0, 0, 0];
fn pad(buf: &mut Vec<u8>) {
    match buf.len() % 4 {
        0 => {}
        1 => buf.extend_from_slice(&ZEROES[..]),
        2 => buf.extend_from_slice(&ZEROES[0..2]),
        3 => {
            buf.write_u8(0).unwrap();
        }
        _ => {}
    }
}

pub fn avp_write_string(buf: &mut Vec<u8>, code: u32, vendor: u32, flags: u8, value: &str) {
    let valbytes = value.as_bytes();
    avp_write_raw(buf, code, vendor, flags, valbytes)
}

pub fn avp_write_u32(buf: &mut Vec<u8>, code: u32, vendor: u32, flags: u8, value: u32) {
    buf.write_avp_header(code, vendor, flags, 4);
    let _ = buf.write_u32::<BigEndian>(value);
}

pub fn avp_write_raw(buf: &mut Vec<u8>, code: u32, vendor: u32, flags: u8, value: &[u8]) {
    buf.write_avp_header(code, vendor, flags, value.len() as u32);
    buf.extend_from_slice(value);
    pad(buf);
}

#[derive(Debug)]
pub struct Avp<'a> {
    code: u32,
    vendor: u32,
    raw_data: &'a [u8],
}

#[derive(Debug)]
pub struct AvpList<'a> {
    avps: Vec<Avp<'a>>,
}

impl Avp<'_> {
    pub fn as_str(&self) -> Result<&str> {
        match from_utf8(self.raw_data) {
            Ok(v) => Ok(v),
            Err(_) => Err(Error::from(ErrorKind::InvalidData)),
        }
    }
    pub fn as_bytes(&self) -> &[u8] {
        self.raw_data
    }
    pub fn as_u32(&self) -> Result<u32> {
        if self.raw_data.len() != 4 {
            Err(Error::from(ErrorKind::InvalidData))
        } else {
            Ok((self.raw_data[0] as u32) << 24
                | (self.raw_data[1] as u32) << 16
                | (self.raw_data[2] as u32) << 8
                | self.raw_data[3] as u32)
        }
    }
    pub fn as_group(&self) -> Result<AvpList> {
        parse_avp_group(self.raw_data)
    }
}

impl AvpList<'_> {
    pub fn get_grp(&self, code: u32, vendor: u32) -> Option<AvpList> {
        for a in &self.avps {
            if (a.vendor == vendor) && (a.code == code) {
                return match a.as_group() {
                    Ok(n) => Some(n),
                    Err(_) => None,
                };
            }
        }
        None
    }
    pub fn get_u32(&self, code: u32, vendor: u32) -> Option<u32> {
        for a in &self.avps {
            if (a.vendor == vendor) && (a.code == code) && (a.raw_data.len() == 4) {
                return match a.as_u32() {
                    Ok(n) => Some(n),
                    Err(_) => None,
                };
            }
        }
        None
    }
    pub fn get_string(&self, code: u32, vendor: u32) -> Option<String> {
        for a in &self.avps {
            if (a.vendor == vendor) && (a.code == code) {
                return match a.as_str() {
                    Ok(n) => Some(n.to_owned()),
                    Err(_) => None,
                };
            }
        }
        None
    }
    pub fn get_str(&self, code: u32, vendor: u32) -> Option<&str> {
        for a in &self.avps {
            if (a.vendor == vendor) && (a.code == code) {
                return match a.as_str() {
                    Ok(n) => Some(n),
                    Err(_) => None,
                };
            }
        }
        None
    }
    pub fn get_raw(&self, code: u32, vendor: u32) -> Option<&[u8]> {
        for a in &self.avps {
            if (a.vendor == vendor) && (a.code == code) {
                return Some(a.raw_data);
            }
        }
        None
    }
    pub fn as_slice(&self) -> &[Avp] {
        self.avps.as_slice()
    }
}

impl<'a> IntoIterator for AvpList<'a> {
    type Item = Avp<'a>;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.avps.into_iter()
    }
}

pub fn parse_avp_group(data: &[u8]) -> Result<AvpList> {
    let mut avps: AvpList = AvpList { avps: Vec::new() };
    let mut cursor = std::io::Cursor::new(data);
    while cursor.remaining() > 3 {
        let code = cursor.read_u32::<BigEndian>()?;
        let flags = cursor.read_u8()?;
        let len: u32 = (cursor.read_u8()? as u32) << 16
            | (cursor.read_u8()? as u32) << 8
            | (cursor.read_u8()? as u32);
        let (vendor, hlen) = match flags & 0x80 {
            0x80 => (cursor.read_u32::<BigEndian>()?, 12),
            _ => (0, 8),
        };
        let datalen = (len - hlen) as usize;
        let p1 = cursor.position() as usize;
        avps.avps.push(Avp {
            code,
            vendor,
            raw_data: &data[p1..p1 + datalen],
        });
        cursor.advance(datalen);
        match datalen % 4 {
            0 => { }
            1 => { cursor.seek(SeekFrom::Current(3))?; }
            2 => { cursor.seek(SeekFrom::Current(2))?; }
            3 => { cursor.seek(SeekFrom::Current(1))?; }
            _ => { }
        }
    }

    Ok(avps)
}

#[cfg(test)]
mod tests {

    use crate::codec::{parse_avp_group, AvpGroupBuilder, AVP_FLAG_VENDOR};

    use super::{AvpInnerGroupBuilder, DiamUtilAvp, Message};

    #[test]
    fn avp_encode_decode() {
        let mut grp1 = AvpGroupBuilder::with_capacity(512);
        grp1.avp_write_u32(1, 0, 0, 123);
        grp1.avp_write_string(2, 0, 0, "test1");

        let decoded = parse_avp_group(grp1.as_slice()).unwrap();
        assert_eq!(decoded.get_u32(1, 0), Some(123));
        assert_eq!(decoded.get_str(2, 0), Some("test1"));
        assert_eq!(decoded.avps[1].as_str().unwrap(), "test1");
        assert_eq!(decoded.avps[0].as_u32().unwrap(), 123);
        for avp in decoded {
            println!("{:?}", avp)
        }
    }

    #[test]
    fn avp_encode_decode_with_vendor() {
        let mut grp1 = AvpGroupBuilder::with_capacity(512);
        grp1.avp_write_u32(1, 10, AVP_FLAG_VENDOR, 123);
        grp1.avp_write_string(2, 10, AVP_FLAG_VENDOR, "test1");
        let decoded = parse_avp_group(grp1.as_slice()).unwrap();
        assert!(decoded.get_raw(2, 10).is_some());
        assert!(decoded.get_raw(2, 0).is_none());
        assert!(decoded.get_raw(1, 10).is_some());
        assert!(decoded.get_raw(1, 0).is_none());
        for avp in decoded.as_slice() {
            println!("{:?}", avp)
        }
    }

    #[test]
    fn avp_encode_decode_group_in_group() {
        let mut inner = AvpGroupBuilder::with_capacity(512);
        inner.avp_write_string(1, 0, 0, "a1");
        inner.avp_write_string(2, 0, 0, "aw");

        let mut grp1 = AvpGroupBuilder::with_capacity(512);
        grp1.avp_write_u32(10, 0, 0, 123);
        grp1.avp_write_string(12, 0, 0, "test1");
        grp1.avp_write_raw(13, 0, 0, inner.as_slice());

        let decoded = parse_avp_group(grp1.as_slice()).unwrap();
        assert_eq!(decoded.get_u32(10, 0), Some(123));
        assert_eq!(decoded.get_str(12, 0), Some("test1"));
        let decoded_inner = decoded.get_grp(13, 0);
        assert!(decoded_inner.is_some());
        let decoded_inner = decoded_inner.unwrap();
        assert_eq!(decoded_inner.get_str(1, 0), Some("a1"));
        assert_eq!(decoded_inner.get_str(2, 0), Some("aw"));
    }

    fn make_reference_message() -> Vec<u8> {
        let mut body = AvpGroupBuilder::with_capacity(512);
        body.avp_write_string(10, 11, AVP_FLAG_VENDOR, "a1");
        body.avp_write_string(11, 0, 0, "a2");
        Message {
            flags: 0,
            command: 1,
            application: 2,
            h2h: 3,
            e2e: 4,
            body: body.into(),
        }.encode()
    }

    #[test]
    fn encode_into_one_buffer() {
        let mut buffer = Message {
            flags: 0,
            command: 1,
            application: 2,
            h2h: 3,
            e2e: 4,
            body: Vec::with_capacity(0),
        }.encode();
        buffer.avp_write_string(10, 11, AVP_FLAG_VENDOR, "a1");
        buffer.avp_write_string(11, 0, 0, "a2");
        Message::fix_size(buffer.as_mut_slice());
        assert_eq!(make_reference_message(), buffer)
    }

    #[test]
    fn inner_group() {
        let buf = {
            let mut buf = Vec::with_capacity(1024);
            {
                let mut inner1 = AvpInnerGroupBuilder::new(&mut buf);
                inner1.avp_write_string(1, 0, 0, "123");
                inner1.avp_start_group(2, 0, 0);
                inner1.avp_write_string(3, 0, 0, "123");
                inner1.avp_start_group(4, 0, 0);
                inner1.avp_write_string(5, 0, 0, "999");
                inner1.avp_end_group();
                inner1.avp_end_group();
            }
            buf
        };
        println!("{:x?}", buf.as_slice());
        let decoded = parse_avp_group(&buf).unwrap();
        let grp = decoded.get_grp(2, 0).unwrap();
        let s = grp.get_str(3, 0);
        println!("{:?}", s);
        let grp2 = grp.get_grp(4, 0).unwrap();
        let s2 = grp2.get_str(5, 0).unwrap();
        println!("{:?}", s2);
        
    }

}
