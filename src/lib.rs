#![forbid(unsafe_code)]

//! Streams that arrive framed by MLLP over a TCP connection. One framed
//! message is one Stream.
//!
//! The Minimal Lower Layer Protocol is how HL7 v2 travels in a hospital:
//! `<VT>` (0x0B) opens a message, `<FS><CR>` (0x1C 0x0D) closes it, and the
//! receiver answers on the same connection with a message framed the same way
//! — the HL7 acknowledgement. The frame is this transport's; the
//! acknowledgement's content is HL7's and the `hl7-v2` contract composes it.
//! What this transport does is deliver the message and, when told, write the
//! answer back before the connection closes.
//!
//! The pushed case with a reply channel, like HTTP: the caller holds the
//! connection open waiting for the acknowledgement, so a Contract failure can
//! be answered rather than only audited.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use transport::error::{Result, classify, protocol_error};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Directions, Transport};

/// Start of block.
pub const VT: u8 = 0x0B;
/// End of block.
pub const FS: u8 = 0x1C;
/// Carriage return, closing the end of block.
pub const CR: u8 = 0x0D;

/// The most an MLLP message may be: HL7 messages are kilobytes, and a frame
/// that never closes should not read the peer forever.
pub const MAX_MESSAGE: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct MllpTransport {
    bind: String,
    read_timeout: Option<Duration>,
}

impl MllpTransport {
    #[must_use]
    pub fn new(bind: impl Into<String>) -> Self {
        Self {
            bind: bind.into(),
            read_timeout: None,
        }
    }

    /// Give up on a peer that stops sending mid-frame.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.read_timeout = Some(timeout);
        self
    }

    /// Bind and report the address actually assigned.
    ///
    /// # Errors
    /// Where the address is taken, malformed, or not permitted.
    pub fn bind(&self) -> Result<(TcpListener, String)> {
        socket::bind_tcp(&self.bind)
    }

    /// Take one framed message from an already-bound listener, and hand back
    /// the connection so an acknowledgement can be written with [`acknowledge`].
    ///
    /// # Errors
    /// Where the connection could not be accepted, the frame is malformed, or
    /// the peer stopped mid-message.
    pub fn accept_one(&self, listener: &TcpListener) -> Result<(Arrived, TcpStream)> {
        let (stream, peer) = listener
            .accept()
            .map_err(|e| classify("accepting a connection", &e))?;
        if let Some(timeout) = self.read_timeout {
            stream
                .set_read_timeout(Some(timeout))
                .map_err(|e| classify("setting the read timeout", &e))?;
        }
        let reader = stream
            .try_clone()
            .map_err(|e| classify("cloning the connection", &e))?;
        let message = read_frame(&mut BufReader::new(reader))?;
        Ok((Arrived::new(format!("mllp://{peer}"), message), stream))
    }
}

/// Read one MLLP frame: everything between `<VT>` and the `<FS><CR>` pair.
/// The end of block is the pair: an `<FS>` followed by anything else is
/// inside the message, as is a `<CR>` on its own — HL7 ends every segment
/// with one. Until 2026-09-09 the first `<FS>` ended the block, and a
/// message carrying one anywhere closed the connection.
///
/// # Errors
/// No start byte, a frame that ends without its end bytes, or one over
/// [`MAX_MESSAGE`].
pub fn read_frame(reader: &mut impl BufRead) -> Result<Vec<u8>> {
    let mut first = [0u8; 1];
    reader
        .read_exact(&mut first)
        .map_err(|e| classify("reading the start of block", &e))?;
    if first[0] != VT {
        return Err(protocol_error("a message that does not start with <VT>"));
    }
    let mut message = Vec::new();
    loop {
        let read = reader
            .read_until(CR, &mut message)
            .map_err(|e| classify("reading the message", &e))?;
        if read == 0 || message.last() != Some(&CR) {
            return Err(protocol_error("a connection that closed inside a message"));
        }
        if message.len() > MAX_MESSAGE + 2 {
            return Err(protocol_error("a message over the size Xmip will read"));
        }
        if message.ends_with(&[FS, CR]) {
            message.truncate(message.len() - 2);
            return Ok(message);
        }
    }
}

/// `message`, framed.
#[must_use]
pub fn frame(message: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(message.len() + 3);
    out.push(VT);
    out.extend_from_slice(message);
    out.push(FS);
    out.push(CR);
    out
}

/// Write `acknowledgement` back on the connection a message arrived on.
///
/// # Errors
/// Where the peer went away before the answer.
pub fn acknowledge(connection: &mut TcpStream, acknowledgement: &[u8]) -> Result<()> {
    connection
        .write_all(&frame(acknowledgement))
        .map_err(|e| classify("writing the acknowledgement", &e))?;
    connection
        .flush()
        .map_err(|e| classify("flushing the acknowledgement", &e))
}

impl Transport for MllpTransport {
    fn name(&self) -> &'static str {
        "mllp"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    fn receive(&self) -> Result<Vec<Arrived>> {
        let (listener, _) = self.bind()?;
        let (arrived, _connection) = self.accept_one(&listener)?;
        Ok(vec![arrived])
    }

    /// Send one framed message and read the framed acknowledgement, which is
    /// discarded here: the transport proves delivery, the `hl7-v2` contract
    /// reads what the acknowledgement said. [`send_and_receive`] keeps it.
    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        send_and_receive(target, bytes, self.read_timeout).map(|_| ())
    }
}

/// Send one framed message to `target` and return the framed acknowledgement.
///
/// # Errors
/// Where the peer refused, could not be reached, or answered without a frame.
pub fn send_and_receive(target: &str, bytes: &[u8], timeout: Option<Duration>) -> Result<Vec<u8>> {
    let mut stream =
        TcpStream::connect(target).map_err(|e| classify("connecting to the peer", &e))?;
    if let Some(timeout) = timeout {
        stream
            .set_read_timeout(Some(timeout))
            .map_err(|e| classify("setting the read timeout", &e))?;
    }
    stream
        .write_all(&frame(bytes))
        .map_err(|e| classify("writing to the peer", &e))?;
    stream
        .flush()
        .map_err(|e| classify("flushing to the peer", &e))?;
    let reader = stream
        .try_clone()
        .map_err(|e| classify("cloning the connection", &e))?;
    let mut reader = BufReader::new(reader.take(MAX_MESSAGE as u64 + 3));
    read_frame(&mut reader)
}

impl MllpTransport {
    /// Both ends on this machine: an ephemeral local port, the loopback
    /// timeout on both the accept and the wait for the acknowledgement.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("127.0.0.1:0").timing_out_after(LOOPBACK_TIMEOUT)
    }
}

/// A bound listener waiting for its one framed message, which it answers
/// with the bytes it got: the acknowledgement is HL7's to compose, and the
/// echo proves the reply channel and nothing more.
struct Listening {
    transport: MllpTransport,
    listener: TcpListener,
    address: String,
}

impl FarEnd for Listening {
    fn address(&self) -> &str {
        &self.address
    }

    fn take_one(self: Box<Self>) -> Result<Arrived> {
        let (arrived, mut connection) = self.transport.accept_one(&self.listener)?;
        acknowledge(&mut connection, &arrived.bytes)?;
        Ok(arrived)
    }
}

impl Loopback for MllpTransport {
    /// A block cannot hold its own end: `<FS><CR>` inside the message ends
    /// it there, and what follows is read as the next one.
    fn refuses(&self, payload: &[u8]) -> Option<String> {
        payload
            .windows(2)
            .any(|pair| pair == [FS, CR])
            .then(|| "an MLLP block cannot hold its own end of block, <FS><CR>".to_string())
    }

    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let (listener, address) = self.bind()?;
        Ok(Box::new(Listening {
            transport: self.clone(),
            listener,
            address,
        }))
    }

    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        send_and_receive(address, payload, self.read_timeout).map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shapes a transport is most likely to change: nothing, one byte,
    /// every byte value, a run of NULs, high bytes, and line endings alone.
    fn edge_payloads() -> Vec<(&'static str, Vec<u8>)> {
        vec![
            ("empty", Vec::new()),
            ("one byte", vec![0x2a]),
            ("every byte", (0..=255).collect()),
            ("nul run", vec![0; 512]),
            ("high bytes", vec![0xff; 512]),
            ("crlf storm", b"\r\n".repeat(400)),
        ]
    }

    #[test]
    fn the_loopback_round_trips_a_message_and_acknowledges_it() {
        let arrived = MllpTransport::loopback()
            .round(b"MSH|^~\\&|LAB|HOSP\rPID|1\r")
            .expect("round");
        assert_eq!(arrived.bytes, b"MSH|^~\\&|LAB|HOSP\rPID|1\r");
        assert!(arrived.origin_uri.starts_with("mllp://127.0.0.1:"));
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole_and_refuses_its_own_end() {
        let mllp = MllpTransport::loopback();
        assert!(mllp.ceiling().is_none());
        for (name, bytes) in edge_payloads() {
            assert!(mllp.refuses(&bytes).is_none(), "{name}");
            assert_eq!(mllp.round(&bytes).expect(name).bytes, bytes, "{name}");
        }
        // A declared refusal is true: the bytes really do not come back whole.
        let holds_its_end = [b'a', FS, CR, b'b'];
        assert!(mllp.refuses(&holds_its_end).is_some());
        assert_ne!(
            mllp.round(&holds_its_end).expect("cut").bytes,
            holds_its_end
        );
    }

    #[test]
    fn a_frame_round_trips_and_a_bad_one_is_refused() {
        let framed = frame(b"MSH|^~\\&|A|B");
        assert_eq!(framed[0], VT);
        assert_eq!(&framed[framed.len() - 2..], &[FS, CR]);
        let mut reader = BufReader::new(framed.as_slice());
        assert_eq!(read_frame(&mut reader).expect("frame"), b"MSH|^~\\&|A|B");
        assert!(read_frame(&mut BufReader::new(&b"MSH|no start"[..])).is_err());
        assert!(read_frame(&mut BufReader::new(&[VT, b'M', b'S', b'H'][..])).is_err());
        assert!(read_frame(&mut BufReader::new(&[VT, b'M', FS, b'x'][..])).is_err());
    }

    #[test]
    fn a_message_carrying_the_block_bytes_apart_reads_whole() {
        // Every byte value in order: an <FS> followed by 0x1d, a <CR> on its
        // own, a NUL and a 0xff. Only the pair ends the block.
        let every: Vec<u8> = (0..=255).collect();
        let framed = frame(&every);
        let mut reader = BufReader::new(framed.as_slice());
        assert_eq!(read_frame(&mut reader).expect("frame"), every);
        let segments = b"MSH|^~\\&|A|B\rPID|1\x1cx\r";
        let framed = frame(segments);
        let mut reader = BufReader::new(framed.as_slice());
        assert_eq!(read_frame(&mut reader).expect("frame"), segments);
        let framed = frame(b"");
        let mut reader = BufReader::new(framed.as_slice());
        assert!(read_frame(&mut reader).expect("empty").is_empty());
    }

    #[test]
    fn a_message_arrives_and_the_acknowledgement_comes_back_on_the_connection() {
        let receiver = MllpTransport::new("127.0.0.1:0").timing_out_after(Duration::from_secs(2));
        let (listener, address) = receiver.bind().expect("binding");
        let sender = std::thread::spawn(move || {
            send_and_receive(
                &address,
                b"MSH|^~\\&|LAB|HOSP",
                Some(Duration::from_secs(2)),
            )
        });
        let (arrived, mut connection) = receiver.accept_one(&listener).expect("accepting");
        assert_eq!(arrived.bytes, b"MSH|^~\\&|LAB|HOSP");
        assert!(arrived.origin_uri.starts_with("mllp://127.0.0.1:"));
        acknowledge(&mut connection, b"MSH|^~\\&|HOSP|LAB\rMSA|AA|1").expect("acknowledging");
        let answer = sender
            .join()
            .expect("sender thread")
            .expect("acknowledgement");
        assert_eq!(answer, b"MSH|^~\\&|HOSP|LAB\rMSA|AA|1");
    }

    #[test]
    fn a_listening_socket_has_no_artefact_to_claim() {
        assert!(MllpTransport::new("127.0.0.1:0").claims().is_none());
        assert_eq!(MllpTransport::new("127.0.0.1:0").name(), "mllp");
    }
}
