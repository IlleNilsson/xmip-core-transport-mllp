#![forbid(unsafe_code)]

//! Streams that arrive framed by MLLP over a TCP connection. One framed
//! message is one Stream.
//!
//! The Minimal Lower Layer Protocol is how HL7 v2 travels in a hospital:
//! `<VT>` (0x0B) opens a message, `<FS><CR>` (0x1C 0x0D) closes it, and the
//! receiver answers on the same connection with a message framed the same way
//! — the HL7 acknowledgement. The frame is this transport's; the
//! acknowledgement's content is HL7's and the `hl7-v2` contract composes it.
//! What this transport does is deliver the message and write the answer back
//! before the connection closes.
//!
//! The pushed case with a reply channel, like HTTP: the caller holds the
//! connection open waiting for the acknowledgement, so it is told after the
//! whole receive cycle. The message arrives whole, as its frame carried it,
//! and its acknowledgement is deferred, its code from HL7 v2 Table 0008
//! (MSA-1) under the original acknowledgment rules of chapter 2, section
//! 2.9.2: an `AA` (application accept) on [`Verdict::Accepted`]; an `AE`
//! (application error: the message was understood and refused, so the
//! sender does not send it again) on [`Verdict::Refused`]; an `AR`
//! (application reject: not processed for a reason other than its content,
//! so the sender sends it again) on [`Verdict::Failed`]. A connection
//! dropped without a verdict is closed unanswered, and the sender sends
//! again.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::time::Duration;

use hl7_v2::Acknowledgement as Hl7Answer;
use transport::ArrivalIdentity;
use transport::Configured;
use transport::answer::Answer;
use transport::error::TransportError;
use transport::error::{Result, classify, protocol_error};
use transport::kept::Kept;
use transport::listening::{Accepting, Listening};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Acknowledgement, Taken, Verdict};
use transport::{Arrived, Directions, Transport};
use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

/// Start of block.
const VT: u8 = 0x0B;
/// End of block.
const FS: u8 = 0x1C;
/// Carriage return, closing the end of block.
pub const CR: u8 = 0x0D;

#[derive(Clone)]
pub struct MllpTransport {
    bind: String,
    read_timeout: Option<Duration>,
    /// The listener the first receive binds, and every receive takes from.
    receiving: Kept<TcpListener>,
}

impl MllpTransport {
    #[must_use]
    pub fn new(bind: impl Into<String>) -> Self {
        Self {
            bind: bind.into(),
            read_timeout: None,
            receiving: Kept::new(),
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

    /// Take one framed message from an already-bound listener, whole. The
    /// caller waits on the connection for the HL7 acknowledgement, which the
    /// arrival's verdict writes: `AA` on accepted, `AE` on refused, `AR` on
    /// failed.
    ///
    /// # Errors
    /// Where the connection could not be accepted, the frame is malformed, or
    /// the peer stopped mid-message.
    pub fn accept_one(&self, listener: &TcpListener) -> Result<Arrived> {
        // The wait for the connection is bounded as well as the reads. It was
        // bare until 2026-09-21, and a far end nobody reached waited for good.
        let (stream, peer) = socket::accept_tcp(listener, self.read_timeout)?;
        let answer = Answer::held(&stream)?;
        let message = read_frame(&mut BufReader::new(stream))?;
        let original = String::from_utf8_lossy(&message).into_owned();
        // Let go without a verdict, the connection is shut: the sender
        // sends the message again.
        let acknowledgement = Acknowledgement::deferred(move |verdict| {
            let (code, text) = match verdict {
                Verdict::Accepted => (Hl7Answer::Accept, ""),
                Verdict::Refused(_) => (Hl7Answer::Error, "refused; do not send it again"),
                Verdict::Failed => (Hl7Answer::Reject, "not processed; send it again"),
            };
            answer.write(&frame(
                hl7_v2::acknowledge(&original, code, text).as_bytes(),
            ))
        });
        Ok(Arrived::whole(format!("mllp://{peer}"), message, acknowledgement).from_peer(peer))
    }
}

impl Configured for MllpTransport {
    /// The address is where a Receive Location listens and where a Send
    /// Location connects; the one setting bounds the wait for either.
    const SETTINGS: &'static Settings = &Settings {
        technology: env!("CARGO_PKG_NAME"),
        settings: &[Setting {
            name: "timeout",
            kind: Kind::Duration,
            presence: Presence::Optional,
            meaning: "How long a connection, a frame and its acknowledgement are waited on.",
            applies: Applies::Both,
        }],
    };

    fn configured(address: &str, settings: &xcore::settings::Read) -> Result<Self> {
        let transport = Self::new(address);
        Ok(match settings.optional_duration("timeout") {
            Some(timeout) => transport.timing_out_after(timeout),
            None => transport,
        })
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
/// `net::MAX_BODY`: a frame that never closes does not read the peer forever.
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
        let read = net::read::until(reader, CR, net::MAX_BODY + 2, &mut message)?;
        if read == 0 || message.last() != Some(&CR) {
            return Err(protocol_error("a connection that closed inside a message"));
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

impl Transport for MllpTransport {
    fn name(&self) -> &'static str {
        "mllp"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    fn arrivals(&self) -> transport::Arrivals {
        transport::Arrivals::Unordered(
            "each caller's connection is its own, and a caller waits for its own answer",
        )
    }

    /// One framed message, from the listener the first receive bound and
    /// kept. Its caller waits for the acknowledgement until the receive
    /// cycle has ended: `AA` on accepted, `AR` on refused.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let listener = self.receiving.bound(|| self.bind())?;
        Ok(vec![self.accept_one(listener)?])
    }

    /// Send one framed message and read the framed acknowledgement. An `AR`
    /// or `CR` (rejected, send again) fails the send as retryable; an `AE`
    /// or `CE` (the content was refused) as permanent. An answer that is not
    /// HL7 is delivery, as it always was.
    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let answer = send_and_receive(target, bytes, self.read_timeout)?;
        refusal(&answer).map_or(Ok(()), Err)
    }
}

/// The failure an acknowledgement says, where it says one: MSA-1 `AR` or
/// `CR` retryable, `AE` or `CE` permanent.
fn refusal(answer: &[u8]) -> Option<TransportError> {
    let text = String::from_utf8_lossy(answer);
    let message = hl7_v2::Message::parse(&text).ok()?;
    let said = format!(
        "the receiver answered {}: {}",
        message.field("MSA", 1),
        message.field("MSA", 3)
    );
    match message.field("MSA", 1) {
        "AR" | "CR" => Some(TransportError::retryable(said)),
        "AE" | "CE" => Some(TransportError::permanent(said)),
        _ => None,
    }
}

/// Send one framed message to `target` and return the framed acknowledgement.
///
/// # Errors
/// Where the peer refused, could not be reached, or answered without a frame.
fn send_and_receive(target: &str, bytes: &[u8], timeout: Option<Duration>) -> Result<Vec<u8>> {
    // The connect is bounded as well as the reads. It was bare until
    // 2026-09-21, and a machine out of ephemeral ports waited without end.
    let mut stream = socket::connect_tcp(target, timeout)?;
    stream
        .write_all(&frame(bytes))
        .map_err(|e| classify("writing to the peer", &e))?;
    stream
        .flush()
        .map_err(|e| classify("flushing to the peer", &e))?;
    let reader = stream
        .try_clone()
        .map_err(|e| classify("cloning the connection", &e))?;
    let mut reader = BufReader::new(reader);
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

impl Accepting for MllpTransport {
    fn take_one(self, listener: &TcpListener) -> Result<Taken> {
        self.accept_one(listener)?.taken()
    }
}

impl Loopback for MllpTransport {
    fn arrival_identity(&self) -> ArrivalIdentity {
        ArrivalIdentity::PEER
    }

    /// A block cannot hold its own end: `<FS><CR>` inside the message ends
    /// it there, and what follows is read as the next one.
    fn refuses(&self, payload: &[u8]) -> Option<String> {
        payload
            .windows(2)
            .any(|pair| pair == [FS, CR])
            .then(|| "an MLLP block cannot hold its own end of block, <FS><CR>".to_string())
    }

    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        Ok(Box::new(Listening::new(self.clone(), self.bind()?)))
    }

    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        self.send(address, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use transport::Refusal;
    use transport::payload::edge_payloads;

    #[test]
    fn every_receive_takes_from_the_listener_the_first_bound() {
        // Every frame lands before any receive: queued on the kept
        // listener, not refused, and taken in order by receives that bind
        // nothing.
        let receiver = MllpTransport::loopback();
        receiver.receiving.bound(|| receiver.bind()).expect("bound");
        let address = receiver.receiving.address().expect("address");
        // Each peer stays connected, waiting for its acknowledgement.
        let mut peers = Vec::new();
        for round in 0..5 {
            let mut peer = socket::connect_tcp(address, Some(LOOPBACK_TIMEOUT)).expect("peer");
            let message = format!("MSH|round {round}\r");
            peer.write_all(&frame(message.as_bytes())).expect("framed");
            peers.push(peer);
        }
        for round in 0..5 {
            let mut arrived = receiver.receive().expect("received");
            let taken = arrived.remove(0).taken().expect("taken");
            assert_eq!(taken.bytes, format!("MSH|round {round}\r").as_bytes());
        }
        for peer in peers {
            let answer = read_frame(&mut BufReader::new(peer)).expect("acknowledged");
            assert!(answer.windows(7).any(|w| w == b"\rMSA|AA"), "{answer:?}");
        }
    }

    #[test]
    fn mllp_declares_its_settings_and_reads_through_them() {
        use xcore::settings::Given;
        assert!(MllpTransport::SETTINGS.problems().is_empty());
        let given = [("timeout".to_string(), Given::Text("30s".to_string()))];
        let built = MllpTransport::open("0.0.0.0:2575", Applies::Receive, &given).expect("built");
        assert_eq!(built.bind, "0.0.0.0:2575");
        assert_eq!(built.read_timeout, Some(Duration::from_secs(30)));
        let unknown = [("ack".to_string(), Given::Boolean(true))];
        let Err(refused) = MllpTransport::open("0.0.0.0:2575", Applies::Send, &unknown) else {
            panic!("mllp declares no ack");
        };
        assert!(refused.message.contains("\"ack\""), "{refused}");
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

    const ADT: &[u8] = b"MSH|^~\\&|LAB|HOSP|XMIP|HUB|20261002||ADT^A01|C1|P|2.5\rPID|1\r";

    /// One message sent on a thread of its own, its answer handed back, and
    /// the arrival it made at a fresh receiver, its verdict not yet given.
    fn one_sent() -> (std::thread::JoinHandle<Result<Vec<u8>>>, Arrived) {
        let receiver = MllpTransport::new("127.0.0.1:0").timing_out_after(Duration::from_secs(2));
        let (listener, address) = receiver.bind().expect("binding");
        let sender = std::thread::spawn(move || {
            send_and_receive(&address, ADT, Some(Duration::from_secs(2)))
        });
        let arrived = receiver.accept_one(&listener).expect("accepting");
        (sender, arrived)
    }

    #[test]
    fn an_accepted_message_is_answered_aa_after_the_cycle() {
        let (sender, arrived) = one_sent();
        assert!(arrived.defers(), "the caller waits for the verdict");
        assert!(arrived.origin_uri.starts_with("mllp://127.0.0.1:"));
        assert_eq!(arrived.taken().expect("accepted").bytes, ADT);
        let answer = sender.join().expect("sender thread").expect("answer");
        let text = String::from_utf8(answer).expect("text");
        assert!(text.contains("\rMSA|AA|C1|"), "{text:?}");
        assert!(refusal(text.as_bytes()).is_none());
    }

    #[test]
    fn a_refused_message_is_answered_ae_so_the_sender_does_not_send_it_again() {
        let (sender, arrived) = one_sent();
        arrived.refused(Refusal::Unacceptable).expect("refused");
        let answer = sender.join().expect("sender thread").expect("answer");
        let text = String::from_utf8(answer).expect("text");
        assert!(text.contains("\rMSA|AE|C1|"), "{text:?}");
        let refused = refusal(text.as_bytes()).expect("a refusal");
        assert!(!refused.retryable, "{refused}");
    }

    #[test]
    fn a_failed_message_is_answered_ar_so_the_sender_sends_it_again() {
        let (sender, arrived) = one_sent();
        arrived.failed().expect("failed");
        let answer = sender.join().expect("sender thread").expect("answer");
        let text = String::from_utf8(answer).expect("text");
        assert!(text.contains("\rMSA|AR|C1|"), "{text:?}");
        let failure = refusal(text.as_bytes()).expect("a refusal");
        assert!(failure.retryable, "{failure}");
    }

    #[test]
    fn a_send_fails_on_a_refusing_answer() {
        let receiver = MllpTransport::loopback();
        let (listener, address) = receiver.bind().expect("binding");
        let refusing =
            std::thread::spawn(move || receiver.accept_one(&listener).expect("accepting").failed());
        let error = MllpTransport::loopback()
            .send(&address, ADT)
            .expect_err("refused");
        refusing.join().expect("thread").expect("answered");
        assert!(error.retryable, "{error}");
        assert!(error.message.contains("AR"), "{error}");
    }

    #[test]
    fn a_listening_socket_has_no_artefact_to_claim() {
        assert!(MllpTransport::new("127.0.0.1:0").claims().is_none());
        assert_eq!(MllpTransport::new("127.0.0.1:0").name(), "mllp");
    }
}
