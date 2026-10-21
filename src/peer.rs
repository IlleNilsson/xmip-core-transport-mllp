//! A sender's connection, kept open at a Receive Location between its
//! frames.
//!
//! Every frame is one turn (`transport::serving::Turn`): read whole, handed
//! up as a Stream, and answered with its HL7 acknowledgement once the
//! receive cycle has ended. Until then the connection is busy
//! (`transport::serving::Open::busy`): MLLP answers one frame at a time, so
//! the sender's next frame waits in the socket until the acknowledgement of
//! the one before is written. Until 2026-10-21 a receive accepted a
//! connection for every frame and the sender opened one for each.

use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpStream};

use hl7_v2::Acknowledgement as Hl7Answer;
use transport::answer::{Answer, Busy};
use transport::error::{Result, classify};
use transport::serving::{Open, Turn};
use transport::{Acknowledgement, Arrived, Verdict};

use crate::{frame, read_frame};

/// One sender's connection, kept between its frames.
pub struct Peer {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    from: SocketAddr,
    /// A frame taken and its acknowledgement not yet written.
    busy: Busy,
}

impl Peer {
    /// A sender's new connection, from `peer`.
    ///
    /// # Errors
    /// Where the connection could not be cloned for its reads.
    pub fn new(stream: TcpStream, peer: SocketAddr) -> Result<Self> {
        let (reader, writer) = transport::socket::split(stream)?;
        Ok(Self {
            reader,
            writer,
            from: peer,
            busy: Busy::new(),
        })
    }

    /// The next frame, whole, as a Stream whose verdict writes its HL7
    /// acknowledgement on this connection: `AA` on accepted, `AE` on
    /// refused, `AR` on failed. A sender that closed between frames has
    /// hung up, not failed.
    ///
    /// # Errors
    /// A malformed frame, or a sender that stopped mid-frame.
    pub fn turn(&mut self) -> Result<Turn<Arrived>> {
        let ended = self
            .reader
            .fill_buf()
            .map_err(|e| classify("reading the start of block", &e))?
            .is_empty();
        if ended {
            return Ok(Turn::Closed);
        }
        let message = read_frame(&mut self.reader)?;
        let original = String::from_utf8_lossy(&message).into_owned();
        // Let go without a verdict, the connection is shut: the sender
        // sends the message again.
        let answer = Answer::held(&self.writer)?.busy(&self.busy);
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
        let arrived = Arrived::whole(format!("mllp://{}", self.from), message, acknowledgement)
            .from_peer(self.from);
        Ok(Turn::Taken(arrived))
    }
}

impl Open for Peer {
    fn socket(&self) -> &TcpStream {
        &self.writer
    }

    /// A frame the last read took beside its own waits in the reader.
    fn waiting(&self) -> bool {
        !self.reader.buffer().is_empty()
    }

    fn busy(&self) -> Option<&Busy> {
        Some(&self.busy)
    }
}
