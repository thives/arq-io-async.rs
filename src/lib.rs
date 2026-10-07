//! ARQ (Automatic Repeat reQuest) layer for unreliable point-to-point links.
//!
//! The layer wraps a framed lower transport and is itself a [`Transport`]
//! that presents a reliable, in-order, bidirectional byte stream. Frames are
//! retransmitted until they are acknowledged and out-of-order frames are
//! buffered until their predecessors arrive, so the reader sees exactly the
//! bytes the peer wrote, in order.
//!
//! The crate is `no_std`, has no internal tasks, and does not depend on any
//! runtime. [`Arq`] is a polled state machine that makes progress whenever
//! one of its [`Transport`] methods is polled. Adaptors for a specific
//! runtime, such as `tokio::io::AsyncRead` or `embedded_io_async::Read`, are
//! left to the user.
//!
//! One instance is one link: the peer must run its own instance on the other
//! end of the channel.
//!
//! ## The lower transport
//!
//! The lower transport must be framed: one `poll_read` returns exactly one
//! frame, and one `poll_write` accepts exactly one frame. The layer does not
//! split or reassemble frames.
//!
//! - `poll_read` is always given a buffer of [`MAX_FRAME`] bytes. It returns
//!   the length of the frame it delivered; `0` means the read side has ended.
//! - `poll_write` is offered one complete frame and must accept all of it. Any
//!   other length fails the link with [`ArqError::WriteLength`], and `0` fails
//!   it with [`ArqError::Closed`].
//! - `poll_flush` is called after every accepted frame, and the frame counts
//!   as sent only once the flush completes. A `Pending` operation is resumed
//!   by polling again with the same frame; it is never replaced.
//!
//! ## Building an instance
//!
//! Instances are built through [`ArqLayer`], which fixes the retransmission
//! window `N` and carries the CRC algorithm and ACK codec. The result
//! implements [`Transport`] for the upper layer:
//!
//! ```
//! use arq_io_async::{Arq, ArqLayer, BchAckCodec, Timer, Transport};
//!
//! fn build<L: Transport, T: Timer>(
//!     lower: L,
//!     timer: T,
//! ) -> Arq<8, 16, L, crc::Crc<u16>, BchAckCodec, T> {
//!     ArqLayer::<8>::new().build(lower, timer)
//! }
//! ```
//!
//! `16` is the codeword length of the default ACK codec. Use
//! [`ArqLayer::build_with_codec`] with a custom [`AckCodec`].
//!
//! The upper layer reads and writes through [`Transport`]; `poll_close`
//! shuts the link down, see "Closing the link".
//!
//! ## Retransmission timer
//!
//! Unacknowledged frames are retransmitted when a retransmission timeout
//! expires, never merely because the layer is polled. The timeout starts at
//! 250 ms, doubles on every expiry without progress up to 4 s, and returns to
//! its initial value whenever an ACK acknowledges new data. Change the bounds
//! with [`ArqLayer::with_retransmit_timeout`]. By default, 16 retransmission
//! rounds without ACK progress are allowed; expiry after the last round fails
//! the link with [`ArqError::Timeout`]. Set this limit with
//! [`ArqLayer::with_retry_limit`].
//!
//! The time source is supplied through the [`Timer`] trait. Implement it for
//! your platform and pass an instance to [`ArqLayer::build`]; each instance
//! needs its own timer.
//!
//! ## Flushing
//!
//! `poll_flush` completes once every written byte has been acknowledged by the peer
//! and every ACK the layer owes the peer has been written and flushed on the
//! lower channel.
//! The last data frame of the flushed burst asks the peer to acknowledge it
//! immediately, so a short write followed by `poll_flush` does not wait for more
//! traffic. Ordinary writes are still acknowledged in batches. `poll_flush` does not
//! close the link and can be called any number of times.
//!
//! ## Corrupt and lost frames
//!
//! Because the lower transport is framed, frame boundaries are
//! authoritative. A frame that fails validation (unknown type, impossible
//! length, bad CRC, or an ACK the codec cannot decode) is discarded whole,
//! without retaining bytes or consuming any part of the next frame, and it is
//! never terminal. It is recovered by retransmission.
//!
//! Lost or corrupt ACKs are recovered the same way: the retransmitted frame is
//! a duplicate, and the peer answers duplicates with a fresh ACK. This includes
//! a duplicate `FIN` received after the stream has completed; see "Closing the
//! link".
//!
//! ## Closing the link
//!
//! [`Arq::poll_close`] flushes any pending data in a final `FIN` frame and
//! signals end-of-stream to the peer. Afterwards `poll_write` fails with
//! [`ArqError::Closed`], and the peer's `poll_read` returns `0` bytes once its
//! stream is drained.
//!
//! The reader sees end-of-stream only after the layer has sent and flushed the
//! ACK for the peer's `FIN`. `poll_close` likewise completes only after the ACK
//! for a received `FIN` has been delivered.
//!
//! Completion means that application data transfer is finished, not that the
//! layer can no longer receive. If the ACK for a `FIN` is lost, the peer
//! retransmits the `FIN`, and the completed layer acknowledges the duplicate
//! whenever it is polled. It never sends new data or reopens writing, and no
//! fixed linger time is used, because no finite delay can prove that the peer
//! received the last ACK.
//!
//! Recovery therefore works only while the instance keeps being polled. Once
//! polling stops or the instance is dropped, a lost final ACK cannot be
//! recovered.
//!
//! ## Lower end-of-stream
//!
//! If the lower channel's read side ends, frames already received are still
//! delivered. `read` returns the accepted data first, then `0` bytes if a
//! valid `FIN` was received, otherwise [`ArqError::Closed`]. Writing is not
//! disabled, but `poll_flush` and `poll_close` fail with [`ArqError::Closed`] once
//! they would have to wait for an ACK that can no longer arrive.
//!
//! ## Terminal errors
//!
//! Lower I/O, write-length, ACK-encoding, and retry-exhaustion errors stop the engine.
//! Reads still deliver buffered in-order data before reporting the error. A
//! write, flush, or close may report that error first without discarding
//! buffered reads. The original error is reported once; later operations fail
//! with [`ArqError::Closed`] after buffered reads are drained.
//!
//! ## Work per poll
//!
//! Each poll processes a bounded amount of received input, including corrupt
//! frames, and then services transmission. When input remains, the polled task
//! is woken so processing continues in a later poll. This also holds after the
//! stream has completed, where received frames are still processed to
//! acknowledge duplicates.

#![no_std]

#[cfg(test)]
extern crate std;

mod ack_codec;
mod arq;
mod bch;
mod crc;
mod error;
mod frame;
mod timer;
mod transport;

pub use crate::ack_codec::{AckCodec, BchAckCodec};
pub use crate::arq::{Arq, ArqLayer, MAX_FRAME};
pub use crate::crc::Crc16;
pub use crate::error::{AckError, ArqError, FrameError};
pub use crate::frame::{AckFrame, MAX_SEQ};
pub use crate::timer::Timer;
pub use crate::transport::Transport;
