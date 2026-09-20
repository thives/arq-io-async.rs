#![cfg_attr(not(feature = "std"), no_std)]

use core::fmt;
use core::ops::Add;
use core::task::{Context, Poll};

#[cfg(any(feature = "tokio", feature = "embedded-io"))]
use core::pin::Pin;

#[cfg(feature = "tokio")]
use tokio::io::ReadBuf;

// ===================== message format =====================
// One frame == one message:
//   [0]     type     u8
//   [1]     pkt_id   u8
//   [2]     len      u8  (payload length; 0 for control frames)
//   [3..5]  crc16    BE  (X25/SDLC over bytes 0..2 + payload)
//   [5..]   payload   (DAT only)
//
// The wire to the lower layer is a raw byte stream: frames are delimited by
// their length field. Nothing is assumed about how much data a lower
// read/write accepts; only the returned lengths are used.

const MSG_TYPE_ACK: u8 = 1;
const MSG_TYPE_NAK: u8 = 2;
const MSG_TYPE_SYN: u8 = 3;
const MSG_TYPE_RYN: u8 = 4;
const MSG_TYPE_DAT: u8 = 5;
const HDR_LEN: usize = 5;
const MAX_FRAME: usize = 256;
const MAX_PAYLOAD: usize = MAX_FRAME - HDR_LEN;
const PAYLOAD_RING: usize = 4;
const TX_FIFO: usize = 3;
const SYNC_PERIOD: Duration = Duration::from_millis(1000);
const ACK_TIMEOUT: Duration = Duration::from_millis(50);

pub trait Crc: Clone + Copy + fmt::Debug {
    type Output: Copy + Eq + fmt::Debug;
    fn init(&self) -> Self::Output;
    fn crc_update(&self, acc: Self::Output, data: &[u8]) -> Self::Output;
    fn finalize(&self, acc: Self::Output) -> Self::Output;
    fn encode(&self, value: Self::Output) -> [u8; 2];
    fn decode(&self, bytes: [u8; 2]) -> Self::Output;
}

#[derive(Clone, Copy, Debug)]
pub struct Crc16X25;

impl Crc for Crc16X25 {
    type Output = u16;

    fn init(&self) -> u16 {
        0xFFFF
    }

    fn crc_update(&self, mut acc: u16, data: &[u8]) -> u16 {
        for &b in data {
            acc ^= b as u16;
            for _ in 0..8 {
                acc = (acc >> 1) ^ ((acc & 1) * 0x8408);
            }
        }
        acc
    }

    fn finalize(&self, acc: u16) -> u16 {
        !acc
    }

    fn encode(&self, value: u16) -> [u8; 2] {
        value.to_be_bytes()
    }

    fn decode(&self, bytes: [u8; 2]) -> u16 {
        u16::from_be_bytes(bytes)
    }
}

#[derive(Clone, Copy, Debug)]
struct Frame {
    data: [u8; MAX_FRAME],
    len: usize,
}

fn ctl_frame<C: Crc>(crc: &C, ty: u8, pkt_id: u8) -> Frame {
    let bytes = crc.encode(crc.finalize(crc.crc_update(crc.init(), &[ty, pkt_id, 0])));
    let mut data = [0u8; MAX_FRAME];
    data[0] = ty;
    data[1] = pkt_id;
    data[3] = bytes[0];
    data[4] = bytes[1];
    Frame { data, len: HDR_LEN }
}

fn parse_frame<C: Crc>(crc: &C, b: &[u8]) -> Parsed {
    let ty = b[0];
    let id = b[1];
    let plen = b.len() - HDR_LEN;
    let received = crc.decode([b[3], b[4]]);
    let mut payload = [0u8; MAX_PAYLOAD];
    payload[..plen].copy_from_slice(&b[HDR_LEN..]);
    let calc = crc.finalize(crc.crc_update(crc.crc_update(crc.init(), &b[..3]), &b[HDR_LEN..]));
    Parsed {
        ty,
        id,
        payload,
        plen,
        crc_ok: calc == received,
    }
}

fn build_dat_frame<C: Crc>(crc: &C, pkt_id: u8, payload: &[u8]) -> Frame {
    let mut data = [0u8; MAX_FRAME];
    data[0] = MSG_TYPE_DAT;
    data[1] = pkt_id;
    data[2] = payload.len() as u8;
    data[HDR_LEN..HDR_LEN + payload.len()].copy_from_slice(payload);
    let bytes = crc.encode(crc.finalize(crc.crc_update(
        crc.crc_update(crc.init(), &data[..3]),
        &data[HDR_LEN..HDR_LEN + payload.len()],
    )));
    data[3] = bytes[0];
    data[4] = bytes[1];
    Frame {
        data,
        len: HDR_LEN + payload.len(),
    }
}

#[derive(Debug)]
struct Parsed {
    ty: u8,
    id: u8,
    payload: [u8; MAX_PAYLOAD],
    plen: usize,
    crc_ok: bool,
}

// ===================== timer abstraction =====================
// No background tasks: the state machine is polled from outside, so time is
// only read. Deadlines are stored as Instant and compared in the state methods.

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub struct Instant(u64);

impl Instant {
    pub const fn as_millis(&self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Duration(u64);

impl Duration {
    pub const fn from_millis(ms: u64) -> Self {
        Self(ms)
    }
    pub const fn from_secs(secs: u64) -> Self {
        Self(secs * 1000)
    }
    pub const fn as_millis(&self) -> u64 {
        self.0
    }
}

impl Add<Duration> for Instant {
    type Output = Instant;
    fn add(self, d: Duration) -> Instant {
        Instant(self.0.saturating_add(d.0))
    }
}

pub trait Timer {
    fn now(&self) -> Instant;
}

#[cfg(feature = "tokio")]
#[derive(Clone, Copy)]
pub struct TokioTimer {
    start: std::time::Instant,
}

#[cfg(feature = "tokio")]
impl TokioTimer {
    pub fn new() -> Self {
        Self {
            start: std::time::Instant::now(),
        }
    }
}

#[cfg(feature = "tokio")]
impl Default for TokioTimer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "tokio")]
impl Timer for TokioTimer {
    fn now(&self) -> Instant {
        Instant(self.start.elapsed().as_millis() as u64)
    }
}

// ===================== errors =====================

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ArqError<E = ()> {
    Io(E),
    Framing,
    Timeout,
    Closed,
}

impl<E: fmt::Display> fmt::Display for ArqError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArqError::Io(e) => write!(f, "io error: {e}"),
            ArqError::Framing => write!(f, "arq framing error"),
            ArqError::Timeout => write!(f, "arq ack timeout"),
            ArqError::Closed => write!(f, "arq link closed"),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> core::error::Error for ArqError<E> {}

#[cfg(feature = "std")]
impl From<ArqError<std::io::Error>> for std::io::Error {
    fn from(e: ArqError<std::io::Error>) -> Self {
        match e {
            ArqError::Io(e) => e,
            ArqError::Framing => {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "arq framing error")
            }
            ArqError::Timeout => {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "arq ack timeout")
            }
            ArqError::Closed => {
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "arq link closed")
            }
        }
    }
}

#[cfg(feature = "embedded-io")]
impl<E: embedded_io_async::Error> embedded_io_async::Error for ArqError<E> {
    fn kind(&self) -> embedded_io_async::ErrorKind {
        match self {
            ArqError::Io(e) => e.kind(),
            ArqError::Framing => embedded_io_async::ErrorKind::InvalidData,
            ArqError::Timeout => embedded_io_async::ErrorKind::TimedOut,
            ArqError::Closed => embedded_io_async::ErrorKind::BrokenPipe,
        }
    }
}

// ===================== metrics (poller-owned, snapshot for readers) =====================

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct SendMetrics {
    pub dat: u32,
    pub retrans: u32,
    pub syn: u32,
    pub ack: u32,
    pub nak: u32,
    pub ryn: u32,
    pub lost: u32,
}

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct RecvMetrics {
    pub dat: u32,
    pub framing: u32,
    pub crc: u32,
    pub dup: u32,
    pub backpressure: u32,
}

// ===================== lower layer driver =====================
// The machine never assumes how much the lower layer accepts per call; it
// only uses the returned lengths and polls again until the frame is out.

pub trait Lower {
    type Error: fmt::Debug + 'static;

    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>>;
    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Self::Error>>;
    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;
}

#[cfg(feature = "tokio")]
pub struct TokioLower<S>(pub S);

#[cfg(feature = "tokio")]
impl<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> Lower for TokioLower<S> {
    type Error = std::io::Error;

    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        let mut rb = ReadBuf::new(buf);
        match tokio::io::AsyncRead::poll_read(Pin::new(&mut self.0), cx, &mut rb) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(rb.filled().len())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Self::Error>> {
        tokio::io::AsyncWrite::poll_write(Pin::new(&mut self.0), cx, buf)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        tokio::io::AsyncWrite::poll_flush(Pin::new(&mut self.0), cx)
    }
}

#[cfg(feature = "embedded-io")]
pub struct EiaLower<S>(pub S);

// embedded-io-async exposes async fn only, so the poll-based driver creates a
// fresh future on every poll and polls it once. Drivers are expected to be
// side-effect-free on cancel (as the embedded-io-async contract recommends).
#[cfg(feature = "embedded-io")]
impl<S: embedded_io_async::Read + embedded_io_async::Write> Lower for EiaLower<S>
where
    S::Error: 'static,
{
    type Error = S::Error;

    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        let mut fut = self.0.read(buf);
        unsafe { Pin::new_unchecked(&mut fut) }.poll(cx)
    }

    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Self::Error>> {
        let mut fut = self.0.write(buf);
        unsafe { Pin::new_unchecked(&mut fut) }.poll(cx)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut fut = self.0.flush();
        unsafe { Pin::new_unchecked(&mut fut) }.poll(cx)
    }
}

// ===================== internal buffers =====================

#[derive(Clone, Copy, Debug)]
struct Payload {
    data: [u8; MAX_PAYLOAD],
    len: usize,
}

impl Default for Payload {
    fn default() -> Self {
        Self {
            data: [0; MAX_PAYLOAD],
            len: 0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Fifo<T: Copy, const N: usize> {
    slots: [T; N],
    head: usize,
    len: usize,
}

impl<T: Copy, const N: usize> Fifo<T, N> {
    fn new(empty: T) -> Self {
        Self {
            slots: [empty; N],
            head: 0,
            len: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn is_full(&self) -> bool {
        self.len == N
    }

    fn push(&mut self, v: T) -> bool {
        if self.len == N {
            return false;
        }
        let idx = (self.head + self.len) % N;
        self.slots[idx] = v;
        self.len += 1;
        true
    }

    fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        let v = self.slots[self.head];
        self.head = (self.head + 1) % N;
        self.len -= 1;
        Some(v)
    }

    fn front(&self) -> Option<&T> {
        if self.len == 0 {
            None
        } else {
            Some(&self.slots[self.head])
        }
    }
}

struct Delivering {
    payload: Payload,
    off: usize,
}

// ===================== state machine =====================
// One instance per link endpoint, polled from outside. All state is owned by
// the machine: with no second task there is no shared state. The upper layer
// talks to the machine through the two payload rings via Read/Write.

#[derive(Clone, Copy, Debug)]
enum State {
    // Send SYN until RYN arrives.
    Syncing,
    // DAT in flight.
    AwaitingAck {
        pending_id: u8,
        attempts: usize,
        deadline: Instant,
    },
    // Synced, nothing pending.
    Idle,
}

pub struct Arq<L, T, C> {
    lower: L,
    timer: T,
    crc: C,
    send: SendMetrics,
    recv: RecvMetrics,
    next_pkt_id: u8,
    recv_pkt_id: u8,
    pending: Option<Payload>,
    max_resend: usize,
    next_syn_at: Instant,
    payload_tx: Fifo<Payload, PAYLOAD_RING>,
    payload_rx: Fifo<Payload, PAYLOAD_RING>,
    tx_fifo: Fifo<Frame, TX_FIFO>,
    tx_off: usize,
    frame_buf: [u8; MAX_FRAME],
    frame_len: usize,
    staging: Payload,
    delivering: Option<Delivering>,
    delivered: usize,
    state: State,
}

impl<L: Lower, T: Timer, C: Crc> Arq<L, T, C> {
    pub fn new(lower: L, timer: T, crc: C, max_resend: usize) -> Self {
        Self {
            lower,
            timer,
            crc,
            send: SendMetrics::default(),
            recv: RecvMetrics::default(),
            next_pkt_id: 0,
            recv_pkt_id: u8::MAX,
            pending: None,
            max_resend,
            next_syn_at: Instant::default(),
            payload_tx: Fifo::new(Payload::default()),
            payload_rx: Fifo::new(Payload::default()),
            tx_fifo: Fifo::new(Frame {
                data: [0; MAX_FRAME],
                len: 0,
            }),
            tx_off: 0,
            frame_buf: [0; MAX_FRAME],
            frame_len: 0,
            staging: Payload::default(),
            delivering: None,
            delivered: 0,
            state: State::Syncing,
        }
    }

    pub fn send_metrics(&self) -> SendMetrics {
        self.send
    }

    pub fn recv_metrics(&self) -> RecvMetrics {
        self.recv
    }

    // Single entry point for both reading and writing; poll_read / poll_write
    // / poll_flush all delegate here.
    pub fn poll(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<(), ArqError<L::Error>>> {
        self.delivered = 0;
        let mut st;
        loop {
            let frames_in = match self.poll_recv(cx) {
                Ok(n) => n,
                Err(e) => return Poll::Ready(Err(e)),
            };
            st = match self.state {
                State::Syncing => self.poll_syncing(cx, buf),
                State::AwaitingAck { .. } => self.poll_awaiting_ack(cx, buf),
                State::Idle => self.poll_idle(cx, buf),
            };
            let bytes_out = match self.poll_drain(cx) {
                Ok(n) => n,
                Err(e) => return Poll::Ready(Err(e)),
            };
            self.poll_deliver(buf);
            if self.delivered > 0 {
                return Poll::Ready(Ok(()));
            }
            if matches!(st, Poll::Ready(_)) {
                return st;
            }
            // Keep going while this pass made progress; otherwise the lower
            // layer is pending and has registered a waker.
            if frames_in == 0 && bytes_out == 0 {
                break;
            }
        }
        st
    }

    // Drive the machine until bytes are available in buf, or it errors.
    pub fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, ArqError<L::Error>>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        match self.poll(cx, buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(Ok(self.delivered)),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
        }
    }

    // Accept bytes into the send path (payload ring + staging), then drive the
    // machine so queued payloads make progress.
    pub fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, ArqError<L::Error>>> {
        let mut consumed = 0;
        let mut rem = buf;
        // Fill staging first; a full staging chunk moves into the payload ring.
        'accept: loop {
            if self.staging.len == MAX_PAYLOAD {
                if self.payload_tx.push(self.staging) {
                    self.staging = Payload::default();
                } else {
                    break 'accept;
                }
            }
            if rem.is_empty() {
                break 'accept;
            }
            let n = core::cmp::min(rem.len(), MAX_PAYLOAD - self.staging.len);
            self.staging.data[self.staging.len..self.staging.len + n].copy_from_slice(&rem[..n]);
            self.staging.len += n;
            rem = &rem[n..];
            consumed += n;
            if self.staging.len < MAX_PAYLOAD {
                break 'accept;
            }
        }
        if let Poll::Ready(Err(e)) = self.poll(cx, &mut []) {
            return Poll::Ready(Err(e));
        }
        if consumed == 0 && !rem.is_empty() {
            Poll::Pending
        } else {
            Poll::Ready(Ok(consumed))
        }
    }

    // Push a partial staged payload, then drive the machine until everything
    // is queued, sent, and acknowledged.
    pub fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ArqError<L::Error>>> {
        loop {
            if self.staging.len > 0 && self.payload_tx.push(self.staging) {
                self.staging = Payload::default();
            }
            if self.quiescent() {
                return match Lower::poll_flush(&mut self.lower, cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                    Poll::Ready(Err(e)) => Poll::Ready(Err(ArqError::Io(e))),
                };
            }
            match self.poll(cx, &mut []) {
                Poll::Pending => {
                    // The machine may have drained to quiescent on this poll.
                    if self.quiescent() {
                        continue;
                    }
                    return Poll::Pending;
                }
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
        }
    }

    // Nothing staged, nothing queued, nothing in flight: ready to flush.
    fn quiescent(&self) -> bool {
        self.staging.len == 0
            && self.payload_tx.is_empty()
            && self.tx_fifo.is_empty()
            && matches!(self.state, State::Idle)
    }

    // Pull bytes from the lower layer and hand each complete frame to the
    // state machine. A frame's size is given by its length field; bytes of
    // the following frame are rolled forward. If the tx fifo is full,
    // draining stops so the control reply a next frame would push always has
    // room; the next poll continues. Returns the frames processed.
    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Result<usize, ArqError<L::Error>> {
        let mut processed = 0;
        loop {
            if self.tx_fifo.is_full() {
                return Ok(processed);
            }
            if self.frame_len >= HDR_LEN {
                let total = HDR_LEN + self.frame_buf[2] as usize;
                if total > MAX_FRAME {
                    self.recv.framing += 1;
                    self.frame_len = 0;
                    return Err(ArqError::Framing);
                }
                if self.frame_len >= total {
                    let p = parse_frame(&self.crc, &self.frame_buf[..total]);
                    self.frame_buf.copy_within(total..self.frame_len, 0);
                    self.frame_len -= total;
                    self.handle_message(p);
                    processed += 1;
                    continue;
                }
            }
            match Lower::poll_read(
                &mut self.lower,
                cx,
                &mut self.frame_buf[self.frame_len..MAX_FRAME],
            ) {
                Poll::Pending => return Ok(processed),
                Poll::Ready(Ok(0)) => return Err(ArqError::Closed),
                Poll::Ready(Ok(n)) => self.frame_len += n,
                Poll::Ready(Err(e)) => return Err(ArqError::Io(e)),
            }
        }
    }

    // Write queued frames to the lower layer; returns the bytes written.
    fn poll_drain(&mut self, cx: &mut Context<'_>) -> Result<usize, ArqError<L::Error>> {
        let mut written = 0;
        loop {
            let frame = match self.tx_fifo.front() {
                Some(f) => *f,
                None => return Ok(written),
            };
            let n =
                match Lower::poll_write(&mut self.lower, cx, &frame.data[self.tx_off..frame.len]) {
                    Poll::Pending => return Ok(written),
                    Poll::Ready(Ok(0)) => return Err(ArqError::Closed),
                    Poll::Ready(Ok(n)) => n,
                    Poll::Ready(Err(e)) => return Err(ArqError::Io(e)),
                };
            written += n;
            self.tx_off += n;
            if self.tx_off >= frame.len {
                self.tx_off = 0;
                self.tx_fifo.pop();
            } else {
                return Ok(written);
            }
        }
    }

    // Hand over a received payload, if any, into the caller's buffer.
    fn poll_deliver(&mut self, buf: &mut [u8]) {
        if buf.is_empty() {
            return;
        }
        let (payload, off) = match self.delivering.take() {
            Some(d) => (d.payload, d.off),
            None => match self.payload_rx.pop() {
                Some(p) => (p, 0),
                None => return,
            },
        };
        let n = core::cmp::min(buf.len(), payload.len - off);
        buf[..n].copy_from_slice(&payload.data[off..off + n]);
        if off + n < payload.len {
            self.delivering = Some(Delivering {
                payload,
                off: off + n,
            });
        }
        self.delivered = n;
    }

    // ---- variant methods: one per phase of the link ----

    fn poll_syncing(
        &mut self,
        _cx: &mut Context<'_>,
        _buf: &mut [u8],
    ) -> Poll<Result<(), ArqError<L::Error>>> {
        if self.timer.now() >= self.next_syn_at {
            self.next_syn_at = self.timer.now() + SYNC_PERIOD;
            if self
                .tx_fifo
                .push(ctl_frame(&self.crc, MSG_TYPE_SYN, self.next_pkt_id))
            {
                self.send.syn += 1;
            }
        }
        Poll::Pending
    }

    fn poll_awaiting_ack(
        &mut self,
        _cx: &mut Context<'_>,
        _buf: &mut [u8],
    ) -> Poll<Result<(), ArqError<L::Error>>> {
        // ACK / NAK for the pending id are handled in handle_message;
        // this method only retransmits on deadline expiry.
        let State::AwaitingAck {
            attempts, deadline, ..
        } = self.state
        else {
            return Poll::Ready(Ok(()));
        };
        if self.timer.now() < deadline {
            return Poll::Pending;
        }
        if attempts >= self.max_resend {
            self.pending = None;
            self.state = State::Idle;
            self.send.lost += 1;
            return Poll::Ready(Err(ArqError::Timeout));
        }
        self.resend();
        Poll::Pending
    }

    fn poll_idle(
        &mut self,
        _cx: &mut Context<'_>,
        _buf: &mut [u8],
    ) -> Poll<Result<(), ArqError<L::Error>>> {
        // Write side: take the next payload from the upper layer. The payload is
        // only popped when the fifo has room, otherwise it would be lost and the
        // id sequence would gap.
        if !self.tx_fifo.is_full() {
            if let Some(payload) = self.payload_tx.pop() {
                let id = self.next_pkt_id;
                self.next_pkt_id = id.wrapping_add(1);
                let frame = build_dat_frame(&self.crc, id, &payload.data[..payload.len]);
                if self.tx_fifo.push(frame) {
                    self.send.dat += 1;
                    self.pending = Some(payload);
                    self.state = State::AwaitingAck {
                        pending_id: id,
                        attempts: 0,
                        deadline: self.timer.now() + ACK_TIMEOUT,
                    };
                }
            }
        }
        Poll::Pending
    }

    // Re-send the pending DAT under the same id (on NAK and on ack timeout).
    // If the fifo is full the retransmit is deferred: no attempt is consumed
    // and the past deadline retries it on the next poll.
    fn resend(&mut self) {
        let State::AwaitingAck {
            pending_id,
            attempts,
            ..
        } = self.state
        else {
            return;
        };
        let Some(payload) = self.pending.as_ref() else {
            return;
        };
        let frame = build_dat_frame(&self.crc, pending_id, &payload.data[..payload.len]);
        if self.tx_fifo.push(frame) {
            self.send.retrans += 1;
            self.state = State::AwaitingAck {
                pending_id,
                attempts: attempts + 1,
                deadline: self.timer.now() + ACK_TIMEOUT,
            };
        }
    }

    // ---- inbound message handling ----

    fn handle_message(&mut self, p: Parsed) {
        if !p.crc_ok {
            self.recv.crc += 1;
            if p.ty == MSG_TYPE_DAT && self.tx_fifo.push(ctl_frame(&self.crc, MSG_TYPE_NAK, p.id)) {
                self.send.nak += 1;
            }
            return;
        }
        match p.ty {
            MSG_TYPE_ACK => {
                // A matching ack completes the in-flight DAT; any other id is
                // ignored.
                if let State::AwaitingAck { pending_id, .. } = self.state {
                    if pending_id == p.id {
                        self.pending = None;
                        self.state = State::Idle;
                    }
                }
            }
            MSG_TYPE_NAK => {
                if let State::AwaitingAck { pending_id, .. } = self.state {
                    if pending_id == p.id {
                        self.resend();
                    }
                }
            }
            MSG_TYPE_SYN => {
                // Peer re-sync: re-anchor the receive sequence. Drop any
                // pending DAT, which the peer would ACK as a duplicate and
                // silently lose, and go back to syncing.
                self.recv_pkt_id = p.id.wrapping_sub(1);
                if matches!(self.state, State::AwaitingAck { .. }) {
                    self.pending = None;
                    self.send.lost += 1;
                    self.state = State::Syncing;
                }
                if self
                    .tx_fifo
                    .push(ctl_frame(&self.crc, MSG_TYPE_RYN, self.next_pkt_id))
                {
                    self.send.ryn += 1;
                }
            }
            MSG_TYPE_RYN => {
                self.recv_pkt_id = p.id.wrapping_sub(1);
                if matches!(self.state, State::Syncing) {
                    self.state = State::Idle;
                }
            }
            MSG_TYPE_DAT => {
                // Stop-and-wait: only the next expected id is accepted in
                // order; anything else is a duplicate and still gets an ACK.
                let expected = self.recv_pkt_id.wrapping_add(1);
                if p.id != expected {
                    self.recv.dup += 1;
                    if self.tx_fifo.push(ctl_frame(&self.crc, MSG_TYPE_ACK, p.id)) {
                        self.send.ack += 1;
                    }
                    return;
                }
                self.recv_pkt_id = p.id;
                let pl = Payload {
                    data: p.payload,
                    len: p.plen,
                };
                if self.payload_rx.push(pl) {
                    self.recv.dat += 1;
                    if self.tx_fifo.push(ctl_frame(&self.crc, MSG_TYPE_ACK, p.id)) {
                        self.send.ack += 1;
                    }
                } else {
                    // Backpressure: force the peer to retransmit.
                    self.recv.backpressure += 1;
                    if self.tx_fifo.push(ctl_frame(&self.crc, MSG_TYPE_NAK, p.id)) {
                        self.send.nak += 1;
                    }
                }
            }
            _ => {}
        }
    }
}

// ===================== trait impls =====================
// Both trait pairs funnel into the single entry point Arq::poll; the machine
// is never polled anywhere else.

#[cfg(feature = "embedded-io")]
impl<L, T, C> embedded_io_async::ErrorType for Arq<L, T, C>
where
    L: Lower,
    T: Timer,
    C: Crc,
    L::Error: embedded_io_async::Error,
{
    type Error = ArqError<L::Error>;
}

#[cfg(feature = "embedded-io")]
impl<L, T, C> embedded_io_async::Read for Arq<L, T, C>
where
    L: Lower,
    T: Timer,
    C: Crc,
    L::Error: embedded_io_async::Error,
{
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        if buf.is_empty() {
            return Ok(0);
        }
        ReadDrive { arq: self, buf }.await
    }
}

#[cfg(feature = "embedded-io")]
impl<L, T, C> embedded_io_async::Write for Arq<L, T, C>
where
    L: Lower,
    T: Timer,
    C: Crc,
    L::Error: embedded_io_async::Error,
{
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        WriteDrive { arq: self, buf }.await
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        FlushDrive { arq: self }.await
    }
}

// embedded-io-async is async-fn based; these thin futures drive the
// poll-based state machine from within the async trait methods.
#[cfg(feature = "embedded-io")]
struct ReadDrive<'a, L, T, C> {
    arq: &'a mut Arq<L, T, C>,
    buf: &'a mut [u8],
}

#[cfg(feature = "embedded-io")]
impl<L: Lower, T: Timer, C: Crc> core::future::Future for ReadDrive<'_, L, T, C> {
    type Output = Result<usize, ArqError<L::Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.arq.poll_read(cx, this.buf)
    }
}

#[cfg(feature = "embedded-io")]
struct WriteDrive<'a, L, T, C> {
    arq: &'a mut Arq<L, T, C>,
    buf: &'a [u8],
}

#[cfg(feature = "embedded-io")]
impl<L: Lower, T: Timer, C: Crc> core::future::Future for WriteDrive<'_, L, T, C> {
    type Output = Result<usize, ArqError<L::Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.arq.poll_write(cx, this.buf)
    }
}

#[cfg(feature = "embedded-io")]
struct FlushDrive<'a, L, T, C> {
    arq: &'a mut Arq<L, T, C>,
}

#[cfg(feature = "embedded-io")]
impl<L: Lower, T: Timer, C: Crc> core::future::Future for FlushDrive<'_, L, T, C> {
    type Output = Result<(), ArqError<L::Error>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.arq.poll_flush(cx)
    }
}

#[cfg(feature = "tokio")]
impl<S, T, C> tokio::io::AsyncRead for Arq<TokioLower<S>, T, C>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    T: Timer + Unpin,
    C: Crc + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<tokio::io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let n = match self.get_mut().poll_read(cx, buf.initialize_unfilled()) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e.into())),
            Poll::Ready(Ok(n)) => n,
        };
        buf.set_filled(n);
        Poll::Ready(Ok(()))
    }
}

#[cfg(feature = "tokio")]
impl<S, T, C> tokio::io::AsyncWrite for Arq<TokioLower<S>, T, C>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    T: Timer + Unpin,
    C: Crc + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<tokio::io::Result<usize>> {
        match self.get_mut().poll_write(cx, buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
            Poll::Ready(Ok(n)) => Poll::Ready(Ok(n)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<tokio::io::Result<()>> {
        match self.get_mut().poll_flush(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<tokio::io::Result<()>> {
        // Graceful shutdown: make sure everything is sent and acknowledged.
        match self.get_mut().poll_flush(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::convert::Infallible;
    use core::task::Waker;

    struct MockLink {
        rx: Vec<u8>,
        tx: Vec<u8>,
        write_chunk: usize,
        partial_pending: bool,
    }

    impl MockLink {
        fn new() -> Self {
            Self {
                rx: Vec::new(),
                tx: Vec::new(),
                write_chunk: 0,
                partial_pending: false,
            }
        }
    }

    impl Lower for MockLink {
        type Error = Infallible;

        fn poll_read(
            &mut self,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<Result<usize, Infallible>> {
            if self.rx.is_empty() {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let n = core::cmp::min(buf.len(), self.rx.len());
            buf[..n].copy_from_slice(&self.rx[..n]);
            self.rx.drain(..n);
            Poll::Ready(Ok(n))
        }

        fn poll_write(
            &mut self,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, Infallible>> {
            if self.partial_pending {
                self.partial_pending = false;
                return Poll::Pending;
            }
            let n = if self.write_chunk == 0 {
                buf.len()
            } else {
                core::cmp::min(buf.len(), self.write_chunk)
            };
            self.tx.extend_from_slice(&buf[..n]);
            if n < buf.len() {
                self.partial_pending = true;
            }
            Poll::Ready(Ok(n))
        }

        fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }
    }

    struct MockTimer(std::cell::Cell<u64>);

    impl MockTimer {
        fn new() -> Self {
            Self(std::cell::Cell::new(0))
        }
        fn advance(&self, ms: u64) {
            self.0.set(self.0.get() + ms);
        }
    }

    impl Timer for MockTimer {
        fn now(&self) -> Instant {
            Instant(self.0.get())
        }
    }

    fn wire_ctl(ty: u8, id: u8) -> Vec<u8> {
        let f = ctl_frame(&Crc16X25, ty, id);
        f.data[..f.len].to_vec()
    }

    fn wire_dat(id: u8, payload: &[u8]) -> Vec<u8> {
        let f = build_dat_frame(&Crc16X25, id, payload);
        f.data[..f.len].to_vec()
    }

    // The stream carries back-to-back frames; returns the last complete one.
    fn last_frame(tx: &[u8]) -> Option<&[u8]> {
        let mut start = 0;
        let mut last = None;
        while start + HDR_LEN <= tx.len() {
            let total = HDR_LEN + tx[start + 2] as usize;
            if start + total > tx.len() {
                break;
            }
            last = Some(&tx[start..start + total]);
            start += total;
        }
        last
    }

    fn count_frames(tx: &[u8], ty: u8) -> usize {
        let mut count = 0;
        let mut start = 0;
        while start + HDR_LEN <= tx.len() {
            let total = HDR_LEN + tx[start + 2] as usize;
            if start + total > tx.len() {
                break;
            }
            if tx[start] == ty {
                count += 1;
            }
            start += total;
        }
        count
    }

    fn synced() -> Arq<MockLink, MockTimer, Crc16X25> {
        let mut arq = Arq::new(MockLink::new(), MockTimer::new(), Crc16X25, 3);
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        assert!(matches!(arq.poll_write(&mut cx, &[]), Poll::Ready(Ok(0))));
        arq.lower.rx.extend(wire_ctl(MSG_TYPE_RYN, arq.next_pkt_id));
        let mut buf = [0u8; 16];
        assert!(matches!(arq.poll_read(&mut cx, &mut buf), Poll::Pending));
        assert!(matches!(arq.state, State::Idle));
        arq
    }

    #[test]
    fn crc16_x25_check_value() {
        let crc = Crc16X25;
        assert_eq!(
            crc.finalize(crc.crc_update(crc.init(), b"123456789")),
            0x906E
        );
    }

    #[test]
    fn syn_sent_on_first_poll_and_repeated() {
        let mut arq = Arq::new(MockLink::new(), MockTimer::new(), Crc16X25, 3);
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        assert!(matches!(arq.poll_write(&mut cx, &[]), Poll::Ready(Ok(0))));
        assert_eq!(arq.lower.tx.len(), HDR_LEN);
        arq.timer.advance(SYNC_PERIOD.as_millis());
        assert!(matches!(arq.poll_write(&mut cx, &[]), Poll::Ready(Ok(0))));
        assert_eq!(arq.lower.tx.len(), 2 * HDR_LEN);
    }

    #[test]
    fn handshake_reaches_idle() {
        let mut arq = Arq::new(MockLink::new(), MockTimer::new(), Crc16X25, 3);
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        assert!(matches!(arq.poll_write(&mut cx, &[]), Poll::Ready(Ok(0))));
        arq.lower.rx.extend(wire_ctl(MSG_TYPE_SYN, 7));
        arq.lower.rx.extend(wire_ctl(MSG_TYPE_RYN, arq.next_pkt_id));
        let mut buf = [0u8; 16];
        assert!(matches!(arq.poll_read(&mut cx, &mut buf), Poll::Pending));
        assert!(matches!(arq.state, State::Idle));
        assert_eq!(arq.send_metrics().syn, 1);
        assert_eq!(arq.send_metrics().ryn, 1);
    }

    #[test]
    fn send_dat_and_ack() {
        let mut arq = synced();
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        assert!(matches!(
            arq.poll_write(&mut cx, b"hello"),
            Poll::Ready(Ok(5))
        ));
        // flush pushes the staged payload, sends DAT(0, "hello"), waits for ack
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Pending));
        assert!(matches!(
            arq.state,
            State::AwaitingAck {
                pending_id: 0,
                attempts: 0,
                ..
            }
        ));
        arq.lower.rx.extend(wire_ctl(MSG_TYPE_ACK, 0));
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Ready(Ok(()))));
        assert!(matches!(arq.state, State::Idle));
        assert_eq!(arq.send_metrics().dat, 1);
        let frame = last_frame(&arq.lower.tx).expect("DAT frame on the wire");
        assert_eq!(frame[0], MSG_TYPE_DAT);
        assert_eq!(frame[1], 0);
        assert_eq!(&frame[HDR_LEN..], b"hello");
    }

    #[test]
    fn retransmit_on_nak() {
        let mut arq = synced();
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        let _ = arq.poll_write(&mut cx, b"hello");
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Pending));
        arq.lower.rx.extend(wire_ctl(MSG_TYPE_NAK, 0));
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Pending));
        assert!(matches!(
            arq.state,
            State::AwaitingAck {
                pending_id: 0,
                attempts: 1,
                ..
            }
        ));
        assert_eq!(arq.send_metrics().retrans, 1);
        assert_eq!(count_frames(&arq.lower.tx, MSG_TYPE_DAT), 2);
    }

    #[test]
    fn retransmit_on_timeout_and_loss() {
        let mut arq = synced();
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        let _ = arq.poll_write(&mut cx, b"hello");
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Pending));
        arq.timer.advance(ACK_TIMEOUT.as_millis());
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Pending));
        assert!(matches!(arq.state, State::AwaitingAck { attempts: 1, .. }));
        arq.timer.advance(ACK_TIMEOUT.as_millis());
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Pending));
        assert!(matches!(arq.state, State::AwaitingAck { attempts: 2, .. }));
        arq.timer.advance(ACK_TIMEOUT.as_millis());
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Pending));
        assert!(matches!(arq.state, State::AwaitingAck { attempts: 3, .. }));
        arq.timer.advance(ACK_TIMEOUT.as_millis());
        assert!(matches!(
            arq.poll_flush(&mut cx),
            Poll::Ready(Err(ArqError::Timeout))
        ));
        assert!(matches!(arq.state, State::Idle));
        assert_eq!(arq.send_metrics().lost, 1);
        assert_eq!(arq.send_metrics().retrans, 3);
    }

    #[test]
    fn receive_dat() {
        let mut arq = synced();
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        arq.lower.rx.extend(wire_dat(0, b"world"));
        let mut buf = [0u8; 8];
        assert!(matches!(
            arq.poll_read(&mut cx, &mut buf[..5]),
            Poll::Ready(Ok(5))
        ));
        assert_eq!(&buf[..5], b"world");
        assert_eq!(arq.recv_metrics().dat, 1);
        let frame = last_frame(&arq.lower.tx).expect("ACK frame on the wire");
        assert_eq!(frame[0], MSG_TYPE_ACK);
        assert_eq!(frame[1], 0);
        assert!(matches!(arq.poll_read(&mut cx, &mut buf), Poll::Pending));
    }

    #[test]
    fn partial_read_across_calls() {
        let mut arq = synced();
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        arq.lower.rx.extend(wire_dat(0, b"abcdefgh"));
        let mut buf = [0u8; 3];
        assert!(matches!(
            arq.poll_read(&mut cx, &mut buf),
            Poll::Ready(Ok(3))
        ));
        assert_eq!(&buf, b"abc");
        assert!(matches!(
            arq.poll_read(&mut cx, &mut buf),
            Poll::Ready(Ok(3))
        ));
        assert_eq!(&buf, b"def");
        assert!(matches!(
            arq.poll_read(&mut cx, &mut buf),
            Poll::Ready(Ok(2))
        ));
        assert_eq!(&buf[..2], b"gh");
        assert!(matches!(arq.poll_read(&mut cx, &mut buf), Poll::Pending));
    }

    #[test]
    fn duplicate_dat_acked() {
        let mut arq = synced();
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        arq.lower.rx.extend(wire_dat(0, b"abc"));
        arq.lower.rx.extend(wire_dat(0, b"abc"));
        let mut buf = [0u8; 8];
        assert!(matches!(
            arq.poll_read(&mut cx, &mut buf[..3]),
            Poll::Ready(Ok(3))
        ));
        assert_eq!(arq.recv_metrics().dat, 1);
        assert_eq!(arq.recv_metrics().dup, 1);
        assert_eq!(count_frames(&arq.lower.tx, MSG_TYPE_ACK), 2);
    }

    #[test]
    fn crc_error_triggers_nak() {
        let mut arq = synced();
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        let mut bad = wire_dat(0, b"abc");
        bad[HDR_LEN] ^= 0xFF;
        arq.lower.rx.extend(bad);
        assert!(matches!(arq.poll_write(&mut cx, &[]), Poll::Ready(Ok(0))));
        assert_eq!(arq.recv_metrics().crc, 1);
        let frame = last_frame(&arq.lower.tx).expect("NAK frame on the wire");
        assert_eq!(frame[0], MSG_TYPE_NAK);
        assert_eq!(frame[1], 0);
    }

    #[test]
    fn incomplete_frame_is_pending() {
        let mut arq = synced();
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        arq.lower.rx.extend(vec![1, 2, 0]);
        let mut buf = [0u8; 4];
        assert!(matches!(arq.poll_read(&mut cx, &mut buf), Poll::Pending));
        assert_eq!(arq.recv_metrics().framing, 0);
    }

    #[test]
    fn oversized_stream_is_framing_error() {
        let mut arq = synced();
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        arq.lower
            .rx
            .extend(vec![1, 2, (MAX_PAYLOAD + 1) as u8, 0, 0]);
        assert!(matches!(
            arq.poll_write(&mut cx, &[]),
            Poll::Ready(Err(ArqError::Framing))
        ));
        assert_eq!(arq.recv_metrics().framing, 1);
    }

    #[test]
    fn backpressure_triggers_nak() {
        let mut arq = synced();
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        for i in 0..=PAYLOAD_RING as u8 {
            arq.lower.rx.extend(wire_dat(i, b"filler"));
        }
        assert!(matches!(arq.poll_write(&mut cx, &[]), Poll::Ready(Ok(0))));
        assert_eq!(arq.recv_metrics().dat, PAYLOAD_RING as u32);
        assert_eq!(arq.recv_metrics().backpressure, 1);
        let frame = last_frame(&arq.lower.tx).expect("NAK frame on the wire");
        assert_eq!(frame[0], MSG_TYPE_NAK);
        assert_eq!(frame[1], PAYLOAD_RING as u8);
    }

    #[test]
    fn write_backpressure() {
        let mut arq = synced();
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        let big = vec![0u8; MAX_PAYLOAD * (PAYLOAD_RING + 1)];
        match arq.poll_write(&mut cx, &big) {
            Poll::Ready(Ok(n)) => assert_eq!(n, MAX_PAYLOAD * (PAYLOAD_RING + 1)),
            other => panic!("expected a full ring plus staging to be accepted, got {other:?}"),
        }
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Pending));
        arq.lower.rx.extend(wire_ctl(MSG_TYPE_ACK, 0));
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Pending));
        for id in 1..4u8 {
            arq.lower.rx.extend(wire_ctl(MSG_TYPE_ACK, id));
            assert!(matches!(arq.poll_flush(&mut cx), Poll::Pending));
        }
        arq.lower.rx.extend(wire_ctl(MSG_TYPE_ACK, 4));
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Ready(Ok(()))));
        assert!(matches!(arq.state, State::Idle));
    }

    #[test]
    fn syn_from_peer_resets_sequence() {
        let mut arq = synced();
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        arq.lower.rx.extend(wire_ctl(MSG_TYPE_SYN, 7));
        assert!(matches!(arq.poll_write(&mut cx, &[]), Poll::Ready(Ok(0))));
        assert_eq!(arq.recv_pkt_id, 6);
        let frame = last_frame(&arq.lower.tx).expect("RYN frame on the wire");
        assert_eq!(frame[0], MSG_TYPE_RYN);
        assert_eq!(frame[1], arq.next_pkt_id);
    }

    #[test]
    fn partial_lower_write_spans_polls() {
        let mut arq = synced();
        arq.lower.write_chunk = 2;
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        assert!(matches!(
            arq.poll_write(&mut cx, b"hello"),
            Poll::Ready(Ok(5))
        ));
        while arq.lower.tx.len() < 2 * HDR_LEN + 5 {
            assert!(matches!(arq.poll_flush(&mut cx), Poll::Pending));
        }
        assert_eq!(arq.lower.tx.len(), 2 * HDR_LEN + 5);
        let frame = last_frame(&arq.lower.tx).expect("DAT frame on the wire");
        assert_eq!(frame[0], MSG_TYPE_DAT);
        assert_eq!(&frame[HDR_LEN..], b"hello");
        arq.lower.rx.extend(wire_ctl(MSG_TYPE_ACK, 0));
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Ready(Ok(()))));
    }

    #[test]
    fn dat_not_dropped_when_fifo_full_on_idle() {
        let mut arq = synced();
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        // Three in-order peer DATs fill the tx fifo with ACKs.
        for id in 0..TX_FIFO as u8 {
            arq.lower.rx.extend(wire_dat(id, b"fill"));
        }
        let big = vec![0u8; MAX_PAYLOAD];
        assert!(matches!(
            arq.poll_write(&mut cx, &big),
            Poll::Ready(Ok(MAX_PAYLOAD))
        ));
        // The payload must be sent with an unbroken sequence, not dropped.
        assert!(matches!(
            arq.state,
            State::AwaitingAck {
                pending_id: 0,
                attempts: 0,
                ..
            }
        ));
        assert_eq!(arq.send_metrics().dat, 1);
        assert_eq!(count_frames(&arq.lower.tx, MSG_TYPE_DAT), 1);
        assert_eq!(count_frames(&arq.lower.tx, MSG_TYPE_ACK), 3);
        arq.lower.rx.extend(wire_ctl(MSG_TYPE_ACK, 0));
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Ready(Ok(()))));
    }

    #[test]
    fn retransmit_deferred_when_fifo_full() {
        let mut arq = synced();
        arq.lower.write_chunk = 1;
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        assert!(matches!(
            arq.poll_write(&mut cx, b"hello"),
            Poll::Ready(Ok(5))
        ));
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Pending));
        assert_eq!(arq.lower.tx.len(), HDR_LEN + 1);
        // Fill the remaining fifo slots with ACKs for two peer DATs.
        arq.lower.rx.extend(wire_dat(0, b"aa"));
        arq.lower.rx.extend(wire_dat(1, b"bb"));
        arq.timer.advance(ACK_TIMEOUT.as_millis());
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Pending));
        // The retransmit cannot be queued while the fifo is full, so no
        // attempt may be consumed.
        assert!(matches!(
            arq.state,
            State::AwaitingAck {
                pending_id: 0,
                attempts: 0,
                ..
            }
        ));
        assert_eq!(arq.send_metrics().retrans, 0);
        // With room in the fifo the retransmit goes out on the next poll.
        arq.lower.write_chunk = 0;
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Pending));
        assert!(matches!(
            arq.state,
            State::AwaitingAck {
                pending_id: 0,
                attempts: 1,
                ..
            }
        ));
        assert_eq!(arq.send_metrics().retrans, 1);
        assert_eq!(count_frames(&arq.lower.tx, MSG_TYPE_DAT), 2);
        arq.lower.rx.extend(wire_ctl(MSG_TYPE_ACK, 0));
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Ready(Ok(()))));
    }

    #[test]
    fn peer_syn_drops_pending_dat() {
        let mut arq = synced();
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        let _ = arq.poll_write(&mut cx, b"hello");
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Pending));
        assert!(matches!(
            arq.state,
            State::AwaitingAck { pending_id: 0, .. }
        ));
        // Peer re-syncs mid-stream: the pending DAT is dropped and counted as
        // lost; the link goes back to syncing.
        arq.lower.rx.extend(wire_ctl(MSG_TYPE_SYN, 7));
        assert!(matches!(arq.poll_write(&mut cx, &[]), Poll::Ready(Ok(0))));
        assert!(matches!(arq.state, State::Syncing));
        assert_eq!(arq.send_metrics().lost, 1);
        assert_eq!(arq.recv_pkt_id, 6);
        let frame = last_frame(&arq.lower.tx).expect("RYN frame on the wire");
        assert_eq!(frame[0], MSG_TYPE_RYN);
        assert_eq!(frame[1], arq.next_pkt_id);
        // RYN completes the handshake again.
        arq.lower.rx.extend(wire_ctl(MSG_TYPE_RYN, 0));
        let mut buf = [0u8; 4];
        assert!(matches!(arq.poll_read(&mut cx, &mut buf), Poll::Pending));
        assert!(matches!(arq.state, State::Idle));
    }

    #[test]
    fn flush_pending_before_handshake() {
        let mut arq = Arq::new(MockLink::new(), MockTimer::new(), Crc16X25, 3);
        let w = Waker::noop().clone();
        let mut cx = Context::from_waker(&w);
        for _ in 0..5 {
            assert!(matches!(arq.poll_flush(&mut cx), Poll::Pending));
            arq.timer.advance(SYNC_PERIOD.as_millis());
        }
        // Handshake completes; flush returns Ready once the link is idle.
        arq.lower.rx.extend(wire_ctl(MSG_TYPE_RYN, arq.next_pkt_id));
        assert!(matches!(arq.poll_flush(&mut cx), Poll::Ready(Ok(()))));
    }
}
