use core::marker::PhantomData;
use core::task::{Context, Poll};
use core::time::Duration;

use crate::ack_codec::{AckCodec, BchAckCodec};
use crate::crc::Crc16;
use crate::error::ArqError;
use crate::frame::{AckFrame, DatFrame, Frame, MAX_PAYLOAD, MAX_SEQ};
use crate::timer::Timer;
use crate::transport::Transport;

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;

/// Maximum wire-frame length, including the header.
pub const MAX_FRAME: usize = 256;

const DEFAULT_RTO_INITIAL: Duration = Duration::from_millis(250);
const DEFAULT_RTO_MAX: Duration = Duration::from_secs(4);
const DEFAULT_RETRY_LIMIT: usize = 16;

/// Work units (lower reads and decoded or discarded frames) one receive batch
/// may spend before yielding.
const RX_BUDGET: usize = 32;
/// Receive/transmit rounds a flush or shutdown poll may run before yielding.
const MAX_ROUNDS: usize = 32;

fn dist(a: u16, b: u16) -> u16 {
    b.wrapping_sub(a) & (MAX_SEQ - 1)
}

/// Progress of the stream, derived by `transition` from `tx_done` and
/// `rx_complete`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Neither direction has finished.
    Active,
    /// Our `FIN` and all data before it were acknowledged.
    SendDone,
    /// The peer's `FIN` was received, its data drained and its ACK flushed.
    RecvDone,
    /// Both directions finished. Only ACKs for duplicate frames are still
    /// serviced, by `poll_completed`.
    Done,
}

/// What `service_tx` may send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxPolicy {
    /// Only ACKs.
    Acks,
    /// ACKs, retransmissions and data, requesting an ACK for the last frame.
    Flush,
    /// ACKs, retransmissions and data.
    Full,
}

pub(crate) enum Op<'a> {
    Read { buf: &'a mut [u8] },
    Write { buf: &'a [u8] },
    Flush,
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpOut {
    Read(usize),
    Write(usize),
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutgoingKind {
    Ack,
    Data,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// The lower write of the whole frame is outstanding.
    Write,
    /// The frame was accepted by the lower transport; its flush is pending.
    Flush,
}

#[derive(Debug, Clone, Copy)]
struct Outgoing {
    kind: OutgoingKind,
    buf: [u8; MAX_FRAME],
    total: usize,
    phase: Phase,
    /// The lower transport was offered the frame; it must not be replaced.
    started: bool,
    /// Arm the retransmission timer once the frame is flushed.
    arm_timer: bool,
}

struct RecvBatch {
    frames: usize,
    /// The batch stopped on its budget rather than on an idle input.
    more: bool,
}

#[derive(Debug, Clone, Copy)]
struct Pending {
    buf: [u8; MAX_PAYLOAD],
    len: usize,
}

/// Received application bytes awaiting `poll_read`.
///
/// Two banks of `N` payload-sized blocks, used as a ring of
/// `2 * N * MAX_PAYLOAD` bytes.
#[derive(Debug, Clone, Copy)]
struct ReadBuf<const N: usize> {
    banks: [[[u8; MAX_PAYLOAD]; N]; 2],
    head: usize,
    len: usize,
}

impl<const N: usize> ReadBuf<N> {
    const CAPACITY: usize = 2 * N * MAX_PAYLOAD;

    fn new() -> Self {
        Self {
            banks: [[[0; MAX_PAYLOAD]; N]; 2],
            head: 0,
            len: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.len
    }

    /// The rest of the block holding ring position `pos`.
    fn block(&mut self, pos: usize) -> &mut [u8] {
        let bank = pos / (N * MAX_PAYLOAD);
        let block = pos % (N * MAX_PAYLOAD) / MAX_PAYLOAD;
        &mut self.banks[bank][block][pos % MAX_PAYLOAD..]
    }

    /// Appends all of `data`, or nothing when it does not fit.
    fn push(&mut self, data: &[u8]) -> bool {
        if self.len + data.len() > Self::CAPACITY {
            return false;
        }
        let mut pos = (self.head + self.len) % Self::CAPACITY;
        let mut rest = data;
        while !rest.is_empty() {
            let block = self.block(pos);
            let n = block.len().min(rest.len());
            block[..n].copy_from_slice(&rest[..n]);
            rest = &rest[n..];
            pos = (pos + n) % Self::CAPACITY;
        }
        self.len += data.len();
        true
    }

    /// Moves up to `buf.len()` buffered bytes into `buf`.
    fn read(&mut self, buf: &mut [u8]) -> usize {
        let total = self.len.min(buf.len());
        let mut done = 0;
        while done < total {
            let block = self.block(self.head);
            let n = block.len().min(total - done);
            buf[done..done + n].copy_from_slice(&block[..n]);
            done += n;
            self.head = (self.head + n) % Self::CAPACITY;
        }
        self.len -= total;
        total
    }
}

/// The frames of a window of `N` sequence numbers starting at `base`.
///
/// Slots are addressed by distance from `base`, so the mapping holds across
/// sequence-number wrap for any `N`.
#[derive(Debug, Clone, Copy)]
struct Ring<const N: usize> {
    slots: [Option<DatFrame>; N],
    base: u16,
    head: usize,
}

impl<const N: usize> Ring<N> {
    fn new() -> Self {
        Self {
            slots: [None; N],
            base: 0,
            head: 0,
        }
    }

    fn slot(&self, sn: u16) -> Option<usize> {
        let off = dist(self.base, sn) as usize;
        (off < N).then_some((self.head + off) % N)
    }

    fn get(&self, sn: u16) -> Option<DatFrame> {
        let f = self.slots[self.slot(sn)?]?;
        debug_assert_eq!(f.sn(), sn);
        Some(f)
    }

    /// Stores `f` in its slot, unless the slot is outside the window or holds
    /// another frame.
    fn insert(&mut self, f: DatFrame) -> bool {
        let Some(i) = self.slot(f.sn()) else {
            return false;
        };
        match self.slots[i] {
            Some(old) if old.sn() != f.sn() => {
                debug_assert!(false, "slot of {} holds {}", f.sn(), old.sn());
                false
            }
            _ => {
                self.slots[i] = Some(f);
                true
            }
        }
    }

    /// Removes the frame at `base`, if any, and moves the window forward by one.
    fn advance(&mut self) -> Option<DatFrame> {
        let f = self.slots[self.head].take();
        self.head = (self.head + 1) % N;
        self.base = (self.base + 1) % MAX_SEQ;
        f
    }
}

/// A reliable, in-order byte stream over an unreliable duplex channel.
///
/// `Arq` implements [`Transport`], so it is driven by polling `poll_read`,
/// `poll_write` and `poll_flush`, and has [`Arq::poll_close`] to shut the link
/// down. Construct instances with [`ArqLayer::build`] or
/// [`ArqLayer::build_with_codec`].
///
/// Parameters:
///
/// - `N`: retransmission window, in frames. Must be even and in `2..=32`.
/// - `M`: ACK codeword length, in bytes. Must match the `AckCodecType` in
///   use, i.e. [`AckCodec<M>`].
/// - `Channel`: the lower [`Transport`]. It must be framed: one read yields
///   one frame and one write carries one frame.
/// - `Crc`: the [`Crc16`] algorithm used to protect frames.
/// - `AckCodecType`: the [`AckCodec`] used to protect ACK frames.
/// - `Tmr`: the [`Timer`] that schedules retransmissions.
///
/// `poll_read` returns `Ok(0)` when the peer has closed its stream and the buffer
/// is drained. Writing after the link is closed fails with [`ArqError::Closed`].
#[derive(Debug)]
#[allow(private_bounds)]
pub struct Arq<const N: usize, const M: usize, Channel, Crc, AckCodecType, Tmr>
where
    AckCodecType: AckCodec<M>,
    Channel: Transport,
{
    channel: Channel,
    crc: Crc,
    timer: Tmr,
    timer_running: bool,
    rto: Duration,
    rto_initial: Duration,
    rto_max: Duration,
    retx: usize,
    retry_limit: usize,
    retries: usize,
    failed: bool,
    terminal_error: Option<ArqError<Channel::Error>>,
    state: State,
    sb: u16,
    w: usize,
    r: u16,
    sbuf: Ring<N>,
    fin_armed: bool,
    fin_sent: bool,
    fin_acked: bool,
    tx_done: bool,
    pending: Pending,
    outgoing: Option<Outgoing>,
    ack_pending: Option<u16>,
    rn: u16,
    acount: usize,
    rx_finished: bool,
    rbuf: Ring<N>,
    rx_pending: [u8; MAX_FRAME],
    rx_eof: bool,
    read_buf: ReadBuf<N>,
    p_ack_codec: PhantomData<fn() -> AckCodecType>,
}

impl<const N: usize, const M: usize, Channel, Crc, AckCodecType, Tmr> Unpin
    for Arq<N, M, Channel, Crc, AckCodecType, Tmr>
where
    Channel: Transport + Unpin,
    Crc: Unpin,
    AckCodecType: AckCodec<M>,
    Tmr: Unpin,
{
}

#[allow(private_bounds)]
impl<const N: usize, const M: usize, Channel, Crc, AckCodecType, Tmr>
    Arq<N, M, Channel, Crc, AckCodecType, Tmr>
where
    AckCodecType: AckCodec<M>,
    Channel: Transport,
{
    fn new(channel: Channel, crc: Crc, timer: Tmr) -> Self {
        Self {
            channel,
            crc,
            timer,
            timer_running: false,
            rto: DEFAULT_RTO_INITIAL,
            rto_initial: DEFAULT_RTO_INITIAL,
            rto_max: DEFAULT_RTO_MAX,
            retx: 0,
            retry_limit: DEFAULT_RETRY_LIMIT,
            retries: 0,
            failed: false,
            terminal_error: None,
            state: State::Active,
            sb: 0,
            w: 0,
            r: 0,
            sbuf: Ring::new(),
            fin_armed: false,
            fin_sent: false,
            fin_acked: false,
            tx_done: false,
            pending: Pending {
                buf: [0; MAX_PAYLOAD],
                len: 0,
            },
            outgoing: None,
            ack_pending: None,
            rn: 0,
            acount: 0,
            rx_finished: false,
            rbuf: Ring::new(),
            rx_pending: [0; MAX_FRAME],
            rx_eof: false,
            read_buf: ReadBuf::new(),
            p_ack_codec: PhantomData,
        }
    }

    #[cfg(test)]
    fn set_seq(&mut self, sn: u16) {
        self.sb = sn;
        self.r = sn;
        self.rn = sn;
        self.sbuf.base = sn;
        self.rbuf.base = sn;
    }
}

#[allow(private_bounds)]
impl<const N: usize, const M: usize, Channel, Crc, AckCodecType, Tmr>
    Arq<N, M, Channel, Crc, AckCodecType, Tmr>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: Transport,
    Tmr: Timer,
{
    pub(crate) fn poll_op(
        &mut self,
        cx: &mut Context<'_>,
        op: &mut Op<'_>,
    ) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        if self.failed {
            return self.poll_failed(op);
        }
        match self.poll_op_lower(cx, op) {
            Poll::Ready(Err(ArqError::Closed)) if self.failed => self.poll_failed(op),
            Poll::Ready(Err(ArqError::Closed)) => Poll::Ready(Err(ArqError::Closed)),
            Poll::Ready(Err(error)) => {
                self.fail(error);
                self.poll_failed(op)
            }
            result => result,
        }
    }

    fn fail(&mut self, error: ArqError<Channel::Error>) {
        self.failed = true;
        self.terminal_error = Some(error);
        self.stop_timer();
    }

    fn poll_failed(&mut self, op: &mut Op<'_>) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        if matches!(op, Op::Read { .. }) {
            if let Some(out) = self.service_op(op) {
                return Poll::Ready(Ok(out));
            }
        }
        Poll::Ready(Err(self.terminal_error.take().unwrap_or(ArqError::Closed)))
    }

    fn poll_op_lower(
        &mut self,
        cx: &mut Context<'_>,
        op: &mut Op<'_>,
    ) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        if self.state == State::Done {
            return self.poll_completed(cx, op);
        }
        match op {
            Op::Read { buf: [] } => Poll::Ready(Ok(OpOut::Read(0))),
            Op::Write { buf: [] } => Poll::Ready(Ok(OpOut::Write(0))),
            Op::Read { .. } => {
                let tx = if self.state == State::SendDone {
                    TxPolicy::Acks
                } else {
                    TxPolicy::Full
                };
                self.poll_engine(cx, op, tx)
            }
            Op::Write { .. } => {
                if self.fin_armed {
                    Poll::Ready(Err(ArqError::Closed))
                } else {
                    self.poll_engine(cx, op, TxPolicy::Full)
                }
            }
            Op::Flush => {
                let tx = if self.state == State::SendDone {
                    TxPolicy::Acks
                } else {
                    TxPolicy::Flush
                };
                self.poll_flush_inner(cx, tx)
            }
            Op::Shutdown => self.poll_shutdown(cx),
        }
    }

    fn poll_engine(
        &mut self,
        cx: &mut Context<'_>,
        op: &mut Op<'_>,
        tx: TxPolicy,
    ) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        let batch = self.service_recv(cx)?;
        let mut acted = batch.frames > 0 || batch.more;

        self.poll_timer(cx)?;
        match self.service_tx(cx, tx) {
            // A stalled lower write or flush must not starve the operation:
            // data already received can still be read.
            Poll::Pending => {}
            Poll::Ready(Ok(sent)) => acted |= sent,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
        }

        self.transition();

        let rn = self.rn;
        if let Some(out) = self.service_op(op) {
            if self.rn != rn {
                // Reading released receive-window space; do not defer its ACK
                // until the application's next operation when the lower I/O is ready.
                if let Poll::Ready(Err(error)) = self.service_tx(cx, TxPolicy::Acks) {
                    self.fail(error);
                }
            }
            return Poll::Ready(Ok(out));
        }

        match op {
            Op::Read { .. } => {
                if self.rx_complete() {
                    return Poll::Ready(Ok(OpOut::Read(0)));
                }
                if self.rx_eof && !self.rx_finished {
                    return Poll::Ready(Err(ArqError::Closed));
                }
            }
            Op::Write { .. } => {
                if self.rx_eof && self.w > 0 && self.outgoing.is_none() {
                    return Poll::Ready(Err(ArqError::Closed));
                }
            }
            Op::Flush | Op::Shutdown => {}
        }

        if acted {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }

    fn poll_flush_inner(
        &mut self,
        cx: &mut Context<'_>,
        tx: TxPolicy,
    ) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        for _ in 0..MAX_ROUNDS {
            let batch = self.service_recv(cx)?;
            self.poll_timer(cx)?;
            match self.service_tx(cx, tx) {
                Poll::Pending => {
                    if batch.more {
                        cx.waker().wake_by_ref();
                    }
                    return Poll::Pending;
                }
                Poll::Ready(Ok(true)) => continue,
                Poll::Ready(Ok(false)) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
            if batch.more {
                continue;
            }
            let stalled = self.pending.len > 0 || self.w > 0 || (self.fin_armed && !self.fin_sent);
            if stalled {
                return if self.rx_eof {
                    Poll::Ready(Err(ArqError::Closed))
                } else {
                    Poll::Pending
                };
            }
            return self.flush_channel(cx);
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    fn poll_shutdown(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        self.fin_armed = true;
        for _ in 0..MAX_ROUNDS {
            let batch = self.service_recv(cx)?;
            self.poll_timer(cx)?;
            let tx = if self.tx_done {
                TxPolicy::Acks
            } else {
                TxPolicy::Flush
            };
            match self.service_tx(cx, tx) {
                Poll::Pending => {
                    if batch.more {
                        cx.waker().wake_by_ref();
                    }
                    return Poll::Pending;
                }
                Poll::Ready(Ok(true)) => continue,
                Poll::Ready(Ok(false)) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
            if batch.more {
                continue;
            }
            if self.tx_done {
                return self.flush_channel(cx);
            }
            return if self.rx_eof {
                Poll::Ready(Err(ArqError::Closed))
            } else {
                Poll::Pending
            };
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    /// Application data transfer is complete. Frames are still received so a
    /// duplicate FIN, which means the peer lost our ACK, is acknowledged again.
    fn poll_completed(
        &mut self,
        cx: &mut Context<'_>,
        op: &mut Op<'_>,
    ) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        if matches!(op, Op::Write { .. }) {
            return Poll::Ready(Err(ArqError::Closed));
        }
        for _ in 0..MAX_ROUNDS {
            let batch = self.service_recv(cx)?;
            match self.service_tx(cx, TxPolicy::Acks) {
                Poll::Pending => {
                    if batch.more {
                        cx.waker().wake_by_ref();
                    }
                    return Poll::Pending;
                }
                Poll::Ready(Ok(true)) => continue,
                Poll::Ready(Ok(false)) => {}
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
            if batch.more {
                continue;
            }
            return match op {
                Op::Read { .. } => Poll::Ready(Ok(OpOut::Read(0))),
                _ => self.flush_channel(cx),
            };
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    fn flush_channel(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        self.channel
            .poll_flush(cx)
            .map(|r| r.map(|()| OpOut::Done).map_err(ArqError::Io))
    }

    /// Takes frames from the lower channel until it is idle, at its end, or
    /// the work budget is spent.
    fn service_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Result<RecvBatch, ArqError<Channel::Error>> {
        // Bound corrupt-frame work so noise cannot starve retransmissions.
        let mut budget = RX_BUDGET;
        let mut frames = 0;
        while !self.rx_eof {
            if budget == 0 {
                return Ok(RecvBatch { frames, more: true });
            }
            budget -= 1;
            match self.channel.poll_read(cx, &mut self.rx_pending) {
                Poll::Pending => break,
                Poll::Ready(Ok(0)) => self.rx_eof = true,
                Poll::Ready(Ok(n)) => {
                    let Some(bytes) = self.rx_pending.get(..n) else {
                        continue;
                    };
                    // An invalid frame is discarded whole; nothing is retained.
                    if let Ok(frame) = Frame::from_bytes::<AckCodecType, M, _>(&self.crc, bytes) {
                        self.on_frame(frame);
                        frames += 1;
                    }
                }
                Poll::Ready(Err(e)) => return Err(ArqError::Io(e)),
            }
        }
        Ok(RecvBatch {
            frames,
            more: false,
        })
    }

    /// Sends the next frame, if there is one. Returns whether a frame was
    /// completed; a frame that was started stays in `outgoing` across polls.
    fn service_tx(
        &mut self,
        cx: &mut Context<'_>,
        tx: TxPolicy,
    ) -> Poll<Result<bool, ArqError<Channel::Error>>> {
        let picked = match self.pick_next(tx) {
            Ok(picked) => picked,
            Err(e) => return Poll::Ready(Err(e)),
        };
        if !picked && self.outgoing.is_none() {
            return Poll::Ready(Ok(false));
        }
        self.send_one(cx).map_ok(|()| true)
    }

    fn pick_next(&mut self, tx: TxPolicy) -> Result<bool, ArqError<Channel::Error>> {
        if let Some(out) = self.outgoing.as_ref() {
            if tx == TxPolicy::Flush
                && out.kind == OutgoingKind::Data
                && !out.started
                && self.w > 0
                && self.pending.len == 0
                && !(self.fin_armed && !self.fin_sent)
            {
                let last = (self.sb + self.w as u16 - 1) % MAX_SEQ;
                if let Some(f) = self.sbuf.get(last) {
                    let outgoing_sn = u16::from_le_bytes([out.buf[0], out.buf[1]]) >> 2;
                    if outgoing_sn == last && !f.is_fin() && !f.requests_ack() {
                        let arm_timer = out.arm_timer;
                        let f = f.to_ack_req(&self.crc);
                        self.sbuf.insert(f);
                        self.arm_outgoing(&Frame::DatAckReq(f), arm_timer)?;
                    }
                }
            }
            return Ok(false);
        }
        if let Some(an) = self.ack_pending.take() {
            let ack = AckFrame::new(&self.crc, an)?;
            return self.arm_outgoing(&Frame::Ack(ack), false).map(|_| true);
        }
        if tx == TxPolicy::Acks {
            return Ok(false);
        }
        if self.retx > 0 {
            let Some(f) = self.sbuf.get(self.r) else {
                debug_assert!(false, "retransmit slot empty");
                self.retx = 0;
                return Ok(false);
            };
            self.r = (self.r + 1) % MAX_SEQ;
            self.retx -= 1;
            return self
                .arm_outgoing(&Frame::from_dat(f), self.retx == 0)
                .map(|_| true);
        }
        if self.fin_armed && !self.fin_sent && self.w < N {
            let sn = (self.sb + self.w as u16) % MAX_SEQ;
            let fin = DatFrame::new_fin(&self.crc, sn, &self.pending.buf[..self.pending.len]);
            let stored = self.sbuf.insert(fin);
            debug_assert!(stored);
            self.fin_sent = true;
            self.pending.len = 0;
            self.w += 1;
            return self.arm_outgoing(&Frame::Fin(fin), true).map(|_| true);
        }
        if self.w < N && self.pending.len > 0 {
            let sn = (self.sb + self.w as u16) % MAX_SEQ;
            let payload = &self.pending.buf[..self.pending.len];
            let dat = if tx == TxPolicy::Flush {
                DatFrame::new_dat_ack_req(&self.crc, sn, payload)
            } else {
                DatFrame::new_dat(&self.crc, sn, payload)
            };
            let stored = self.sbuf.insert(dat);
            debug_assert!(stored);
            self.pending.len = 0;
            self.w += 1;
            return self.arm_outgoing(&Frame::from_dat(dat), true).map(|_| true);
        }
        if tx == TxPolicy::Flush
            && self.w > 0
            && self.pending.len == 0
            && !(self.fin_armed && !self.fin_sent)
        {
            let last = (self.sb + self.w as u16 - 1) % MAX_SEQ;
            match self.sbuf.get(last) {
                Some(f) if !f.is_fin() && !f.requests_ack() => {
                    let f = f.to_ack_req(&self.crc);
                    self.sbuf.insert(f);
                    return self.arm_outgoing(&Frame::DatAckReq(f), false).map(|_| true);
                }
                _ => {}
            }
        }
        Ok(false)
    }

    fn arm_timer(&mut self) {
        if !self.timer_running {
            self.restart_timer();
        }
    }

    fn restart_timer(&mut self) {
        self.timer.start(self.rto);
        self.timer_running = true;
    }

    fn stop_timer(&mut self) {
        if self.timer_running {
            self.timer.stop();
            self.timer_running = false;
        }
    }

    fn poll_timer(&mut self, cx: &mut Context<'_>) -> Result<(), ArqError<Channel::Error>> {
        if !self.timer_running || self.w == 0 {
            return Ok(());
        }
        if self.timer.poll_expired(cx).is_pending() {
            return Ok(());
        }
        if self.retries >= self.retry_limit {
            return Err(ArqError::Timeout);
        }
        self.retries += 1;
        self.timer_running = false;
        self.r = self.sb;
        self.retx = self.w;
        self.rto = self.rto.saturating_mul(2).min(self.rto_max);
        Ok(())
    }

    fn arm_outgoing(
        &mut self,
        frame: &Frame,
        arm_timer: bool,
    ) -> Result<(), ArqError<Channel::Error>> {
        let mut buf = [0u8; MAX_FRAME];
        let n = frame
            .to_bytes::<AckCodecType, M>(&mut buf)
            .map_err(ArqError::InvalidAck)?;
        let kind = match frame {
            Frame::Ack(_) => OutgoingKind::Ack,
            _ => OutgoingKind::Data,
        };
        self.outgoing = Some(Outgoing {
            kind,
            buf,
            total: n,
            phase: Phase::Write,
            started: false,
            arm_timer,
        });
        Ok(())
    }

    /// Writes the outgoing frame to the lower transport in one piece, then
    /// flushes it. The frame is delivered only once the flush completes.
    fn send_one(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ArqError<Channel::Error>>> {
        loop {
            let Some(out) = self.outgoing.as_mut() else {
                return Poll::Ready(Ok(()));
            };
            match out.phase {
                Phase::Write => {
                    out.started = true;
                    match self.channel.poll_write(cx, &out.buf[..out.total]) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(n)) if n == out.total => out.phase = Phase::Flush,
                        Poll::Ready(Ok(0)) => {
                            self.fail(ArqError::Closed);
                            return Poll::Ready(Err(ArqError::Closed));
                        }
                        Poll::Ready(Ok(_)) => return Poll::Ready(Err(ArqError::WriteLength)),
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(ArqError::Io(e))),
                    }
                }
                Phase::Flush => match self.channel.poll_flush(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(ArqError::Io(e))),
                    Poll::Ready(Ok(())) => {
                        let arm = out.arm_timer;
                        self.outgoing = None;
                        if arm && self.w > 0 {
                            self.arm_timer();
                        }
                        return Poll::Ready(Ok(()));
                    }
                },
            }
        }
    }

    fn on_frame(&mut self, frame: Frame) {
        match frame {
            Frame::Ack(ack) => self.on_ack(ack.an()),
            Frame::Dat(d) | Frame::DatAckReq(d) | Frame::Fin(d) => self.on_dat(d),
        }
    }

    fn on_ack(&mut self, an: u16) {
        let d = dist(self.sb, an);
        if d == 0 || d as usize > self.w {
            return;
        }
        if self.retx > 0 && dist(self.sb, self.r) < d {
            self.retx -= (dist(self.r, an) as usize).min(self.retx);
            self.r = an;
        }
        for _ in 0..d as usize {
            debug_assert_eq!(self.sbuf.base, self.sb);
            if let Some(f) = self.sbuf.advance() {
                if f.is_fin() {
                    self.fin_acked = true;
                }
            }
            self.sb = (self.sb + 1) % MAX_SEQ;
            self.w -= 1;
        }
        self.rto = self.rto_initial;
        self.retries = 0;
        if self.w == 0 {
            self.stop_timer();
        } else if self.retx == 0 {
            self.restart_timer();
        }
        self.tx_done = self.w == 0 && self.fin_acked;
    }

    fn schedule_ack(&mut self) {
        self.ack_pending = Some(self.rn);
        self.acount = 0;
    }

    fn on_dat(&mut self, f: DatFrame) {
        let is_fin = f.is_fin();
        if self.rx_finished {
            self.schedule_ack();
            return;
        }
        let sn = f.sn();
        let d = dist(self.rn, sn);
        if d == 0 {
            if !self.read_buf.push(f.payload()) {
                self.rbuf.insert(f);
                self.schedule_ack();
                return;
            }
            // Drop any copy of this frame left over from a full read buffer.
            self.rbuf.advance();
            self.rn = (self.rn + 1) % MAX_SEQ;
            self.acount += 1;
            if is_fin {
                self.rx_finished = true;
                self.schedule_ack();
                return;
            }
            let drained = self.drain_recv();
            if self.rx_finished || drained || self.acount >= N || f.requests_ack() {
                self.schedule_ack();
            }
        } else if (d as usize) < N {
            match self.rbuf.get(sn) {
                Some(b) if b.sn() == sn => self.schedule_ack(),
                _ => {
                    self.rbuf.insert(f);
                    if f.requests_ack() {
                        self.schedule_ack();
                    }
                }
            }
        } else if dist(sn, self.rn) as usize <= N {
            self.schedule_ack();
        }
    }

    fn drain_recv(&mut self) -> bool {
        let mut drained = false;
        while !self.rx_finished {
            let Some(next) = self.rbuf.get(self.rn) else {
                break;
            };
            if !self.read_buf.push(next.payload()) {
                break;
            }
            self.rbuf.advance();
            self.rn = (self.rn + 1) % MAX_SEQ;
            self.acount += 1;
            drained = true;
            if next.is_fin() {
                self.rx_finished = true;
            }
        }
        drained
    }

    fn service_op(&mut self, op: &mut Op<'_>) -> Option<OpOut> {
        match op {
            Op::Read { buf } => {
                if buf.is_empty() {
                    return Some(OpOut::Read(0));
                }
                if self.read_buf.is_empty() {
                    return None;
                }
                let n = self.read_buf.read(buf);
                if self.drain_recv() {
                    self.schedule_ack();
                }
                Some(OpOut::Read(n))
            }
            Op::Write { buf } => {
                if buf.is_empty() {
                    return Some(OpOut::Write(0));
                }
                let space = MAX_PAYLOAD - self.pending.len;
                if space == 0 {
                    return None;
                }
                let n = space.min(buf.len());
                self.pending.buf[self.pending.len..self.pending.len + n].copy_from_slice(&buf[..n]);
                self.pending.len += n;
                Some(OpOut::Write(n))
            }
            Op::Flush | Op::Shutdown => None,
        }
    }

    /// An ACK is queued or still being written or flushed.
    fn ack_work_pending(&self) -> bool {
        self.ack_pending.is_some()
            || matches!(&self.outgoing, Some(out) if out.kind == OutgoingKind::Ack)
    }

    /// The peer's FIN was received and acknowledged, and its data is drained.
    fn rx_complete(&self) -> bool {
        self.rx_finished && self.read_buf.is_empty() && !self.ack_work_pending()
    }

    fn transition(&mut self) {
        self.state = match (self.tx_done, self.rx_complete()) {
            (true, true) if self.outgoing.is_none() => State::Done,
            (true, _) => State::SendDone,
            (false, true) => State::RecvDone,
            (false, false) => State::Active,
        };
    }
}

impl<const N: usize, const M: usize, Channel, Crc, AckCodecType, Tmr> Transport
    for Arq<N, M, Channel, Crc, AckCodecType, Tmr>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: Transport,
    Tmr: Timer,
{
    type Error = ArqError<Channel::Error>;

    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        self.poll_op(cx, &mut Op::Read { buf })
            .map_ok(|out| match out {
                OpOut::Read(n) => n,
                _ => unreachable!(),
            })
    }

    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Self::Error>> {
        self.poll_op(cx, &mut Op::Write { buf })
            .map_ok(|out| match out {
                OpOut::Write(n) => n,
                _ => unreachable!(),
            })
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_op(cx, &mut Op::Flush).map_ok(|out| match out {
            OpOut::Done => (),
            _ => unreachable!(),
        })
    }
}

#[allow(private_bounds)]
impl<const N: usize, const M: usize, Channel, Crc, AckCodecType, Tmr>
    Arq<N, M, Channel, Crc, AckCodecType, Tmr>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: Transport,
    Tmr: Timer,
{
    /// Sends the final `FIN` and completes once both directions are finished.
    pub fn poll_close(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ArqError<Channel::Error>>> {
        self.poll_op(cx, &mut Op::Shutdown).map_ok(|out| match out {
            OpOut::Done => (),
            _ => unreachable!(),
        })
    }
}

/// Builder for [`Arq`] instances.
///
/// Carries the retransmission window `N`, the frame CRC algorithm, and the ACK
/// codec type. [`ArqLayer::new`] uses the defaults: CRC-16/X-25 and the
/// built-in error-correcting ACK codec.
pub struct ArqLayer<const N: usize, Crc = ::crc::Crc<u16>, AckCodecType = BchAckCodec>
where
    Crc: Clone,
{
    crc: Crc,
    rto_initial: Duration,
    rto_max: Duration,
    retry_limit: usize,
    p_ack_codec: PhantomData<fn() -> AckCodecType>,
}

impl<const N: usize> ArqLayer<N, ::crc::Crc<u16>, BchAckCodec> {
    /// Creates a layer with a retransmission window of `N` and the default CRC
    /// and ACK codec.
    ///
    /// Panics if `N` is not even or not in `2..=32`.
    pub fn new() -> Self {
        assert!((2..=32).contains(&N), "N must be between 2 and 32");
        assert!(N.is_multiple_of(2), "N must be even");
        ArqLayer {
            crc: ::crc::Crc::<u16>::new(&::crc::CRC_16_IBM_SDLC),
            rto_initial: DEFAULT_RTO_INITIAL,
            rto_max: DEFAULT_RTO_MAX,
            retry_limit: DEFAULT_RETRY_LIMIT,
            p_ack_codec: PhantomData,
        }
    }
}

/// Equivalent to [`ArqLayer::new`].
impl<const N: usize> Default for ArqLayer<N, ::crc::Crc<u16>, BchAckCodec> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize, Crc> ArqLayer<N, Crc, BchAckCodec>
where
    Crc: Clone,
{
    /// Replaces the ACK codec type with `AckCodecType`.
    ///
    /// `M`, the codeword length, is chosen at [`ArqLayer::build_with_codec`]
    /// and must match the codec.
    pub fn with_ack_codec_type<AckCodecType>(self) -> ArqLayer<N, Crc, AckCodecType> {
        ArqLayer {
            crc: self.crc,
            rto_initial: self.rto_initial,
            rto_max: self.rto_max,
            retry_limit: self.retry_limit,
            p_ack_codec: PhantomData,
        }
    }
}

impl<const N: usize, Crc, AckCodecType> ArqLayer<N, Crc, AckCodecType>
where
    Crc: Clone,
{
    /// Replaces the frame CRC algorithm with `crc`.
    pub fn with_crc<NewCrc>(self, crc: NewCrc) -> ArqLayer<N, NewCrc, AckCodecType>
    where
        NewCrc: Crc16 + Clone,
    {
        ArqLayer {
            crc,
            rto_initial: self.rto_initial,
            rto_max: self.rto_max,
            retry_limit: self.retry_limit,
            p_ack_codec: PhantomData,
        }
    }

    /// Sets the number of retransmission rounds allowed without ACK progress.
    ///
    /// Defaults to 16. After these rounds, the next retransmission timeout
    /// fails the link with [`ArqError::Timeout`]. An ACK advancing the send
    /// window resets the count; duplicate ACKs do not. A limit of zero allows
    /// the initial transmission only, failing on its first timeout.
    ///
    /// This bounds waiting for missing ACKs, not stalls in the lower I/O layer.
    pub fn with_retry_limit(mut self, limit: usize) -> Self {
        self.retry_limit = limit;
        self
    }

    /// Sets the retransmission timeout bounds.
    ///
    /// Unacknowledged frames are retransmitted `initial` after they are sent.
    /// Each further expiry without an acknowledgement doubles the timeout, up
    /// to `max`, and an acknowledgement of new data resets it to `initial`.
    /// Defaults to 250 ms and 4 s.
    ///
    /// Choose `initial` above the link's round-trip time for a full window of
    /// frames; a shorter timeout causes needless retransmissions.
    ///
    /// Panics if `initial` is zero or `max < initial`.
    pub fn with_retransmit_timeout(mut self, initial: Duration, max: Duration) -> Self {
        assert!(
            !initial.is_zero(),
            "initial retransmit timeout must be non-zero"
        );
        assert!(max >= initial, "max retransmit timeout must be >= initial");
        self.rto_initial = initial;
        self.rto_max = max;
        self
    }
}

impl<const N: usize, Crc> ArqLayer<N, Crc, BchAckCodec>
where
    Crc: Crc16 + Clone,
{
    /// Builds an [`Arq`] over `channel`, using `timer` for retransmissions.
    ///
    /// Each instance needs its own timer.
    pub fn build<Channel, Tmr>(
        &self,
        channel: Channel,
        timer: Tmr,
    ) -> Arq<N, 16, Channel, Crc, BchAckCodec, Tmr>
    where
        Channel: Transport,
        Tmr: Timer,
    {
        self.build_with_codec::<16, Channel, Tmr>(channel, timer)
    }
}

#[allow(private_bounds)]
impl<const N: usize, Crc, AckCodecType> ArqLayer<N, Crc, AckCodecType>
where
    Crc: Crc16 + Clone,
{
    /// Builds an [`Arq`] over `channel`, using `timer` for retransmissions and
    /// an ACK codec of codeword length `M`.
    ///
    /// `M` must be the codeword length of `AckCodecType`. Each instance needs
    /// its own timer.
    pub fn build_with_codec<const M: usize, Channel, Tmr>(
        &self,
        channel: Channel,
        timer: Tmr,
    ) -> Arq<N, M, Channel, Crc, AckCodecType, Tmr>
    where
        AckCodecType: AckCodec<M>,
        Channel: Transport,
        Tmr: Timer,
    {
        let mut arq = Arq::new(channel, self.crc.clone(), timer);
        arq.rto = self.rto_initial;
        arq.rto_initial = self.rto_initial;
        arq.rto_max = self.rto_max;
        arq.retry_limit = self.retry_limit;
        arq
    }
}
