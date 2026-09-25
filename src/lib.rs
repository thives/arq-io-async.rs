#![cfg_attr(not(feature = "std"), no_std)]

mod ack_codec;
mod bch;
mod crc;
#[cfg(feature = "embedded-io")]
pub mod embedded_io;
mod error;
mod frame;
#[cfg(feature = "tokio")]
mod tokio;
mod transport;

#[cfg(test)]
mod tests;

use ::futures::task::AtomicWaker;
use core::marker::PhantomData;
use core::task::{Context, Poll};

pub use crate::ack_codec::AckCodec;
pub use crate::crc::Crc16;
pub use crate::error::{AckError, ArqError, FrameError};
pub use crate::frame::{AckFrame, MAX_SEQ};

use crate::ack_codec::CodeRsAckCodec;
use crate::frame::{DatFrame, Frame, MAX_PAYLOAD};
use crate::transport::FrameIo;

pub(crate) const MAX_FRAME: usize = 256;

pub const fn r<const N: usize>() -> usize {
    2 * N * MAX_PAYLOAD
}

fn dist(a: u16, b: u16) -> u16 {
    b.wrapping_sub(a) & (MAX_SEQ - 1)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Active,
    SendDone,
    RecvDone,
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxPolicy {
    Acks,
    Flush,
    Full,
}

pub(crate) enum Op<'a> {
    Read {
        buf: &'a mut [u8],
    },
    Write {
        buf: &'a [u8],
    },
    Flush,
    #[cfg_attr(not(feature = "tokio"), allow(dead_code))]
    Shutdown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpOut {
    Read(usize),
    Write(usize),
    Done,
}

#[derive(Debug, Clone, Copy)]
struct Outgoing {
    buf: [u8; MAX_FRAME],
    off: usize,
    total: usize,
}

#[derive(Debug, Clone, Copy)]
struct Pending {
    buf: [u8; MAX_PAYLOAD],
    len: usize,
}

#[derive(Debug, Clone, Copy)]
struct Ring<const N: usize> {
    slots: [Option<DatFrame>; N],
}

impl<const N: usize> Ring<N> {
    fn new() -> Self {
        Self { slots: [None; N] }
    }

    fn get(&self, sn: u16) -> Option<DatFrame> {
        self.slots[(sn as usize) % N]
    }

    fn set(&mut self, sn: u16, f: DatFrame) {
        self.slots[(sn as usize) % N] = Some(f);
    }

    fn take(&mut self, sn: u16) -> Option<DatFrame> {
        self.slots[(sn as usize) % N].take()
    }
}

#[derive(Debug)]
pub struct Arq<const N: usize, const M: usize, const R: usize, Channel, Crc, AckCodecType>
where
    AckCodecType: AckCodec<M>,
{
    channel: Channel,
    crc: Crc,
    state: State,
    sb: u16,
    w: usize,
    g: usize,
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
    rx_len: usize,
    read_buf: [u8; R],
    read_head: usize,
    read_tail: usize,
    read_waker: AtomicWaker,
    write_waker: AtomicWaker,
    spin_waker: AtomicWaker,
    p_ack_codec: PhantomData<fn() -> AckCodecType>,
}

impl<const N: usize, const M: usize, const R: usize, Channel, Crc, AckCodecType>
    Arq<N, M, R, Channel, Crc, AckCodecType>
where
    AckCodecType: AckCodec<M>,
{
    fn new(channel: Channel, crc: Crc) -> Self {
        Self {
            channel,
            crc,
            state: State::Active,
            sb: 0,
            w: 0,
            g: 0,
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
            rx_len: 0,
            read_buf: [0; R],
            read_head: 0,
            read_tail: 0,
            read_waker: AtomicWaker::new(),
            write_waker: AtomicWaker::new(),
            spin_waker: AtomicWaker::new(),
            p_ack_codec: PhantomData,
        }
    }
}

#[allow(private_bounds)]
impl<const N: usize, const M: usize, const R: usize, Channel, Crc, AckCodecType>
    Arq<N, M, R, Channel, Crc, AckCodecType>
where
    Crc: Crc16,
    AckCodecType: AckCodec<M>,
    Channel: FrameIo,
{
    pub(crate) fn poll_op(
        &mut self,
        cx: &mut Context<'_>,
        op: &mut Op<'_>,
    ) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        if self.state == State::Done {
            return self.poll_closed(cx, op);
        }
        match op {
            Op::Read { buf: [] } => Poll::Ready(Ok(OpOut::Read(0))),
            Op::Write { buf: [] } => Poll::Ready(Ok(OpOut::Write(0))),
            Op::Read { .. } => {
                if self.state == State::RecvDone {
                    Poll::Ready(Ok(OpOut::Read(0)))
                } else {
                    let tx = if self.state == State::SendDone {
                        TxPolicy::Acks
                    } else {
                        TxPolicy::Full
                    };
                    self.poll_engine(cx, op, tx)
                }
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
                self.poll_flush(cx, tx)
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
        let mut acted = false;
        loop {
            match self.poll_recv_frame(cx) {
                Poll::Pending => break,
                Poll::Ready(Ok(frame)) => {
                    self.on_frame(frame);
                    acted = true;
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
        }

        match self.pick_next(tx) {
            Err(e) => return Poll::Ready(Err(e)),
            Ok(false) if self.outgoing.is_some() => match self.send_one(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => acted = true,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            },
            Ok(false) => {}
            Ok(true) => match self.send_one(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => acted = true,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            },
        }

        self.transition();

        if let Some(out) = self.service_op(op) {
            return Poll::Ready(Ok(out));
        }

        if matches!(op, Op::Read { .. })
            && (self.state == State::RecvDone || self.state == State::Done)
        {
            return Poll::Ready(Ok(OpOut::Read(0)));
        }

        if acted {
            self.spin_waker.register(cx.waker());
            self.spin_waker.wake();
        }
        Poll::Pending
    }

    fn poll_flush(
        &mut self,
        cx: &mut Context<'_>,
        tx: TxPolicy,
    ) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        loop {
            match self.poll_recv_frame(cx) {
                Poll::Pending => {}
                Poll::Ready(Ok(frame)) => self.on_frame(frame),
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
            match self.pick_next(tx) {
                Err(e) => return Poll::Ready(Err(e)),
                Ok(true) => match self.send_one(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                },
                Ok(false) if self.outgoing.is_some() => match self.send_one(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                },
                Ok(false) => {
                    let stalled =
                        self.pending.len > 0 || self.w > 0 || (self.fin_armed && !self.fin_sent);
                    if stalled {
                        return Poll::Pending;
                    }
                    return match self.channel.poll_flush(cx) {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready(Ok(())) => Poll::Ready(Ok(OpOut::Done)),
                        Poll::Ready(Err(e)) => Poll::Ready(Err(ArqError::Io(e))),
                    };
                }
            }
        }
    }

    fn poll_shutdown(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        self.fin_armed = true;
        loop {
            if self.tx_done {
                return self.flush_channel(cx);
            }
            match self.poll_recv_frame(cx) {
                Poll::Pending => {}
                Poll::Ready(Ok(frame)) => self.on_frame(frame),
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
            if self.tx_done {
                return self.flush_channel(cx);
            }
            match self.pick_next(TxPolicy::Flush) {
                Err(e) => return Poll::Ready(Err(e)),
                Ok(true) => match self.send_one(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                },
                Ok(false) if self.outgoing.is_some() => match self.send_one(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                },
                Ok(false) => return Poll::Pending,
            }
        }
    }

    fn poll_closed(
        &mut self,
        cx: &mut Context<'_>,
        op: &mut Op<'_>,
    ) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        match op {
            Op::Read { .. } => Poll::Ready(Ok(OpOut::Read(0))),
            Op::Write { .. } => Poll::Ready(Err(ArqError::Closed)),
            Op::Flush | Op::Shutdown => self.flush_channel(cx),
        }
    }

    fn flush_channel(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<OpOut, ArqError<Channel::Error>>> {
        match self.channel.poll_flush(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(Ok(OpOut::Done)),
            Poll::Ready(Err(e)) => Poll::Ready(Err(ArqError::Io(e))),
        }
    }

    fn poll_recv_frame(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Frame, ArqError<Channel::Error>>> {
        loop {
            match Frame::wire_len::<AckCodecType, M, _>(&self.crc, &self.rx_pending[..self.rx_len])
            {
                Err(FrameError::TooShort(_)) => {}
                Err(e) => return Poll::Ready(Err(ArqError::Framing(e))),
                Ok(len) if self.rx_len >= len => {
                    let frame =
                        Frame::from_bytes::<AckCodecType, M, _>(&self.crc, &self.rx_pending[..len])
                            .map_err(ArqError::Framing)?;
                    self.rx_pending.copy_within(len..self.rx_len, 0);
                    self.rx_len -= len;
                    return Poll::Ready(Ok(frame));
                }
                Ok(_) if self.rx_len >= MAX_FRAME => {
                    return Poll::Ready(Err(ArqError::Framing(FrameError::TooLong(MAX_FRAME))));
                }
                Ok(_) => {}
            }
            match self
                .channel
                .poll_recv(cx, &mut self.rx_pending[self.rx_len..])
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(0)) => return Poll::Ready(Err(ArqError::Closed)),
                Poll::Ready(Ok(n)) => self.rx_len += n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(ArqError::Io(e))),
            }
        }
    }

    fn pick_next(&mut self, tx: TxPolicy) -> Result<bool, ArqError<Channel::Error>> {
        if self.outgoing.is_some() {
            return Ok(false);
        }
        if let Some(an) = self.ack_pending.take() {
            let ack = AckFrame::new(&self.crc, an)?;
            return self.arm_outgoing(&Frame::Ack(ack)).map(|_| true);
        }
        if tx != TxPolicy::Acks && self.fin_armed && !self.fin_sent && self.w < N {
            let sn = (self.sb + self.w as u16) % MAX_SEQ;
            let fin = DatFrame::new_fin(&self.crc, sn, &self.pending.buf[..self.pending.len]);
            self.sbuf.set(sn, fin);
            self.fin_sent = true;
            self.pending.len = 0;
            self.w += 1;
            self.g += 1;
            return self.arm_outgoing(&Frame::Fin(fin)).map(|_| true);
        }
        if tx != TxPolicy::Acks && self.w < N && self.pending.len > 0 {
            let sn = (self.sb + self.w as u16) % MAX_SEQ;
            let dat = DatFrame::new_dat(&self.crc, sn, &self.pending.buf[..self.pending.len]);
            self.sbuf.set(sn, dat);
            self.pending.len = 0;
            self.w += 1;
            self.g += 1;
            return self.arm_outgoing(&Frame::Dat(dat)).map(|_| true);
        }
        if tx == TxPolicy::Full && self.w > 0 && self.g >= self.w {
            if dist(self.sb, self.r) >= self.w as u16 {
                self.r = self.sb;
            }
            let Some(f) = self.sbuf.get(self.r) else {
                debug_assert!(false, "retransmit slot empty");
                return Ok(false);
            };
            self.r = (self.r + 1) % MAX_SEQ;
            self.g += 1;
            let frame = if f.is_fin() {
                Frame::Fin(f)
            } else {
                Frame::Dat(f)
            };
            return self.arm_outgoing(&frame).map(|_| true);
        }
        Ok(false)
    }

    fn arm_outgoing(&mut self, frame: &Frame) -> Result<(), ArqError<Channel::Error>> {
        let mut buf = [0u8; MAX_FRAME];
        let n = frame
            .to_bytes::<AckCodecType, M>(&mut buf)
            .map_err(ArqError::InvalidAck)?;
        self.outgoing = Some(Outgoing {
            buf,
            off: 0,
            total: n,
        });
        Ok(())
    }

    fn send_one(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), ArqError<Channel::Error>>> {
        loop {
            let (off, total) = match &self.outgoing {
                None => return Poll::Ready(Ok(())),
                Some(o) => (o.off, o.total),
            };
            if off == total {
                self.outgoing = None;
                return Poll::Ready(Ok(()));
            }
            let res = {
                let ch = &mut self.channel;
                let out = self.outgoing.as_mut().unwrap();
                ch.poll_send(cx, &out.buf[off..total])
            };
            match res {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(0)) => return Poll::Ready(Err(ArqError::Closed)),
                Poll::Ready(Ok(n)) => self.outgoing.as_mut().unwrap().off += n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(ArqError::Io(e))),
            }
        }
    }

    fn on_frame(&mut self, frame: Frame) {
        match frame {
            Frame::Ack(ack) => self.on_ack(ack.an()),
            Frame::Dat(d) => self.on_dat(d),
            Frame::Fin(d) => self.on_dat(d),
        }
    }

    fn on_ack(&mut self, an: u16) {
        let d = dist(self.sb, an);
        if d == 0 || d as usize > self.w {
            return;
        }
        for _ in 0..d as usize {
            if let Some(f) = self.sbuf.take(self.sb) {
                if f.is_fin() {
                    self.fin_acked = true;
                }
            }
            self.sb = (self.sb + 1) % MAX_SEQ;
            self.w -= 1;
        }
        self.g = 0;
        self.r = self.sb;
        self.tx_done = self.w == 0 && self.fin_acked;
        if self.w < N {
            self.write_waker.wake();
        }
    }

    fn on_dat(&mut self, f: DatFrame) {
        let is_fin = f.is_fin();
        if self.rx_finished {
            self.ack_pending = Some(self.rn);
            self.acount = 0;
            return;
        }
        let sn = f.sn();
        let d = dist(self.rn, sn);
        if d == 0 {
            if !self.push_read(f.payload()) {
                return;
            }
            self.rn = (self.rn + 1) % MAX_SEQ;
            self.acount += 1;
            self.read_waker.wake();
            if is_fin {
                self.rx_finished = true;
                self.ack_pending = Some(self.rn);
                self.acount = 0;
                return;
            }
            let mut drained = false;
            while let Some(next) = self.rbuf.get(self.rn) {
                if next.sn() != self.rn {
                    break;
                }
                if !self.push_read(next.payload()) {
                    break;
                }
                self.rbuf.take(self.rn);
                self.rn = (self.rn + 1) % MAX_SEQ;
                self.acount += 1;
                drained = true;
                self.read_waker.wake();
                if next.is_fin() {
                    self.rx_finished = true;
                    break;
                }
            }
            if self.rx_finished || drained || self.acount >= N {
                self.ack_pending = Some(self.rn);
                self.acount = 0;
            }
        } else if (d as usize) < N && self.rbuf.get(sn).is_none() {
            self.rbuf.set(sn, f);
        }
    }

    fn push_read(&mut self, data: &[u8]) -> bool {
        if self.read_head == self.read_tail {
            self.read_head = 0;
            self.read_tail = 0;
        } else if self.read_head > 0 {
            self.read_buf.copy_within(self.read_head..self.read_tail, 0);
            self.read_tail -= self.read_head;
            self.read_head = 0;
        }
        if self.read_tail + data.len() > self.read_buf.len() {
            return false;
        }
        self.read_buf[self.read_tail..self.read_tail + data.len()].copy_from_slice(data);
        self.read_tail += data.len();
        true
    }

    fn service_op(&mut self, op: &mut Op<'_>) -> Option<OpOut> {
        match op {
            Op::Read { buf } => {
                if buf.is_empty() {
                    return Some(OpOut::Read(0));
                }
                let avail = self.read_tail - self.read_head;
                if avail == 0 {
                    return None;
                }
                let n = avail.min(buf.len());
                buf[..n].copy_from_slice(&self.read_buf[self.read_head..self.read_head + n]);
                self.read_head += n;
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

    fn transition(&mut self) {
        let rx_drained = self.rx_finished && self.read_head == self.read_tail;
        self.state = match (self.tx_done, rx_drained) {
            (true, true) => State::Done,
            (true, false) => State::SendDone,
            (false, true) => State::RecvDone,
            (false, false) => State::Active,
        };
    }
}

pub struct ArqLayer<const N: usize, Crc, AckCodecType>
where
    Crc: Clone,
{
    crc: Crc,
    p_ack_codec: PhantomData<fn() -> AckCodecType>,
}

impl<const N: usize> ArqLayer<N, ::crc::Crc<u16>, CodeRsAckCodec> {
    pub fn new() -> Self {
        assert!((2..=32).contains(&N), "N must be between 2 and 32");
        assert!(N.is_multiple_of(2), "N must be even");
        ArqLayer {
            crc: ::crc::Crc::<u16>::new(&::crc::CRC_16_IBM_SDLC),
            p_ack_codec: PhantomData,
        }
    }
}

impl<const N: usize> Default for ArqLayer<N, ::crc::Crc<u16>, CodeRsAckCodec> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize, Crc> ArqLayer<N, Crc, CodeRsAckCodec>
where
    Crc: Clone,
{
    pub fn with_ack_codec_type<AckCodecType>(self) -> ArqLayer<N, Crc, AckCodecType> {
        ArqLayer {
            crc: self.crc,
            p_ack_codec: PhantomData,
        }
    }
}

impl<const N: usize, Crc, AckCodecType> ArqLayer<N, Crc, AckCodecType>
where
    Crc: Clone,
{
    pub fn with_crc<NewCrc>(self, crc: NewCrc) -> ArqLayer<N, NewCrc, AckCodecType>
    where
        NewCrc: Crc16 + Clone,
    {
        ArqLayer {
            crc,
            p_ack_codec: PhantomData,
        }
    }
}

impl<const N: usize, Crc, AckCodecType> ArqLayer<N, Crc, AckCodecType>
where
    Crc: Crc16 + Clone,
{
    pub fn build<const M: usize, const R: usize, Channel>(
        &self,
        channel: Channel,
    ) -> Arq<N, M, R, Channel, Crc, AckCodecType>
    where
        AckCodecType: AckCodec<M>,
    {
        assert!(R >= r::<N>());
        Arq::new(channel, self.crc.clone())
    }
}
