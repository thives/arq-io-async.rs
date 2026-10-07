[![Crates.io](https://img.shields.io/crates/v/arq-io-async)](https://crates.io/crates/arq-io-async)
[![docs.rs](https://img.shields.io/docsrs/arq-io-async)](https://docs.rs/arq-io-async)
[![ci](https://github.com/thives/arq-io-async.rs/actions/workflows/ci.yml/badge.svg)](https://github.com/thives/arq-io-async.rs/actions/workflows/ci.yml)

# arq-io-async

> [!WARNING]
> This crate is in early development. The API is not yet stable and may change.

> [!CAUTION]
> This crate is not yet production-ready. It has not been widely tested and may contain bugs.

Implementation of ARQ (Automatic Repeat reQuest) in Rust, for an unreliable point-to-point link.

`arq-io-async` turns an unreliable link into a reliable, in-order, bidirectional byte stream. It retransmits frames until they are acknowledged and buffers out-of-order frames until their predecessors arrive, so the reader sees exactly the bytes the peer wrote, in order.

The layer is a single polled state machine with no internal tasks, and the crate is `no_std` and runtime independent. It sits between your channel (e.g. a radio link) and the layers above it. Both sides use the poll-based `Transport` trait:

```rust
pub trait Transport {
    type Error;

    fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<Result<usize, Self::Error>>;
    fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, Self::Error>>;
    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>>;
}
```

The lower channel implements `Transport` and must be **framed**; `Arq` implements `Transport` for the layer above. The crate provides no runtime adaptors (for example for `tokio::io` or `embedded-io-async`); write a small one for your runtime.

One instance is one link: the peer must run its own instance on the other end of the channel.

## Features

| Feature | Description |
| --- | --- |
| `serde` (default) | `Serialize`/`Deserialize` for the error types. |
| `defmt` | `defmt::Format` for the error types. |

## Lower transport contract

- `poll_read` returns exactly one frame per call, into a buffer of `MAX_FRAME` (256) bytes, and returns `0` once the read side has ended.
- `poll_write` is offered one complete frame and must accept all of it. A different length fails the link with `ArqError::WriteLength`, and `0` with `ArqError::Closed`.
- `poll_flush` is called after every accepted frame. A frame counts as sent only once its flush completes.
- An operation that returns `Pending` is resumed by polling again; a started frame is never replaced.

## Usage

```rust
use arq_io_async::{ArqLayer, Transport};

let layer = ArqLayer::<8>::new();
let mut arq = layer.build(lower, timer); // `lower: impl Transport`, `timer: impl Timer`

// Poll `arq` through `Transport` from your runtime adaptor.
// `arq.poll_close(cx)` shuts the link down.
```

`build` uses the default ACK codec (16-byte codewords); use `build_with_codec` for a custom `AckCodec`. Each instance needs its own `Timer`.

## Configuration

- `N` (const generic on `ArqLayer`): retransmission window, in frames. Must be even and in `2..=32`.
- `M` (on `Arq` and `build_with_codec`): ACK codeword length, in bytes. It is `16` for the default codec.
- The CRC defaults to CRC-16/X-25; replace it with `ArqLayer::with_crc`.
- The default ACK codec is error-correcting; replace it with your own `AckCodec` implementation via `ArqLayer::with_ack_codec_type`.
- The retransmission timeout defaults to 250 ms, doubling up to 4 s; change it with `ArqLayer::with_retransmit_timeout`.
- Retransmission rounds without ACK progress default to 16; change it with `ArqLayer::with_retry_limit`.

## Retransmission timer

Unacknowledged frames are retransmitted only when the retransmission timeout expires, not every time the layer is polled. Every expiry without an acknowledgement doubles the timeout, up to the maximum. An acknowledgement of new data resets it.

Time comes from the `Timer` trait, so the layer does not depend on any runtime. Implement `Timer` with your platform's timer (see the `Timer` docs for an `embassy-time` example). `start(timeout)` arms a one-shot deadline, `stop()` disarms it, and `poll_expired(cx)` returns `Ready` once the deadline has passed or registers the waker otherwise.

## Flushing

`poll_flush` completes once the peer has acknowledged everything written so far. The last frame of the flushed burst asks the peer to acknowledge it immediately, so request/response traffic does not wait for further writes. Ordinary writes are still acknowledged in batches. `poll_flush` also waits until any ACK owed to the peer has been written and flushed on the lower channel. `poll_flush` does not close the link and can be called repeatedly.

## End of stream

`poll_close` flushes pending data in a final `FIN` frame and signals end-of-stream to the peer. Afterwards:

- further writes fail with `ArqError::Closed`
- the peer's read returns `0` bytes once its stream is drained

Completion means that application data transfer is finished, not that the layer can no longer receive. If the ACK for a `FIN` is lost, the peer retransmits the `FIN` and the completed layer acknowledges the duplicate whenever it is polled. It never sends new data or reopens writing, and no linger timer is used. A lost final ACK is therefore recovered only while the instance keeps being polled; after polling stops or the instance is dropped it cannot be.

## Invalid frames

The lower transport is framed, so frame boundaries are authoritative. An invalid frame (unknown type, impossible length, bad CRC, or an undecodable ACK) is discarded whole and never terminal; it is recovered by retransmission. Discarding consumes no part of the next frame, and a flood of invalid frames is processed in bounded batches that yield and wake the task.

The default ACK codec corrects up to 11 bit errors in each half of its codeword and rejects an ACK that needed more. Outside that radius a decode can still produce another valid codeword, so the CRC is additional validation, not an absolute guarantee.

## Lower end-of-stream

If the lower read side ends, frames already received are still delivered. A read returns the accepted data first, then `0` bytes if a valid `FIN` was received, otherwise `ArqError::Closed`. Writing is not disabled, but `poll_flush` and `poll_close` fail with `Closed` once they would have to wait for an ACK that can no longer arrive.

## Errors

The layer reports `ArqError<ChannelError>`:

| Variant | Meaning |
| --- | --- |
| `Io(e)` | I/O error from the lower transport |
| `Framing(e)` | a frame failed validation; the layer discards invalid frames and does not currently report this |
| `InvalidAck(e)` | an outgoing ACK frame could not be encoded |
| `Timeout` | the retry limit was exhausted without ACK progress |
| `Closed` | the link is closed |
| `WriteLength` | the lower transport accepted a different number of bytes than the whole frame |

Lower I/O, write-length, ACK-encoding and retry-exhaustion errors are terminal. Reads still deliver buffered in-order data before reporting the error; the original error is reported once, and later operations fail with `Closed`.

## Testing

```sh
cargo test --lib
cargo test --lib --no-default-features
cargo test --lib --all-features
cargo test --doc
```

CI runs the tests with default features, `--all-features` and `--no-default-features`, and checks the build on `thumbv7em-none-eabihf` and `armv7-unknown-linux-gnueabihf` without default features.
