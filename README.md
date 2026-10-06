[![Crates.io](https://img.shields.io/crates/v/arq-io-async)](https://crates.io/crates/arq-io-async)
[![docs.rs](https://img.shields.io/docsrs/arq-io-async)](https://docs.rs/arq-io-async)
[![ci](https://github.com/thives/arq-io-async.rs/actions/workflows/ci.yml/badge.svg)](https://github.com/thives/arq-io-async.rs/actions/workflows/ci.yml)

# arq-io-async

> [!WARNING]
> This crate is in early development. The API is not yet stable and may change.

> [!CAUTION]
> This crate is not yet production-ready. It has not been widely tested and may contain bugs.

Asynchronous implementation of ARQ (Automatic Repeat reQuest) in Rust.

`arq-io-async` turns an unreliable point-to-point link into a reliable, in-order, bidirectional byte stream. It retransmits frames until they are acknowledged and buffers out-of-order frames until their predecessors arrive, so the reader sees exactly the bytes the peer wrote, in order.

The layer is a single polled state machine with no internal tasks. It sits between your channel (e.g. a radio link) and the layers above it, and is driven through one of two async interfaces:

- `tokio::io::AsyncRead` / `AsyncWrite` (feature `tokio`, default)
- `embedded_io_async::Read` / `Write` (feature `embedded-io`)

One instance is one link: the peer must run its own instance on the other end of the channel.

## Features

| Feature | Description |
| --- | --- |
| `tokio` (default) | `tokio::io::AsyncRead`/`AsyncWrite` interface. Any `AsyncRead + AsyncWrite + Unpin` stream is a valid channel. |
| `embedded-io` | `embedded_io_async::Read`/`Write` interface. Implement `embedded_io::PollTransport` and wrap it in `embedded_io::EiaPoll`. Works without `std`. |
| `serde` (default) | `Serialize`/`Deserialize` for the error types. |
| `std` | Enables `StdTimer` and `ArqLayer::build`, and `std` in `embedded-io-async`. Enabled by `tokio`. |
| `defmt` | `defmt::Format` for the error types. |

## Usage

### tokio

```rust
use arq_io_async::{ArqLayer, r};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

let (mut rx, _tx) = tokio::io::duplex(1024); // your channel
let layer = ArqLayer::<8, _, _>::new();
let mut arq = layer.build::<16, { r::<8>() }, _>(rx);

arq.write_all(b"hello").await?;
arq.flush().await?;

let mut buf = [0u8; 5];
let n = arq.read_exact(&mut buf).await?;
assert_eq!(&buf[..n], b"hello");
```

`build` uses `StdTimer` for retransmissions and requires the `std` feature, which `tokio` enables.

### embedded-io

```rust
use arq_io_async::embedded_io::{EiaPoll, PollTransport};
use arq_io_async::{ArqLayer, r};

let link = /* ... implements embedded_io::PollTransport ... */;
let timer = /* ... implements arq_io_async::Timer ... */;
let layer = ArqLayer::<8, _, _>::new();
let mut arq = layer.build_with_timer::<16, { r::<8>() }, _, _>(EiaPoll(link), timer);

let mut buf = [0u8; 16];
let n = arq.read(&mut buf).await?;
```

Without `std`, provide a `Timer` for your platform (see [Retransmission timer](#retransmission-timer)).

`PollTransport` has `poll_read`, `poll_write` and `poll_flush`. An operation that returns `Pending` stays in progress inside the transport, so drivers that start a transfer and finish it over several polls, such as DMA UART drivers, are supported. `EiaPoll` calls these methods directly and never recreates an operation. Use the returned lengths for partial reads and writes.

## Configuration

- `N` (const generic on `ArqLayer`): retransmission window, in frames. Must be even and in `2..=32`.
- `M` (chosen at `ArqLayer::build`/`build_with_timer`): ACK codeword length, in bytes. Use `16` for the default codec.
- `R` (chosen at `ArqLayer::build`/`build_with_timer`): read buffer size, in bytes. Must be at least `r::<N>()`.
- The CRC defaults to CRC-16/X-25; replace it with `ArqLayer::with_crc`.
- The default ACK codec is error-correcting; replace it with your own `AckCodec` implementation via `ArqLayer::with_ack_codec_type`.
- The retransmission timeout defaults to 250 ms, doubling up to 4 s; change it with `ArqLayer::with_retransmit_timeout`.

## Retransmission timer

Unacknowledged frames are retransmitted only when the retransmission timeout expires, not every time the layer is polled. Every expiry without an acknowledgement doubles the timeout, up to the maximum. An acknowledgement of new data resets it.

Time comes from the `Timer` trait, so the layer does not depend on any runtime:

- `StdTimer` (feature `std`) works with any executor. It wakes the task from a helper thread, started the first time it is needed.
- On other platforms, implement `Timer` with your platform's timer. `start(timeout)` arms a one-shot deadline, `stop()` disarms it, and `poll_expired(cx)` returns `Ready` once the deadline has passed or registers the waker otherwise.

## Flushing

`flush` completes once the peer has acknowledged everything written so far. The last frame of the flushed burst asks the peer to acknowledge it immediately, so request/response traffic does not wait for further writes. Ordinary writes are still acknowledged in batches. `flush` also waits until any ACK owed to the peer has been written and flushed on the lower channel. `flush` does not close the link and can be called repeatedly.

## End of stream

`AsyncWrite::shutdown` flushes pending data in a final `FIN` frame and signals end-of-stream to the peer. Afterwards:

- further `write`s fail with `ArqError::Closed`
- the peer's `read` returns `0` bytes once its stream is drained

Completion means that application data transfer is finished, not that the layer can no longer receive. If the ACK for a `FIN` is lost, the peer retransmits the `FIN` and the completed layer acknowledges the duplicate whenever it is polled. It never sends new data or reopens writing, and no linger timer is used. A lost final ACK is therefore recovered only while the instance keeps being polled; after polling stops or the instance is dropped it cannot be.

## Errors

The layer reports `ArqError<ChannelError>`:

| Variant | Meaning |
| --- | --- |
| `Io(e)` | I/O error from the underlying channel |
| `Framing(e)` | received data cannot be split into frames or a frame failed validation, and the byte stream cannot be resynchronized (unknown frame type, impossible length, or a bad CRC) |
| `InvalidAck(e)` | an ACK frame could not be decoded |
| `Timeout` | an ACK was not received in time |
| `Closed` | the link is closed |

On a byte-stream channel the length field is the only frame boundary, so a frame that fails its CRC check cannot be trusted to have a valid length. It fails the link with `Framing` and the lower channel is no longer read. With a framing transport (`EiaFramed`) boundaries are authoritative: an invalid frame is discarded and recovered by retransmission.

The default ACK codec corrects up to 11 bit errors in each half of its codeword and rejects an ACK that needed more. Outside that radius a decode can still produce another valid codeword, so the CRC is additional validation, not an absolute guarantee.

With the `tokio` interface, errors are returned as `std::io::Error` carrying the original `ArqError` as payload (recover it with `get_ref` and `downcast_ref`). The kind is:

| Error | `ErrorKind` |
| --- | --- |
| `Io(std::io::Error)` | the underlying error's kind |
| `Io(other)` | `Other` |
| `Framing`, `InvalidAck` | `InvalidData` |
| `Timeout` | `TimedOut` |
| `Closed` | `BrokenPipe` |

## Testing

CI runs the tests with default features, `--all-features`, `--no-default-features --features embedded-io` and `--no-default-features --features embedded-io,std`, and checks `embedded-io` on `thumbv7em-none-eabihf`.
