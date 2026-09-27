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
| `embedded-io` | `embedded_io_async::Read`/`Write` interface. Wrap your stream in `embedded_io::EiaLower`. |
| `serde` (default) | `Serialize`/`Deserialize` for the error types. |
| `std` | Enables `std` in `embedded-io-async`. |
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

### embedded-io

```rust
use arq_io_async::embedded_io::EiaLower;
use arq_io_async::{ArqLayer, r};

let link = /* ... implements embedded_io_async::Read + Write ... */;
let layer = ArqLayer::<8, _, _>::new();
let mut arq = layer.build::<16, { r::<8>() }, _>(EiaLower(link));

let mut buf = [0u8; 16];
let n = arq.read(&mut buf).await?;
```

## Configuration

- `N` (const generic on `ArqLayer`): retransmission window, in frames. Must be even and in `2..=32`.
- `M` (chosen at `ArqLayer::build`): ACK codeword length, in bytes. Use `16` for the default codec.
- `R` (chosen at `ArqLayer::build`): read buffer size, in bytes. Must be at least `r::<N>()`.
- The CRC defaults to CRC-16/X-25; replace it with `ArqLayer::with_crc`.
- The default ACK codec is error-correcting; replace it with your own `AckCodec` implementation via `ArqLayer::with_ack_codec_type`.

## End of stream

`AsyncWrite::shutdown` flushes pending data in a final `FIN` frame and signals end-of-stream to the peer. Afterwards:

- further `write`s fail with `ArqError::Closed`
- the peer's `read` returns `0` bytes once its stream is drained

## Errors

The layer reports `ArqError<ChannelError>`:

| Variant | Meaning |
| --- | --- |
| `Io(e)` | I/O error from the underlying channel |
| `Framing(e)` | a frame failed validation (CRC, length, type, or sequence number) |
| `InvalidAck(e)` | an ACK frame could not be decoded |
| `Timeout` | an ACK was not received in time |
| `Closed` | the link is closed |
