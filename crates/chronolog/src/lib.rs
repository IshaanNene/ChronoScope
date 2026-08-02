pub mod chan;
pub mod client;
pub mod codec;
pub mod kv;
pub mod log;
pub mod msg;
pub mod node;
pub mod raft;
pub mod types;
pub mod wal;

/// An `io::Error` for a `chronolog`-level invariant breach, as opposed to a
/// simulated or real device fault.
pub fn codec_io_error(msg: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg)
}
