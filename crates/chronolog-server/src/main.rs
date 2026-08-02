//! `chronolog-server` — a production Chronolog node.
//!
//! The whole point of this binary is how little is in it. It parses flags,
//! builds a [`RealRuntime`], and hands the resulting `Host` to exactly the same
//! `chronolog::node::start` the simulator calls. There is no production
//! variant of Raft, no production variant of the WAL, no `#[cfg]` seam through
//! the consensus code. The universe is swapped; the system is not.
//!
//! That is what makes the bug ledger meaningful. A bug the simulator finds is a
//! bug in the code that runs here.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use chrono_sim::real::{block_on, RealRuntime};
use chrono_sim::time::Nanos;
use chrono_sim::traits::NodeId;
use chronolog::node::{self, NodeHandle, NodeOptions};
use chronolog::raft::RaftOptions;
use chronolog::types::Config;
use chronolog::wal::WalOptions;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "chronolog-server", about = "A Raft-replicated write-ahead log")]
struct Args {
    /// This node's id. Must be unique and stable across restarts — it is the
    /// identity the cluster votes on.
    #[arg(long)]
    id: NodeId,

    /// Address to bind.
    #[arg(long, default_value = "0.0.0.0:7400")]
    listen: SocketAddr,

    /// Peers as `id=host:port`, repeated. Include every voter, including self.
    #[arg(long = "peer", value_parser = parse_peer)]
    peers: Vec<(NodeId, SocketAddr)>,

    /// Directory for the write-ahead log and snapshots.
    #[arg(long, default_value = "/var/lib/chronolog")]
    data_dir: String,

    /// Address for `/metrics`, `/health`, and `/debug/raft`.
    #[arg(long, default_value = "0.0.0.0:7401")]
    admin: SocketAddr,

    /// Milliseconds per Raft tick. Election timeout is 10-20 ticks.
    #[arg(long, default_value_t = 50)]
    tick_ms: u64,

    /// Serve reads from the leader's lease. Faster, and **not linearizable
    /// under clock skew** — the simulator demonstrates exactly that.
    #[arg(long)]
    lease_reads: bool,

    #[arg(long, short)]
    verbose: bool,
}

fn parse_peer(s: &str) -> Result<(NodeId, SocketAddr), String> {
    let (id, addr) = s.split_once('=').ok_or("expected id=host:port")?;
    let id: NodeId = id.parse().map_err(|_| format!("bad node id {id:?}"))?;
    let addr: SocketAddr = addr
        .parse()
        .map_err(|e| format!("bad address {addr:?}: {e}"))?;
    Ok((id, addr))
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();
    if args.peers.is_empty() {
        eprintln!("at least one --peer is required (include this node)");
        std::process::exit(2);
    }
    if !args.peers.iter().any(|(id, _)| *id == args.id) {
        eprintln!("--peer list must include this node's own id ({})", args.id);
        std::process::exit(2);
    }

    let voters: Vec<NodeId> = args.peers.iter().map(|(id, _)| *id).collect();
    eprintln!(
        "chronolog n{} listening on {} | peers {:?} | data {}",
        args.id, args.listen, voters, args.data_dir
    );

    let runtime = RealRuntime::new(
        args.id,
        args.listen,
        args.peers.clone(),
        &args.data_dir,
        args.verbose,
    )?;

    let handle = node::start(
        runtime.host.clone(),
        NodeOptions {
            raft: RaftOptions {
                lease_reads: args.lease_reads,
                snapshot_interval: 10_000,
                ..RaftOptions::default()
            },
            wal: WalOptions::default(),
            tick_interval: Nanos::from_millis(args.tick_ms),
            bootstrap: Config::simple(voters),
            // The invariant oracles are a simulation facility. Publishing the
            // whole log every driver cycle to nobody would be pure cost.
            inspect: false,
        },
    );

    serve_admin(args.admin, handle)
}

// ---------------------------------------------------------------------------
// Admin endpoint
// ---------------------------------------------------------------------------

/// A hand-rolled HTTP/1.1 server for `/metrics`, `/health`, and `/debug/raft`.
///
/// Three endpoints serving flat text does not justify a web framework and the
/// dependency tree under it. This is one blocking accept loop and a `match` on
/// the request line.
fn serve_admin(addr: SocketAddr, handle: NodeHandle) -> std::io::Result<()> {
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;

    let listener = TcpListener::bind(addr)?;
    eprintln!("admin endpoint on http://{addr}/metrics");
    let handle = Arc::new(handle);

    for stream in listener.incoming() {
        let Ok(mut stream) = stream else { continue };
        let handle = Arc::clone(&handle);
        std::thread::spawn(move || {
            let mut line = String::new();
            let mut reader = BufReader::new(match stream.try_clone() {
                Ok(s) => s,
                Err(_) => return,
            });
            if reader.read_line(&mut line).is_err() {
                return;
            }
            let path = line.split_whitespace().nth(1).unwrap_or("/");
            let (status, content_type, body) = match path {
                "/metrics" => ("200 OK", "text/plain; version=0.0.4", prometheus(&handle)),
                "/debug/raft" => ("200 OK", "text/plain", debug_raft(&handle)),
                "/health" => {
                    let s = handle.state.lock().unwrap();
                    // A node whose driver has stopped is up, listening, and
                    // useless. Reporting it healthy is how a stalled replica
                    // stays in a load balancer for a week.
                    if s.driver_error.is_some() {
                        (
                            "503 Service Unavailable",
                            "text/plain",
                            "driver stopped\n".to_string(),
                        )
                    } else {
                        ("200 OK", "text/plain", "ok\n".to_string())
                    }
                }
                _ => ("404 Not Found", "text/plain", String::new()),
            };
            let _ = write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n{body}",
                body.len()
            );
        });
    }
    Ok(())
}

fn prometheus(handle: &NodeHandle) -> String {
    let s = handle.state.lock().unwrap().clone();
    let m = &handle.metrics;
    let mut out = String::with_capacity(2048);

    let mut counter = |name: &str, help: &str, value: u64| {
        out.push_str(&format!("# HELP chronolog_{name} {help}\n"));
        out.push_str(&format!("# TYPE chronolog_{name} counter\n"));
        out.push_str(&format!(
            "chronolog_{name}{{node=\"{}\"}} {value}\n",
            s.node
        ));
    };
    counter(
        "proposals_total",
        "Client writes accepted by this leader.",
        m.get("proposals"),
    );
    counter("commits_total", "Entries committed.", m.get("commits"));
    counter(
        "applies_total",
        "Entries applied to the state machine.",
        m.get("applies"),
    );
    counter(
        "fsyncs_total",
        "Durability barriers issued.",
        m.get("fsyncs"),
    );
    counter(
        "batches_total",
        "Append batches; commits/batches is the group-commit ratio.",
        m.get("batches"),
    );
    counter(
        "batched_entries_total",
        "Entries written across all batches.",
        m.get("batched_entries"),
    );
    counter(
        "elections_started_total",
        "Elections this node began.",
        m.get("elections_started"),
    );
    counter(
        "leadership_gained_total",
        "Times this node became leader.",
        m.get("leadership_gained"),
    );
    counter(
        "leadership_lost_total",
        "Times this node stopped being leader.",
        m.get("leadership_lost"),
    );
    counter(
        "snapshots_taken_total",
        "Snapshots written locally.",
        m.get("snapshots_taken"),
    );
    counter(
        "snapshots_installed_total",
        "Snapshots accepted from a leader.",
        m.get("snapshots_installed"),
    );
    counter(
        "client_requests_total",
        "Client requests received.",
        m.get("client_requests"),
    );
    counter(
        "not_leader_redirects_total",
        "Requests redirected to the leader.",
        m.get("not_leader_redirects"),
    );
    counter(
        "reads_linearizable_total",
        "ReadIndex reads served.",
        m.get("reads_linearizable"),
    );
    counter(
        "reads_lease_total",
        "Lease reads served (not linearizable under clock skew).",
        m.get("reads_lease"),
    );
    counter(
        "reads_stale_total",
        "Stale local reads served.",
        m.get("reads_stale"),
    );

    let mut gauge = |name: &str, help: &str, value: u64| {
        out.push_str(&format!("# HELP chronolog_{name} {help}\n"));
        out.push_str(&format!("# TYPE chronolog_{name} gauge\n"));
        out.push_str(&format!(
            "chronolog_{name}{{node=\"{}\"}} {value}\n",
            s.node
        ));
    };
    gauge("term", "Current Raft term.", s.term);
    gauge(
        "commit_index",
        "Highest committed log index.",
        s.commit_index,
    );
    gauge(
        "applied_index",
        "Highest applied log index.",
        s.applied_index,
    );
    gauge("last_index", "Last index in the log.", s.last_index);
    gauge(
        "snapshot_index",
        "Index the log is compacted through.",
        s.snapshot_index,
    );
    gauge(
        "is_leader",
        "1 if this node is the leader.",
        (s.role == "leader") as u64,
    );
    gauge("keys", "Live keys in the state machine.", s.keys as u64);
    gauge(
        "wal_segments",
        "Segment files on disk.",
        s.wal_segments as u64,
    );
    gauge(
        "wal_bytes",
        "Bytes of write-ahead log on disk.",
        s.wal_bytes,
    );
    // The most operationally useful number here: how far this replica lags its
    // own commit index. A follower whose apply loop falls behind is invisible
    // in every other metric.
    gauge(
        "apply_lag",
        "commit_index - applied_index.",
        s.commit_index.saturating_sub(s.applied_index),
    );
    gauge(
        "driver_up",
        "0 if the driver loop has stopped.",
        s.driver_error.is_none() as u64,
    );

    out
}

fn debug_raft(handle: &NodeHandle) -> String {
    let s = handle.state.lock().unwrap().clone();
    let m = &handle.metrics;
    let mut out = String::new();
    out.push_str(&format!("node            n{}\n", s.node));
    out.push_str(&format!("role            {}\n", s.role));
    out.push_str(&format!("term            {}\n", s.term));
    out.push_str(&format!("leader          {:?}\n", s.leader));
    out.push_str(&format!("config          {}\n", s.config));
    out.push_str(&format!(
        "commit / applied {} / {}\n",
        s.commit_index, s.applied_index
    ));
    out.push_str(&format!(
        "log             [{}..{}]\n",
        s.snapshot_index + 1,
        s.last_index
    ));
    out.push_str(&format!(
        "wal             {} segments, {} bytes\n",
        s.wal_segments, s.wal_bytes
    ));
    out.push_str(&format!("keys            {}\n", s.keys));
    out.push_str(&format!(
        "group commit    {:.2} entries/fsync\n",
        m.batch_ratio()
    ));
    if let Some(e) = &s.driver_error {
        out.push_str(&format!("\n*** DRIVER STOPPED: {e}\n"));
    }
    out.push_str("\nThis node's exact behaviour is reproducible under the simulator:\n");
    out.push_str("    chronoscope run --seed <seed>\n");
    out
}

/// Silences an unused-import warning in builds where nothing calls `block_on`
/// directly; the server's work happens on spawned task threads.
#[allow(dead_code)]
fn _unused() {
    block_on(async {});
    let _ = Ordering::Relaxed;
}
