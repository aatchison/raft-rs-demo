# raft-rs-demo

A minimal Raft consensus skeleton in Rust.

This project demonstrates core Raft types and message handlers without a full network or persistence layer:

- `Role` – Follower, Candidate, or Leader
- `Node` – holds persistent state (`current_term`, `voted_for`, `log`) and the current role
- `LogEntry` – a single entry in the replicated log
- `RequestVote` – RPC sent by candidates to gather votes
- `AppendEntries` – RPC sent by leaders to replicate log entries

The `Node` handler methods (`handle_request_vote` and `handle_append_entries`) are currently stubs marked with `todo!()`.

## Building

```bash
cargo build
```

## Testing

```bash
cargo test
```
