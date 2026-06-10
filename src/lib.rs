//! A minimal Raft consensus skeleton demonstrating core types and message handlers.

/// Represents the role of a Raft node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    /// Follower replicates entries and votes for candidates.
    Follower,
    /// Candidate is requesting votes to become leader.
    Candidate,
    /// Leader handles client requests and replicates log entries.
    Leader,
}

/// A single entry in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// The term in which this entry was created.
    pub term: u64,
    /// The command to apply to the state machine.
    pub command: String,
}

/// A Raft node holding persistent state and the current role.
#[derive(Debug)]
pub struct Node {
    /// Latest term the node has seen.
    pub current_term: u64,
    /// CandidateId that received this node's vote in the current term (if any).
    pub voted_for: Option<u64>,
    /// The replicated log entries.
    pub log: Vec<LogEntry>,
    /// The current role of this node.
    pub role: Role,
}

/// RequestVote RPC arguments sent by candidates to gather votes.
#[derive(Debug)]
pub struct RequestVote {
    /// Candidate's term.
    pub term: u64,
    /// Candidate requesting vote.
    pub candidate_id: u64,
    /// Index of candidate's last log entry.
    pub last_log_index: u64,
    /// Term of candidate's last log entry.
    pub last_log_term: u64,
}

/// AppendEntries RPC arguments sent by leaders to replicate log entries.
#[derive(Debug)]
pub struct AppendEntries {
    /// Leader's term.
    pub term: u64,
    /// Leader's id so follower can redirect clients.
    pub leader_id: u64,
    /// Index of log entry immediately preceding new ones.
    pub prev_log_index: u64,
    /// Term of prev_log_index entry.
    pub prev_log_term: u64,
    /// Log entries to store (empty for heartbeat).
    pub entries: Vec<LogEntry>,
    /// Leader's commit index.
    pub leader_commit: u64,
}

impl Node {
    /// Creates a new Node initialized as a Follower with term 0 and an empty log.
    pub fn new() -> Self {
        Node {
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            role: Role::Follower,
        }
    }

    /// Handles an incoming RequestVote RPC.
    ///
    /// Returns `true` if the vote is granted.
    pub fn handle_request_vote(&mut self, req: &RequestVote) -> bool {
        todo!()
    }

    /// Handles an incoming AppendEntries RPC.
    ///
    /// Returns `true` if the request is accepted.
    pub fn handle_append_entries(&mut self, req: &AppendEntries) -> bool {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_initial_state() {
        let node = Node::new();
        assert_eq!(node.current_term, 0);
        assert_eq!(node.role, Role::Follower);
        assert!(node.log.is_empty());
    }

    #[test]
    fn log_entry_construction() {
        let entry = LogEntry {
            term: 1,
            command: "set x = 10".to_string(),
        };
        assert_eq!(entry.term, 1);
        assert_eq!(entry.command, "set x = 10");
    }
}
