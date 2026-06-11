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
    /// Grants the vote only when all Raft conditions are satisfied:
    /// * `req.term >= current_term` (step down to Follower and update term if higher).
    /// * The node hasn't voted for a different candidate in this term (`voted_for` is `None` or matches `candidate_id`).
    /// * The candidate's log is at least as up-to-date as the node's own log.
    ///
    /// Returns `true` if the vote is granted.
    pub fn handle_request_vote(&mut self, req: &RequestVote) -> bool {
        if req.term > self.current_term {
            self.current_term = req.term;
            self.voted_for = None;
            self.role = Role::Follower;
        }

        if req.term < self.current_term {
            return false;
        }

        // Check whether we already voted for a different candidate this term.
        if self.voted_for.is_some() && self.voted_for != Some(req.candidate_id) {
            return false;
        }

        // Determine the node's own last-log metadata.
        let (last_log_index, last_log_term) = self
            .log
            .last()
            .map(|e| (self.log.len() as u64, e.term))
            .unwrap_or((0, 0));

        // Candidate's log must be at least as up-to-date.
        let is_up_to_date = req.last_log_term > last_log_term
            || (req.last_log_term == last_log_term && req.last_log_index >= last_log_index);

        if !is_up_to_date {
            return false;
        }

        self.voted_for = Some(req.candidate_id);
        true
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

    // ------------------------------------------------------------------
    // handle_request_vote tests
    // ------------------------------------------------------------------

    #[test]
    fn vote_denied_when_term_is_lower() {
        let mut node = Node::new();
        node.current_term = 5;
        let req = RequestVote {
            term: 3,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        };
        assert!(!node.handle_request_vote(&req));
        assert_eq!(node.voted_for, None);
    }

    #[test]
    fn vote_granted_on_equal_term_when_no_prior_vote() {
        let mut node = Node::new();
        node.current_term = 2;
        let req = RequestVote {
            term: 2,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        };
        assert!(node.handle_request_vote(&req));
        assert_eq!(node.voted_for, Some(1));
    }

    #[test]
    fn steps_down_and_updates_term_when_higher_term_seen() {
        let mut node = Node::new();
        node.current_term = 2;
        node.role = Role::Candidate;
        node.voted_for = Some(99);
        let req = RequestVote {
            term: 5,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        };
        assert!(node.handle_request_vote(&req));
        assert_eq!(node.current_term, 5);
        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.voted_for, Some(1));
    }

    #[test]
    fn vote_denied_when_already_voted_for_different_candidate() {
        let mut node = Node::new();
        node.current_term = 3;
        node.voted_for = Some(2);
        let req = RequestVote {
            term: 3,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        };
        assert!(!node.handle_request_vote(&req));
        assert_eq!(node.voted_for, Some(2));
    }

    #[test]
    fn vote_granted_when_already_voted_for_same_candidate() {
        let mut node = Node::new();
        node.current_term = 3;
        node.voted_for = Some(1);
        let req = RequestVote {
            term: 3,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        };
        assert!(node.handle_request_vote(&req));
        assert_eq!(node.voted_for, Some(1));
    }

    #[test]
    fn vote_denied_when_candidates_log_is_stale_by_term() {
        let mut node = Node::new();
        node.log = vec![
            LogEntry {
                term: 2,
                command: "a".to_string(),
            },
            LogEntry {
                term: 4,
                command: "b".to_string(),
            },
        ];
        let req = RequestVote {
            term: 5,
            candidate_id: 1,
            last_log_index: 2,
            last_log_term: 3, // node last_log_term is 4
        };
        assert!(!node.handle_request_vote(&req));
        assert_eq!(node.voted_for, None);
    }

    #[test]
    fn vote_denied_when_candidates_log_is_stale_by_index() {
        let mut node = Node::new();
        node.log = vec![
            LogEntry {
                term: 2,
                command: "a".to_string(),
            },
            LogEntry {
                term: 2,
                command: "b".to_string(),
            },
        ];
        let req = RequestVote {
            term: 3,
            candidate_id: 1,
            last_log_index: 1, // node last_log_index is 2
            last_log_term: 2,
        };
        assert!(!node.handle_request_vote(&req));
    }

    #[test]
    fn vote_granted_when_candidates_log_is_equally_up_to_date() {
        let mut node = Node::new();
        node.log = vec![
            LogEntry {
                term: 2,
                command: "a".to_string(),
            },
            LogEntry {
                term: 2,
                command: "b".to_string(),
            },
        ];
        let req = RequestVote {
            term: 3,
            candidate_id: 1,
            last_log_index: 2,
            last_log_term: 2,
        };
        assert!(node.handle_request_vote(&req));
        assert_eq!(node.voted_for, Some(1));
    }

    #[test]
    fn vote_granted_when_candidate_log_is_more_up_to_date() {
        let mut node = Node::new();
        node.log = vec![LogEntry {
            term: 1,
            command: "a".to_string(),
        }];
        let req = RequestVote {
            term: 2,
            candidate_id: 7,
            last_log_index: 5,
            last_log_term: 3,
        };
        assert!(node.handle_request_vote(&req));
        assert_eq!(node.voted_for, Some(7));
    }
}
