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
    /// The unique identifier of this node.
    pub id: u64,
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

/// RequestVote RPC reply sent by voters back to the candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteReply {
    /// Current term of the voter (for the candidate to update itself).
    pub term: u64,
    /// `true` means the candidate received the vote.
    pub vote_granted: bool,
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

/// AppendEntries RPC reply sent by followers back to the leader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesReply {
    /// Current term of the follower (for the leader to update itself).
    pub term: u64,
    /// `true` if the follower matched prev_log_index/prev_log_term and appends succeeded.
    pub success: bool,
}

impl Node {
    /// Creates a new Node initialized as a Follower with term 0 and an empty log.
    pub fn new() -> Self {
        Node {
            id: 0,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            role: Role::Follower,
        }
    }

    /// Transitions the node to Follower for the given term.
    ///
    /// Updates `current_term`, resets `voted_for` to `None`, and sets `role` to `Role::Follower`.
    pub fn become_follower(&mut self, term: u64) {
        self.current_term = term;
        self.voted_for = None;
        self.role = Role::Follower;
    }

    /// Transitions the node to Candidate for a new term.
    ///
    /// Increments `current_term`, votes for itself (`voted_for = Some(self.id)`),
    /// and sets `role` to `Role::Candidate`.
    pub fn become_candidate(&mut self) {
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.role = Role::Candidate;
    }

    /// Transitions the node to Leader.
    ///
    /// Sets `role` to `Role::Leader`. Keeps `current_term` and `voted_for` unchanged.
    pub fn become_leader(&mut self) {
        self.role = Role::Leader;
    }

    /// Returns the index of the last entry in the log.
    ///
    /// Returns `0` when the log is empty.
    pub fn last_log_index(&self) -> u64 {
        self.log.len() as u64
    }

    /// Returns the term of the last entry in the log.
    ///
    /// Returns `0` when the log is empty.
    pub fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    /// Returns the term at the given 1-based log index.
    ///
    /// Returns `None` when the index is `0` or out of bounds.
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 || index > self.log.len() as u64 {
            return None;
        }
        Some(self.log[(index - 1) as usize].term)
    }

    /// Appends a single entry to the log.
    pub fn append_to_log(&mut self, entry: LogEntry) {
        self.log.push(entry);
    }

    /// Handles an incoming RequestVote RPC.
    ///
    /// Grants the vote only when all Raft conditions are satisfied:
    /// * `req.term >= current_term` (step down to Follower and update term if higher).
    /// * The node hasn't voted for a different candidate in this term (`voted_for` is `None` or matches `candidate_id`).
    /// * The candidate's log is at least as up-to-date as the node's own log.
    ///
    /// Returns a [`RequestVoteReply`] containing the voter's current term and whether the vote was granted.
    pub fn handle_request_vote(&mut self, req: &RequestVote) -> RequestVoteReply {
        if req.term > self.current_term {
            self.become_follower(req.term);
        }

        if req.term < self.current_term {
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            };
        }

        // Check whether we already voted for a different candidate this term.
        if self.voted_for.is_some() && self.voted_for != Some(req.candidate_id) {
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            };
        }

        // Candidate's log must be at least as up-to-date.
        let is_up_to_date = req.last_log_term > self.last_log_term()
            || (req.last_log_term == self.last_log_term()
                && req.last_log_index >= self.last_log_index());

        if !is_up_to_date {
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            };
        }

        self.voted_for = Some(req.candidate_id);
        RequestVoteReply {
            term: self.current_term,
            vote_granted: true,
        }
    }

    /// Handles an incoming AppendEntries RPC.
    ///
    /// Rejects the request when:
    /// * `req.term < current_term`
    /// * The log is missing `prev_log_index` or the term at that index does not match `prev_log_term`
    ///
    /// Otherwise updates the term and steps down to Follower, appends/truncates entries as needed,
    /// and returns an [`AppendEntriesReply`] with `success: true`.
    pub fn handle_append_entries(&mut self, req: &AppendEntries) -> AppendEntriesReply {
        // 1. Reject if leader's term is older.
        if req.term < self.current_term {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
            };
        }

        // 2. Update term and step down to Follower if leader's term is newer or equal.
        if req.term > self.current_term {
            self.become_follower(req.term);
        } else {
            self.role = Role::Follower;
        }

        // 3. Check prev_log_index / prev_log_term consistency.
        if req.prev_log_index > 0 {
            if req.prev_log_index > self.log.len() as u64 {
                return AppendEntriesReply {
                    term: self.current_term,
                    success: false,
                };
            }
            let prev_term = self.log[(req.prev_log_index - 1) as usize].term;
            if prev_term != req.prev_log_term {
                return AppendEntriesReply {
                    term: self.current_term,
                    success: false,
                };
            }
        }

        // 4. Append/truncate entries.
        let start_idx = req.prev_log_index as usize; // 0-based Vec position for first new entry
        for (i, entry) in req.entries.iter().enumerate() {
            let idx = start_idx + i;
            if idx < self.log.len() {
                if self.log[idx].term != entry.term {
                    // Conflict found: truncate existing log and append remaining entries.
                    self.log.truncate(idx);
                    self.log.push(entry.clone());
                }
                // If terms match, the existing entry is already correct; skip.
            } else {
                self.log.push(entry.clone());
            }
        }

        AppendEntriesReply {
            term: self.current_term,
            success: true,
        }
    }

    /// Starts a new election, transitioning the node to Candidate.
    ///
    /// Increments `current_term`, resets `voted_for` to this node's own `id`,
    /// updates `role` to `Role::Candidate`, and returns a [`RequestVote`] RPC
    /// populated with the node's current term, id and last-log metadata.
    pub fn start_election(&mut self) -> RequestVote {
        self.become_candidate();

        RequestVote {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        }
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

    #[test]
    fn request_vote_reply_construction() {
        let reply = RequestVoteReply {
            term: 3,
            vote_granted: true,
        };
        assert_eq!(reply.term, 3);
        assert!(reply.vote_granted);
    }

    #[test]
    fn append_entries_reply_construction() {
        let reply = AppendEntriesReply {
            term: 2,
            success: false,
        };
        assert_eq!(reply.term, 2);
        assert!(!reply.success);
    }

    // ------------------------------------------------------------------
    // handle_request_vote tests (via RequestVoteReply)
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
        let reply = node.handle_request_vote(&req);
        assert_eq!(reply, RequestVoteReply {
            term: 5,
            vote_granted: false,
        });
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
        let reply = node.handle_request_vote(&req);
        assert_eq!(reply, RequestVoteReply {
            term: 2,
            vote_granted: true,
        });
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
        let reply = node.handle_request_vote(&req);
        assert_eq!(reply, RequestVoteReply {
            term: 5,
            vote_granted: true,
        });
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
        let reply = node.handle_request_vote(&req);
        assert_eq!(reply, RequestVoteReply {
            term: 3,
            vote_granted: false,
        });
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
        let reply = node.handle_request_vote(&req);
        assert_eq!(reply, RequestVoteReply {
            term: 3,
            vote_granted: true,
        });
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
        let reply = node.handle_request_vote(&req);
        assert_eq!(reply, RequestVoteReply {
            term: 5,
            vote_granted: false,
        });
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
        let reply = node.handle_request_vote(&req);
        assert_eq!(reply, RequestVoteReply {
            term: 3,
            vote_granted: false,
        });
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
        let reply = node.handle_request_vote(&req);
        assert_eq!(reply, RequestVoteReply {
            term: 3,
            vote_granted: true,
        });
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
        let reply = node.handle_request_vote(&req);
        assert_eq!(reply, RequestVoteReply {
            term: 2,
            vote_granted: true,
        });
        assert_eq!(node.voted_for, Some(7));
    }

    // ------------------------------------------------------------------
    // handle_append_entries tests (via AppendEntriesReply)
    // ------------------------------------------------------------------

    #[test]
    fn append_entries_rejected_when_term_is_lower() {
        let mut node = Node::new();
        node.current_term = 5;
        let req = AppendEntries {
            term: 3,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                term: 3,
                command: "set x = 1".to_string(),
            }],
            leader_commit: 0,
        };
        let reply = node.handle_append_entries(&req);
        assert_eq!(reply, AppendEntriesReply {
            term: 5,
            success: false,
        });
        assert_eq!(node.current_term, 5);
    }

    #[test]
    fn append_entries_steps_down_and_updates_term_when_higher_term() {
        let mut node = Node::new();
        node.current_term = 2;
        node.role = Role::Candidate;
        let req = AppendEntries {
            term: 5,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        let reply = node.handle_append_entries(&req);
        assert_eq!(reply, AppendEntriesReply {
            term: 5,
            success: true,
        });
        assert_eq!(node.current_term, 5);
        assert_eq!(node.role, Role::Follower);
    }

    #[test]
    fn append_entries_rejected_when_prev_log_missing() {
        let mut node = Node::new();
        node.log = vec![LogEntry {
            term: 1,
            command: "a".to_string(),
        }];
        let req = AppendEntries {
            term: 2,
            leader_id: 1,
            prev_log_index: 2,
            prev_log_term: 1,
            entries: vec![LogEntry {
                term: 2,
                command: "b".to_string(),
            }],
            leader_commit: 0,
        };
        let reply = node.handle_append_entries(&req);
        assert_eq!(reply, AppendEntriesReply {
            term: 2,
            success: false,
        });
        assert_eq!(node.log.len(), 1);
    }

    #[test]
    fn append_entries_rejected_when_prev_log_term_mismatches() {
        let mut node = Node::new();
        node.log = vec![
            LogEntry {
                term: 1,
                command: "a".to_string(),
            },
            LogEntry {
                term: 2,
                command: "b".to_string(),
            },
        ];
        let req = AppendEntries {
            term: 3,
            leader_id: 1,
            prev_log_index: 2,
            prev_log_term: 1, // actual term at index 2 is 2
            entries: vec![LogEntry {
                term: 3,
                command: "c".to_string(),
            }],
            leader_commit: 0,
        };
        let reply = node.handle_append_entries(&req);
        assert_eq!(reply, AppendEntriesReply {
            term: 3,
            success: false,
        });
    }

    #[test]
    fn append_entries_appends_new_entries() {
        let mut node = Node::new();
        node.log = vec![LogEntry {
            term: 1,
            command: "a".to_string(),
        }];
        let req = AppendEntries {
            term: 2,
            leader_id: 1,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![
                LogEntry {
                    term: 2,
                    command: "b".to_string(),
                },
                LogEntry {
                    term: 2,
                    command: "c".to_string(),
                },
            ],
            leader_commit: 0,
        };
        let reply = node.handle_append_entries(&req);
        assert_eq!(reply, AppendEntriesReply {
            term: 2,
            success: true,
        });
        assert_eq!(node.log.len(), 3);
        assert_eq!(node.log[1].command, "b");
        assert_eq!(node.log[2].command, "c");
    }

    #[test]
    fn append_entries_truncates_on_conflict_and_appends() {
        let mut node = Node::new();
        node.log = vec![
            LogEntry {
                term: 1,
                command: "a".to_string(),
            },
            LogEntry {
                term: 2,
                command: "b".to_string(),
            },
            LogEntry {
                term: 3,
                command: "c".to_string(),
            },
        ];
        let req = AppendEntries {
            term: 4,
            leader_id: 1,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![
                LogEntry {
                    term: 4,
                    command: "x".to_string(),
                },
                LogEntry {
                    term: 4,
                    command: "y".to_string(),
                },
            ],
            leader_commit: 0,
        };
        let reply = node.handle_append_entries(&req);
        assert_eq!(reply, AppendEntriesReply {
            term: 4,
            success: true,
        });
        assert_eq!(node.log.len(), 3);
        assert_eq!(node.log[0].command, "a");
        assert_eq!(node.log[0].term, 1);
        assert_eq!(node.log[1].command, "x");
        assert_eq!(node.log[1].term, 4);
        assert_eq!(node.log[2].command, "y");
        assert_eq!(node.log[2].term, 4);
    }

    #[test]
    fn append_entries_heartbeat_accepted_with_empty_entries() {
        let mut node = Node::new();
        node.current_term = 2;
        node.role = Role::Candidate;
        let req = AppendEntries {
            term: 2,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        let reply = node.handle_append_entries(&req);
        assert_eq!(reply, AppendEntriesReply {
            term: 2,
            success: true,
        });
        assert_eq!(node.role, Role::Follower);
    }

    #[test]
    fn append_entries_does_not_duplicate_existing_entries() {
        let mut node = Node::new();
        node.log = vec![
            LogEntry {
                term: 1,
                command: "a".to_string(),
            },
            LogEntry {
                term: 2,
                command: "b".to_string(),
            },
        ];
        let req = AppendEntries {
            term: 2,
            leader_id: 1,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![
                LogEntry {
                    term: 2,
                    command: "b".to_string(),
                },
                LogEntry {
                    term: 2,
                    command: "c".to_string(),
                },
            ],
            leader_commit: 0,
        };
        let reply = node.handle_append_entries(&req);
        assert_eq!(reply, AppendEntriesReply {
            term: 2,
            success: true,
        });
        assert_eq!(node.log.len(), 3);
        assert_eq!(node.log[0].command, "a");
        assert_eq!(node.log[1].command, "b");
        assert_eq!(node.log[2].command, "c");
    }

    // ------------------------------------------------------------------
    // start_election tests
    // ------------------------------------------------------------------

    #[test]
    fn start_election_transitions_to_candidate() {
        let mut node = Node::new();
        node.id = 42;
        let req = node.start_election();
        assert_eq!(node.role, Role::Candidate);
        assert_eq!(node.current_term, 1);
        assert_eq!(node.voted_for, Some(42));
        assert_eq!(req.term, 1);
        assert_eq!(req.candidate_id, 42);
        assert_eq!(req.last_log_index, 0);
        assert_eq!(req.last_log_term, 0);
    }

    #[test]
    fn start_election_increments_term_each_call() {
        let mut node = Node::new();
        node.id = 1;
        let req1 = node.start_election();
        assert_eq!(req1.term, 1);
        let req2 = node.start_election();
        assert_eq!(req2.term, 2);
        assert_eq!(node.current_term, 2);
    }

    #[test]
    fn start_election_populates_last_log_metadata() {
        let mut node = Node::new();
        node.id = 7;
        node.log = vec![
            LogEntry {
                term: 2,
                command: "a".to_string(),
            },
            LogEntry {
                term: 3,
                command: "b".to_string(),
            },
        ];
        let req = node.start_election();
        assert_eq!(node.role, Role::Candidate);
        assert_eq!(node.current_term, 1);
        assert_eq!(node.voted_for, Some(7));
        assert_eq!(req.last_log_index, 2);
        assert_eq!(req.last_log_term, 3);
    }

    #[test]
    fn start_election_overwrites_prior_vote() {
        let mut node = Node::new();
        node.id = 5;
        node.current_term = 3;
        node.voted_for = Some(99);
        node.role = Role::Follower;
        let req = node.start_election();
        assert_eq!(node.voted_for, Some(5));
        assert_eq!(req.candidate_id, 5);
    }

    // ------------------------------------------------------------------
    // state transition helper tests
    // ------------------------------------------------------------------

    #[test]
    fn become_follower_sets_term_role_and_voted_for() {
        let mut node = Node::new();
        node.current_term = 5;
        node.role = Role::Leader;
        node.voted_for = Some(99);
        node.become_follower(7);
        assert_eq!(node.current_term, 7);
        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.voted_for, None);
    }

    #[test]
    fn become_follower_does_not_panic_on_same_term() {
        let mut node = Node::new();
        node.become_follower(3);
        node.become_follower(3);
        assert_eq!(node.current_term, 3);
        assert_eq!(node.role, Role::Follower);
    }

    #[test]
    fn become_candidate_increments_term_votes_self_and_sets_role() {
        let mut node = Node::new();
        node.id = 42;
        node.current_term = 4;
        node.role = Role::Follower;
        node.voted_for = None;
        node.become_candidate();
        assert_eq!(node.current_term, 5);
        assert_eq!(node.role, Role::Candidate);
        assert_eq!(node.voted_for, Some(42));
    }

    #[test]
    fn become_candidate_overwrites_existing_vote() {
        let mut node = Node::new();
        node.id = 7;
        node.current_term = 2;
        node.voted_for = Some(99);
        node.become_candidate();
        assert_eq!(node.current_term, 3);
        assert_eq!(node.voted_for, Some(7));
    }

    #[test]
    fn become_leader_sets_role_keeps_term_and_voted_for() {
        let mut node = Node::new();
        node.current_term = 6;
        node.voted_for = Some(42);
        node.role = Role::Candidate;
        node.become_leader();
        assert_eq!(node.role, Role::Leader);
        assert_eq!(node.current_term, 6);
        assert_eq!(node.voted_for, Some(42));
    }

    #[test]
    fn become_leader_idempotent() {
        let mut node = Node::new();
        node.become_leader();
        node.become_leader();
        assert_eq!(node.role, Role::Leader);
    }

    #[test]
    fn full_cycle_follower_to_candidate_to_leader() {
        let mut node = Node::new();
        node.id = 1;
        node.become_candidate();
        assert_eq!(node.role, Role::Candidate);
        assert_eq!(node.current_term, 1);
        node.become_leader();
        assert_eq!(node.role, Role::Leader);
        assert_eq!(node.current_term, 1);
        node.become_follower(5);
        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.current_term, 5);
        assert_eq!(node.voted_for, None);
    }

    // ------------------------------------------------------------------
    // log helper tests
    // ------------------------------------------------------------------

    #[test]
    fn last_log_index_on_empty_log_is_zero() {
        let node = Node::new();
        assert_eq!(node.last_log_index(), 0);
    }

    #[test]
    fn last_log_index_with_entries() {
        let mut node = Node::new();
        node.append_to_log(LogEntry {
            term: 1,
            command: "a".to_string(),
        });
        node.append_to_log(LogEntry {
            term: 2,
            command: "b".to_string(),
        });
        assert_eq!(node.last_log_index(), 2);
    }

    #[test]
    fn last_log_term_on_empty_log_is_zero() {
        let node = Node::new();
        assert_eq!(node.last_log_term(), 0);
    }

    #[test]
    fn last_log_term_with_entries() {
        let mut node = Node::new();
        node.append_to_log(LogEntry {
            term: 1,
            command: "a".to_string(),
        });
        node.append_to_log(LogEntry {
            term: 3,
            command: "b".to_string(),
        });
        assert_eq!(node.last_log_term(), 3);
    }

    #[test]
    fn term_at_returns_none_for_zero_index() {
        let mut node = Node::new();
        node.append_to_log(LogEntry {
            term: 1,
            command: "a".to_string(),
        });
        assert_eq!(node.term_at(0), None);
    }

    #[test]
    fn term_at_returns_none_for_out_of_bounds() {
        let mut node = Node::new();
        node.append_to_log(LogEntry {
            term: 1,
            command: "a".to_string(),
        });
        assert_eq!(node.term_at(2), None);
        assert_eq!(node.term_at(99), None);
    }

    #[test]
    fn term_at_returns_term_for_valid_index() {
        let mut node = Node::new();
        node.append_to_log(LogEntry {
            term: 1,
            command: "a".to_string(),
        });
        node.append_to_log(LogEntry {
            term: 3,
            command: "b".to_string(),
        });
        assert_eq!(node.term_at(1), Some(1));
        assert_eq!(node.term_at(2), Some(3));
    }

    #[test]
    fn append_to_log_increments_length() {
        let mut node = Node::new();
        assert!(node.log.is_empty());
        node.append_to_log(LogEntry {
            term: 2,
            command: "x".to_string(),
        });
        assert_eq!(node.log.len(), 1);
        assert_eq!(node.log[0].term, 2);
        assert_eq!(node.log[0].command, "x");
    }

    #[test]
    fn log_helpers_used_by_internal_methods() {
        let mut node = Node::new();
        node.id = 1;
        node.append_to_log(LogEntry {
            term: 2,
            command: "a".to_string(),
        });
        let req = node.start_election();
        assert_eq!(req.last_log_index, 1);
        assert_eq!(req.last_log_term, 2);
        assert_eq!(node.last_log_index(), 1);
        assert_eq!(node.last_log_term(), 2);
    }

    #[test]
    fn helpers_agree_with_direct_log_inspection() {
        let mut node = Node::new();
        assert_eq!(node.last_log_index(), node.log.len() as u64);
        node.append_to_log(LogEntry {
            term: 5,
            command: "c".to_string(),
        });
        assert_eq!(node.last_log_index(), node.log.len() as u64);
        assert_eq!(node.last_log_term(), node.log.last().unwrap().term);
        for (i, entry) in node.log.iter().enumerate() {
            assert_eq!(node.term_at((i + 1) as u64), Some(entry.term));
        }
    }
}
