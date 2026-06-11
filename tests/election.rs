use raft_rs_demo::*;

/// Runs a simple 3-node cluster election.
///
/// * Node 1 starts an election (becomes Candidate, term 1).
/// * Nodes 2 and 3 each receive the RequestVote and grant their votes.
/// * Node 1 counts the replies, finds it holds the majority (2 of 3),
///   and transitions to Leader.
#[test]
fn simple_election_produces_leader() {
    let mut nodes: Vec<Node> = (1..=3)
        .map(|id| {
            let mut node = Node::new();
            node.id = id;
            node
        })
        .collect();

    // Node 1 starts the election.
    let request_vote = nodes[0].start_election();
    assert_eq!(nodes[0].role, Role::Candidate);
    assert_eq!(nodes[0].current_term, 1);

    let mut vote_count: usize = 1; // Node 1 votes for itself.

    // Nodes 2 and 3 handle the RequestVote.
    for node in nodes.iter_mut().skip(1) {
        let reply = node.handle_request_vote(&request_vote);
        assert!(reply.vote_granted, "Node {} should grant the vote", node.id);
        assert_eq!(reply.term, 1);
        if reply.vote_granted {
            vote_count += 1;
        }
    }

    // A majority in a 3-node cluster is 2 votes.
    assert!(vote_count >= 2, "Expected a majority of votes, got {}", vote_count);

    // Node 1 transitions to Leader.
    nodes[0].become_leader();
    assert_eq!(nodes[0].role, Role::Leader);
    assert_eq!(nodes[0].current_term, 1);
}
