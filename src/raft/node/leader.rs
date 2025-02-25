use super::super::{Address, Event, Instruction, Message, Request, Response, Status};
use super::{Follower, Node, RoleNode, HEARTBEAT_INTERVAL};
use crate::error::{Error, Result};

use ::log::{debug, info, warn};
use std::collections::HashMap;

// A leader serves requests and replicates the log to followers.
#[derive(Debug)]
pub struct Leader {
    /// Number of ticks since last heartbeat.
    heartbeat_ticks: u64,
    /// The next index to replicate to a peer.
    /// 每个follow节点当前复制的index
    peer_next_index: HashMap<String, u64>,
    /// The last index known to be replicated on a peer.
    peer_last_index: HashMap<String, u64>,
}

impl Leader {
    /// Creates a new leader role.
    pub fn new(peers: Vec<String>, last_index: u64) -> Self {
        let mut leader = Self {
            heartbeat_ticks: 0,
            peer_next_index: HashMap::new(),
            peer_last_index: HashMap::new(),
        };
        for peer in peers {
            leader.peer_next_index.insert(peer.clone(), last_index + 1);
            leader.peer_last_index.insert(peer.clone(), 0);
        }
        // 返回leader
        leader
    }
}

// leader成为follow
impl RoleNode<Leader> {
    /// Transforms the leader into a follower
    fn become_follower(mut self, term: u64, leader: &str) -> Result<RoleNode<Follower>> {
        // 打印leader信息
        info!("Discovered new leader {} for term {}, following", leader, term);
        self.term = term;
        self.log.save_term(term, None)?;
        self.state_tx.send(Instruction::Abort)?;
        // 将自己的状态转化成为follower
        self.become_role(Follower::new(Some(leader), None))
    }

    /// Appends an entry to the log and replicates it to peers.
    pub fn append(&mut self, command: Option<Vec<u8>>) -> Result<u64> {
        // log append
        let entry = self.log.append(self.term, command)?;
        // 迭代每个follower 开始复制
        for peer in self.peers.iter() {
            self.replicate(peer)?;
        }
        // 返回entry的index
        Ok(entry.index)
    }

    /// Commits any pending log entries.
    /// 用来commit
    fn commit(&mut self) -> Result<u64> {
        let mut last_indexes = vec![self.log.last_index];
        last_indexes.extend(self.role.peer_last_index.values());
        last_indexes.sort_unstable();
        last_indexes.reverse();
        let quorum_index = last_indexes[self.quorum() as usize - 1];

        // We can only safely commit up to an entry from our own term, see figure 8 in Raft paper.
        if quorum_index > self.log.commit_index {
            if let Some(entry) = self.log.get(quorum_index)? {
                // 如果任期term
                if entry.term == self.term {
                    let old_commit_index = self.log.commit_index;
                    self.log.commit(quorum_index)?;
                    // 扫描这之间的log
                    let mut scan = self.log.scan((old_commit_index + 1)..=self.log.commit_index);
                    while let Some(entry) = scan.next().transpose()? {
                        // 给状态机执行
                        self.state_tx.send(Instruction::Apply { entry })?;
                    }
                }
            }
        }
        Ok(self.log.commit_index)
    }

    /// Replicates the log to a peer.
    fn replicate(&self, peer: &str) -> Result<()> {
        // 复制偏移量
        let peer_next = self
            .role
            .peer_next_index
            .get(peer)
            .cloned()
            .ok_or_else(|| Error::Internal(format!("Unknown peer {}", peer)))?;
        // 上一个index
        let base_index = if peer_next > 0 { peer_next - 1 } else { 0 };
        // 上一个任期
        let base_term = match self.log.get(base_index)? {
            Some(base) => base.term,
            None if base_index == 0 => 0,
            None => return Err(Error::Internal(format!("Missing base entry {}", base_index))),
        };

        // 多个entries
        let entries = self.log.scan(peer_next..).collect::<Result<Vec<_>>>()?;
        debug!("Replicating {} entries at base {} to {}", entries.len(), base_index, peer);
        self.send(
            Address::Peer(peer.to_string()),
            Event::ReplicateEntries { base_index, base_term, entries },
        )?;
        Ok(())
    }

    /// Processes a message.
    pub fn step(mut self, msg: Message) -> Result<Node> {
        if let Err(err) = self.validate(&msg) {
            warn!("Ignoring invalid message: {}", err);
            return Ok(self.into());
        }
        // 消息的term大于本地节点的term
        if msg.term > self.term {
            if let Address::Peer(from) = &msg.from {
                // 自己成为follow
                return self.become_follower(msg.term, from)?.step(msg);
            }
        }

        // 消息的事件
        match msg.event {
            // 如果是confirm leader
            Event::ConfirmLeader { commit_index, has_committed } => {
                // 获取消息的来源
                if let Address::Peer(from) = msg.from.clone() {
                    // 返回响应
                    self.state_tx.send(Instruction::Vote {
                        term: msg.term,
                        index: commit_index,
                        address: msg.from,
                    })?;
                    if !has_committed {
                        self.replicate(&from)?;
                    }
                }
            }

            // 采纳来该entry
            Event::AcceptEntries { last_index } => {
                // 获取节点
                if let Address::Peer(from) = msg.from {
                    // 对应节点已经accept了
                    self.role.peer_last_index.insert(from.clone(), last_index);
                    // 预期的index
                    self.role.peer_next_index.insert(from, last_index + 1);
                }
                // 能够提交，将能够提交的部分持久化
                self.commit()?;
            }

            // 拒绝来该entry
            Event::RejectEntries => {
                if let Address::Peer(from) = msg.from {
                    self.role.peer_next_index.entry(from.clone()).and_modify(|i| {
                        if *i > 1 {
                            *i -= 1
                        }
                    });
                    self.replicate(&from)?;
                }
            }

            // 客户端请求
            Event::ClientRequest { id, request: Request::Query(command) } => {
                self.state_tx.send(Instruction::Query {
                    id,
                    address: msg.from,
                    command,
                    // 自己的term
                    term: self.term,
                    // 当前commit index
                    index: self.log.commit_index,
                    // 法定人数
                    quorum: self.quorum(),
                })?;
                // 投票投自己
                self.state_tx.send(Instruction::Vote {
                    term: self.term,
                    index: self.log.commit_index,
                    address: Address::Local,
                })?;
                if !self.peers.is_empty() {
                    self.send(
                        // 广播所有的节点
                        Address::Peers,
                        // 心跳内容
                        Event::Heartbeat {
                            commit_index: self.log.commit_index,
                            commit_term: self.log.commit_term,
                        },
                    )?;
                }
            }

            // 客户端的请求，写请求
            Event::ClientRequest { id, request: Request::Mutate(command) } => {
                // append 命令
                let index = self.append(Some(command))?;
                self.state_tx.send(Instruction::Notify { id, address: msg.from, index })?;
                // 只有自己节点
                if self.peers.is_empty() {
                    self.commit()?;
                }
            }

            // 查询状态
            Event::ClientRequest { id, request: Request::Status } => {
                let mut status = Box::new(Status {
                    server: self.id.clone(),
                    leader: self.id.clone(),
                    term: self.term,
                    node_last_index: self.role.peer_last_index.clone(),
                    commit_index: self.log.commit_index,
                    apply_index: 0,
                    storage: self.log.store.to_string(),
                    storage_size: self.log.store.size(),
                });
                status.node_last_index.insert(self.id.clone(), self.log.last_index);
                self.state_tx.send(Instruction::Status { id, address: msg.from, status })?
            }

            // 发送响应
            Event::ClientResponse { id, mut response } => {
                if let Ok(Response::Status(ref mut status)) = response {
                    status.server = self.id.clone();
                }
                self.send(Address::Client, Event::ClientResponse { id, response })?;
            }

            // We ignore these messages, since they are typically additional votes from the previous
            // election that we won after a quorum.
            Event::SolicitVote { .. } | Event::GrantVote => {}

            // 心跳
            Event::Heartbeat { .. } | Event::ReplicateEntries { .. } => {
                warn!("Received unexpected message {:?}", msg)
            }
        }

        Ok(self.into())
    }

    /// Processes a logical clock tick.
    /// 逻辑时钟处理
    pub fn tick(mut self) -> Result<Node> {
        // 心跳协议
        if !self.peers.is_empty() {
            self.role.heartbeat_ticks += 1;
            if self.role.heartbeat_ticks >= HEARTBEAT_INTERVAL {
                self.role.heartbeat_ticks = 0;
                self.send(
                    Address::Peers,
                    // 自己节点信息
                    Event::Heartbeat {
                        commit_index: self.log.commit_index,
                        commit_term: self.log.commit_term,
                    },
                )?;
            }
        }
        // 转成Node类型
        Ok(self.into())
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::{Entry, Log};
    use super::super::tests::{assert_messages, assert_node};
    use super::*;
    use crate::storage::log;
    use futures::FutureExt;
    use pretty_assertions::assert_eq;
    use tokio::sync::mpsc;

    #[allow(clippy::type_complexity)]
    fn setup() -> Result<(
        RoleNode<Leader>,
        mpsc::UnboundedReceiver<Message>,
        mpsc::UnboundedReceiver<Instruction>,
    )> {
        let (node_tx, node_rx) = mpsc::unbounded_channel();
        // 返回接收channel
        let (state_tx, state_rx) = mpsc::unbounded_channel();
        let peers = vec!["b".into(), "c".into(), "d".into(), "e".into()];
        let mut log = Log::new(Box::new(log::Test::new()))?;
        log.append(1, Some(vec![0x01]))?;
        log.append(1, Some(vec![0x02]))?;
        log.append(2, Some(vec![0x03]))?;
        log.append(3, Some(vec![0x04]))?;
        log.append(3, Some(vec![0x05]))?;
        log.commit(2)?;
        log.save_term(3, None)?;

        let node = RoleNode {
            id: "a".into(),
            peers: peers.clone(),
            term: 3,
            role: Leader::new(peers, log.last_index),
            log,
            node_tx,
            state_tx,
            proxied_reqs: HashMap::new(),
            queued_reqs: Vec::new(),
        };
        Ok((node, node_rx, state_rx))
    }

    #[test]
    // ConfirmLeader triggers vote
    fn step_confirmleader_vote() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let mut node: Node = leader.into();

        node = node.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::ConfirmLeader { commit_index: 2, has_committed: true },
        })?;
        assert_node(&node).is_leader().term(3).committed(2);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(
            &mut state_rx,
            vec![Instruction::Vote { term: 3, index: 2, address: Address::Peer("b".into()) }],
        );
        Ok(())
    }

    #[test]
    // ConfirmLeader without has_committed triggers replication
    fn step_confirmleader_replicate() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let mut node: Node = leader.into();

        node = node.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::ConfirmLeader { commit_index: 2, has_committed: false },
        })?;
        assert_node(&node).is_leader().term(3).committed(2);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("b".into()),
                term: 3,
                event: Event::ReplicateEntries { base_index: 5, base_term: 3, entries: vec![] },
            }],
        );
        assert_messages(
            &mut state_rx,
            vec![Instruction::Vote { term: 3, index: 2, address: Address::Peer("b".into()) }],
        );
        Ok(())
    }

    #[test]
    // Heartbeats from other leaders in current term are ignored.
    fn step_heartbeat_current_term() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let mut node: Node = leader.into();

        node = node.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::Heartbeat { commit_index: 5, commit_term: 3 },
        })?;
        assert_node(&node).is_leader().term(3).committed(2);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    // Heartbeats from other leaders in future term converts to follower and steps.
    fn step_heartbeat_future_term() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let mut node: Node = leader.into();

        node = node.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 4,
            event: Event::Heartbeat { commit_index: 7, commit_term: 4 },
        })?;
        assert_node(&node).is_follower().term(4).leader(Some("b")).committed(2);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peer("b".into()),
                term: 4,
                event: Event::ConfirmLeader { commit_index: 7, has_committed: false },
            }],
        );
        assert_messages(&mut state_rx, vec![Instruction::Abort]);
        Ok(())
    }

    #[test]
    // Heartbeats from other leaders in past terms are ignored.
    fn step_heartbeat_past_term() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let mut node: Node = leader.into();

        node = node.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 2,
            event: Event::Heartbeat { commit_index: 3, commit_term: 2 },
        })?;
        assert_node(&node).is_leader().term(3).committed(2);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);
        Ok(())
    }

    #[test]
    fn step_acceptentries() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let mut node: Node = leader.into();

        node = node.step(Message {
            from: Address::Peer("b".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::AcceptEntries { last_index: 4 },
        })?;
        assert_node(&node).committed(2);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(&mut state_rx, vec![]);

        node = node.step(Message {
            from: Address::Peer("c".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::AcceptEntries { last_index: 5 },
        })?;
        assert_node(&node).committed(4);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(
            &mut state_rx,
            vec![
                Instruction::Apply {
                    entry: Entry { index: 3, term: 2, command: Some(vec![0x03]) },
                },
                Instruction::Apply {
                    entry: Entry { index: 4, term: 3, command: Some(vec![0x04]) },
                },
            ],
        );

        node = node.step(Message {
            from: Address::Peer("d".into()),
            to: Address::Peer("a".into()),
            term: 3,
            event: Event::AcceptEntries { last_index: 5 },
        })?;
        assert_node(&node).committed(5);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(
            &mut state_rx,
            vec![Instruction::Apply {
                entry: Entry { index: 5, term: 3, command: Some(vec![0x05]) },
            }],
        );

        assert_node(&node).is_leader().term(3);
        Ok(())
    }

    #[test]
    // Duplicate AcceptEntries from single node should not trigger commit.
    fn step_acceptentries_duplicate() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let mut node: Node = leader.into();

        for _ in 0..5 {
            node = node.step(Message {
                from: Address::Peer("b".into()),
                to: Address::Peer("a".into()),
                term: 3,
                event: Event::AcceptEntries { last_index: 5 },
            })?;
            assert_node(&node).is_leader().term(3).committed(2);
            assert_messages(&mut node_rx, vec![]);
            assert_messages(&mut state_rx, vec![]);
        }
        Ok(())
    }

    #[test]
    // AcceptEntries quorum for entry in past term should not trigger commit
    fn step_acceptentries_past_term() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let peers = leader.peers.clone();
        let mut node: Node = leader.into();

        for peer in peers.into_iter() {
            node = node.step(Message {
                from: Address::Peer(peer),
                to: Address::Peer("a".into()),
                term: 3,
                event: Event::AcceptEntries { last_index: 3 },
            })?;
            assert_node(&node).is_leader().term(3).committed(2);
            assert_messages(&mut node_rx, vec![]);
            assert_messages(&mut state_rx, vec![]);
        }
        Ok(())
    }

    #[test]
    // AcceptEntries quorum for missing future entry
    fn step_acceptentries_future_index() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let peers = leader.peers.clone();
        let mut node: Node = leader.into();

        for (i, peer) in peers.into_iter().enumerate() {
            node = node.step(Message {
                from: Address::Peer(peer),
                to: Address::Peer("a".into()),
                term: 3,
                event: Event::AcceptEntries { last_index: 7 },
            })?;
            // The local leader will cast a vote to commit 5, thus when we have votes 2x7, 1x5, 2x0
            // we will commit index 5. However, we will correctly ignore the following votes for7.
            let c = if i == 0 { 2 } else { 5 };
            assert_node(&node).is_leader().term(3).committed(c).last(5);
            assert_messages(&mut node_rx, vec![]);
            if i == 1 {
                assert_messages(
                    &mut state_rx,
                    vec![
                        Instruction::Apply {
                            entry: Entry { index: 3, term: 2, command: Some(vec![0x03]) },
                        },
                        Instruction::Apply {
                            entry: Entry { index: 4, term: 3, command: Some(vec![0x04]) },
                        },
                        Instruction::Apply {
                            entry: Entry { index: 5, term: 3, command: Some(vec![0x05]) },
                        },
                    ],
                );
            } else {
                assert_messages(&mut state_rx, vec![]);
            }
        }
        Ok(())
    }

    #[test]
    fn step_rejectentries() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let entries = leader.log.scan(0..).collect::<Result<Vec<_>>>()?;
        let mut node: Node = leader.into();

        for i in 0..(entries.len() + 3) {
            node = node.step(Message {
                from: Address::Peer("b".into()),
                to: Address::Peer("a".into()),
                term: 3,
                event: Event::RejectEntries,
            })?;
            assert_node(&node).is_leader().term(3).committed(2);
            let index = if i >= entries.len() { 0 } else { entries.len() - i - 1 };
            let replicate = entries.get(index..).unwrap().to_vec();
            assert_messages(
                &mut node_rx,
                vec![Message {
                    from: Address::Local,
                    to: Address::Peer("b".into()),
                    term: 3,
                    event: Event::ReplicateEntries {
                        base_index: index as u64,
                        base_term: if index > 0 {
                            entries.get(index - 1).map(|e| e.term).unwrap()
                        } else {
                            0
                        },
                        entries: replicate,
                    },
                }],
            );
            assert_messages(&mut state_rx, vec![]);
        }
        Ok(())
    }

    #[test]
    // Sending a client query request will pass it to the state machine and trigger heartbeats.
    fn step_clientrequest_query() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let quorum = leader.quorum();
        let mut node: Node = leader.into();
        node = node.step(Message {
            from: Address::Client,
            to: Address::Local,
            term: 0,
            event: Event::ClientRequest { id: vec![0x01], request: Request::Query(vec![0xaf]) },
        })?;
        assert_node(&node).is_leader().term(3).committed(2).last(5);
        assert_messages(
            &mut node_rx,
            vec![Message {
                from: Address::Local,
                to: Address::Peers,
                term: 3,
                event: Event::Heartbeat { commit_index: 2, commit_term: 1 },
            }],
        );
        assert_messages(
            &mut state_rx,
            vec![
                Instruction::Query {
                    id: vec![0x01],
                    address: Address::Client,
                    command: vec![0xaf],
                    term: 3,
                    index: 2,
                    quorum,
                },
                Instruction::Vote { term: 3, index: 2, address: Address::Local },
            ],
        );
        Ok(())
    }

    #[test]
    // Sending a mutate request should append it to log, replicate it to peers, and register notification.
    fn step_clientrequest_mutate() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let peers = leader.peers.clone();
        let mut node: Node = leader.into();

        node = node.step(Message {
            from: Address::Client,
            to: Address::Local,
            term: 0,
            event: Event::ClientRequest { id: vec![0x01], request: Request::Mutate(vec![0xaf]) },
        })?;
        assert_node(&node).is_leader().term(3).committed(2).last(6).entry(Entry {
            index: 6,
            term: 3,
            command: Some(vec![0xaf]),
        });

        for peer in peers.iter().cloned() {
            assert_eq!(
                node_rx.recv().now_or_never(),
                Some(Some(Message {
                    from: Address::Local,
                    to: Address::Peer(peer),
                    term: 3,
                    event: Event::ReplicateEntries {
                        base_index: 5,
                        base_term: 3,
                        entries: vec![Entry { index: 6, term: 3, command: Some(vec![0xaf]) },]
                    },
                }))
            )
        }
        assert_messages(&mut node_rx, vec![]);
        assert_messages(
            &mut state_rx,
            vec![Instruction::Notify { id: vec![0x01], address: Address::Client, index: 6 }],
        );

        Ok(())
    }

    #[test]
    // Sending a status request should pass it on to state machine, to add status.
    fn step_clientrequest_status() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let mut node: Node = leader.into();

        node = node.step(Message {
            from: Address::Client,
            to: Address::Local,
            term: 0,
            event: Event::ClientRequest { id: vec![0x01], request: Request::Status },
        })?;
        assert_node(&node).is_leader().term(3).committed(2).last(5);
        assert_messages(&mut node_rx, vec![]);
        assert_messages(
            &mut state_rx,
            vec![Instruction::Status {
                id: vec![0x01],
                address: Address::Client,
                status: Box::new(Status {
                    server: "a".into(),
                    leader: "a".into(),
                    term: 3,
                    node_last_index: vec![
                        ("a".into(), 5),
                        ("b".into(), 0),
                        ("c".into(), 0),
                        ("d".into(), 0),
                        ("e".into(), 0),
                    ]
                    .into_iter()
                    .collect(),
                    commit_index: 2,
                    apply_index: 0,
                    storage: "test".into(),
                    storage_size: 130,
                }),
            }],
        );

        Ok(())
    }

    #[test]
    fn tick() -> Result<()> {
        let (leader, mut node_rx, mut state_rx) = setup()?;
        let mut node: Node = leader.into();
        for _ in 0..5 {
            for _ in 0..HEARTBEAT_INTERVAL {
                assert_messages(&mut node_rx, vec![]);
                assert_messages(&mut state_rx, vec![]);
                node = node.tick()?;
                assert_node(&node).is_leader().term(3).committed(2);
            }

            assert_eq!(
                node_rx.recv().now_or_never(),
                Some(Some(Message {
                    from: Address::Local,
                    to: Address::Peers,
                    term: 3,
                    event: Event::Heartbeat { commit_index: 2, commit_term: 1 },
                }))
            );
        }
        Ok(())
    }
}
