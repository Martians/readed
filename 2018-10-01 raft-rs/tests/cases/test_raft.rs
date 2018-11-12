// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2015 CoreOS, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp;
use std::collections::HashMap;
use std::ops::Deref;
use std::ops::DerefMut;
use std::panic::{self, AssertUnwindSafe};

use protobuf::{self, RepeatedField};
use raft::eraftpb::{
    ConfChange, ConfChangeType, ConfState, Entry, EntryType, HardState, Message, MessageType,
    Snapshot,
};
use rand;

use raft::storage::MemStorage;
use raft::*;


//fn simplelog_init() {
//    use simplelog::*;
//    use std::fs::File;
//
//    CombinedLogger::init(
//        vec![
//            TermLogger::new(LevelFilter::Debug, Config::default()).unwrap(),
//            WriteLogger::new(LevelFilter::Debug, Config::default(),
//                             File::create("output.log").unwrap()),
//        ]
//    ).unwrap();
//
//    info!("simple log init");
//}

fn simple_logger_init() {
    extern crate simple_logger;
    simple_logger::init().unwrap();
}

use std::sync::{Once, ONCE_INIT};

static INIT: Once = ONCE_INIT;

/// Setup function that is only run once, even if called multiple times.
fn setup() {

    INIT.call_once(|| {
        simple_logger_init();
//        simplelog_init();
    });
}

pub fn ltoa(raft_log: &RaftLog<MemStorage>) -> String {
    let mut s = format!("committed: {}\n", raft_log.committed);
    s = s + &format!("applied: {}\n", raft_log.applied);
    for (i, e) in raft_log.all_entries().iter().enumerate() {
        s = s + &format!("#{}: {:?}\n", i, e);
    }
    s
}

pub fn new_storage() -> MemStorage {
    MemStorage::new()
}

fn new_progress(
    state: ProgressState,
    matched: u64,
    next_idx: u64,
    pending_snapshot: u64,
    ins_size: usize,
) -> Progress {
    Progress {
        state: state,
        matched: matched,
        next_idx: next_idx,
        pending_snapshot: pending_snapshot,
        ins: Inflights::new(ins_size),
        ..Default::default()
    }
}

pub fn new_test_config(id: u64, peers: Vec<u64>, election: usize, heartbeat: usize) -> Config {
    Config {
        id: id,
        peers: peers,
        election_tick: election,
        heartbeat_tick: heartbeat,
        max_size_per_msg: NO_LIMIT,
        max_inflight_msgs: 256,
        ..Default::default()
    }
}

pub fn new_test_raft(
    id: u64,
    peers: Vec<u64>,
    election: usize,
    heartbeat: usize,
    storage: MemStorage,
) -> Interface {
    Interface::new(Raft::new(
        &new_test_config(id, peers, election, heartbeat),
        storage,
    ))
}

pub fn new_test_raft_with_prevote(
    id: u64,
    peers: Vec<u64>,
    election: usize,
    heartbeat: usize,
    storage: MemStorage,
    pre_vote: bool,
) -> Interface {
    let mut config = new_test_config(id, peers, election, heartbeat);
    config.pre_vote = pre_vote;
    config.tag = format!("[{}]", id);
    new_test_raft_with_config(&config, storage)
}

pub fn new_test_raft_with_config(config: &Config, storage: MemStorage) -> Interface {
    //config.tag.as_mut() = format!("{}", config.id);
    Interface::new(Raft::new(config, storage))
}

fn read_messages<T: Storage>(raft: &mut Raft<T>) -> Vec<Message> {
    raft.msgs.drain(..).collect()
}

/// 设置好已有数据的raft
///
///     参数给出的是term，其index是自动生成，递增的
fn ents_with_config(terms: &[u64], pre_vote: bool) -> Interface {
    let store = MemStorage::new();
    for (i, term) in terms.iter().enumerate() {
        let mut e = Entry::new();
        e.set_index(i as u64 + 1);
        e.set_term(*term);
        store.wl().append(&[e]).expect("");
    }
    let mut raft = new_test_raft_with_prevote(1, vec![], 5, 1, store, pre_vote);
    raft.reset(terms[terms.len() - 1]);
    raft
}

/// 生成: 参与了选举，但是无数据的raft
// voted_with_config creates a raft state machine with vote and term set
// to the given value but no log entries (indicating that it voted in
// the given term but has not receive any logs).
fn voted_with_config(vote: u64, term: u64, pre_vote: bool) -> Interface {
    let mut hard_state = HardState::new();
    hard_state.set_vote(vote);
    hard_state.set_term(term);
    let store = MemStorage::new();
    store.wl().set_hardstate(hard_state);
    let mut raft = new_test_raft_with_prevote(1, vec![], 5, 1, store, pre_vote);
    raft.reset(term);
    raft
}

/// 将raft的数据，提取到 storage 中；同时提取raft中可以commit的日志出来
fn  next_ents(r: &mut Raft<MemStorage>, s: &MemStorage) -> Vec<Entry> {
    s.wl()
        .append(r.raft_log.unstable_entries().unwrap_or(&[]))
        .expect("");
    let (last_idx, last_term) = (r.raft_log.last_index(), r.raft_log.last_term());
    r.raft_log.stable_to(last_idx, last_term);
    let ents = r.raft_log.next_entries();
    let committed = r.raft_log.committed;
    r.raft_log.applied_to(committed);
    ents.unwrap_or_else(Vec::new)
}

fn do_send_append(raft: &mut Raft<MemStorage>, to: u64) {
    let mut prs = raft.take_prs();
    {
        let pr = prs.get_mut(to).unwrap();
        raft.send_append(to, pr);
    }
    raft.set_prs(prs);
}

#[derive(Default, Debug, PartialEq, Eq, Hash)]
struct Connem {
    from: u64,
    to: u64,
}

/// Compare to upstream, we use struct instead of trait here.
/// Because to be able to cast Interface later, we have to make
/// Raft derive Any, which will require a lot of dependencies to derive Any.
/// That's not worthy for just testing purpose.
pub struct Interface {
    raft: Option<Raft<MemStorage>>,
}

/**
 * 封装后，即使 Raft为空，也可以执行相应命令
 */
impl Interface {
    fn new(r: Raft<MemStorage>) -> Interface {
        setup();
        Interface { raft: Some(r) }
    }

    pub fn step(&mut self, m: Message) -> Result<()> {
        match self.raft {
            Some(_) => Raft::step(self, m),
            None => Ok(()),
        }
    }

    pub fn read_messages(&mut self) -> Vec<Message> {
        match self.raft {
            Some(_) => self.msgs.drain(..).collect(),
            None => vec![],
        }
    }

    /** 相当于更新内部的 peer 集合 */
    fn initial(&mut self, id: u64, ids: &[u64]) {
        if self.raft.is_some() {
            /* 这里self之间转换成了 raft，而不是interface */

            self.id = id;
            self.tag = format!("[{}]", id);
            let prs = self.take_prs();
            self.set_prs(ProgressSet::new(ids.len(), prs.learners().len()));
            for id in ids {
                if prs.learners().contains_key(id) {
                    let progress = Progress {
                        is_learner: true,
                        ..Default::default()
                    };
                    self.mut_prs().insert_learner(*id, progress);
                } else {
                    let progress = Progress {
                        ..Default::default()
                    };
                    self.mut_prs().insert_voter(*id, progress);
                }
            }
            let term = self.term;
            self.reset(term);
        }
    }
}

impl Deref for Interface {
    type Target = Raft<MemStorage>;
    fn deref(&self) -> &Raft<MemStorage> {
        self.raft.as_ref().unwrap()
    }
}

impl DerefMut for Interface {
    fn deref_mut(&mut self) -> &mut Raft<MemStorage> {
        self.raft.as_mut().unwrap()
    }
}

pub const NOP_STEPPER: Option<Interface> = Some(Interface { raft: None });

pub const SOME_DATA: Option<&'static str> = Some("somedata");

pub fn new_message_with_entries(from: u64, to: u64, t: MessageType, ents: Vec<Entry>) -> Message {
    let mut m = Message::new();
    m.set_from(from);
    m.set_to(to);
    m.set_msg_type(t);
    if !ents.is_empty() {
        m.set_entries(RepeatedField::from_vec(ents));
    }
    m
}

pub fn new_message(from: u64, to: u64, t: MessageType, n: usize) -> Message {
    setup();
    let mut m = new_message_with_entries(from, to, t, vec![]);
    if n > 0 {
        let mut ents = Vec::with_capacity(n);
        for _ in 0..n {
            ents.push(new_entry(0, 0, SOME_DATA));
        }
        m.set_entries(RepeatedField::from_vec(ents));
    }
    m
}

pub fn empty_entry(term: u64, index: u64) -> Entry {
    new_entry(term, index, None)
}

pub fn new_entry(term: u64, index: u64, data: Option<&str>) -> Entry {
    let mut e = Entry::new();
    e.set_index(index);
    e.set_term(term);
    if let Some(d) = data {
        e.set_data(d.as_bytes().to_vec());
    }
    e
}

fn new_raft_log(ents: &[Entry], offset: u64, committed: u64) -> RaftLog<MemStorage> {
    let store = MemStorage::new();
    store.wl().append(ents).expect("");
    RaftLog {
        store: store,
        unstable: Unstable {
            offset: offset,
            ..Default::default()
        },
        committed: committed,
        ..Default::default()
    }
}

fn new_raft_log_with_storage(s: MemStorage) -> RaftLog<MemStorage> {
    RaftLog::new(s, String::from(""))
}

pub fn new_snapshot(index: u64, term: u64, nodes: Vec<u64>) -> Snapshot {
    let mut s = Snapshot::new();
    s.mut_metadata().set_index(index);
    s.mut_metadata().set_term(term);
    s.mut_metadata().mut_conf_state().set_nodes(nodes);
    s
}

#[derive(Default)]
pub struct Network {
    pub peers: HashMap<u64, Interface>,
    storage: HashMap<u64, MemStorage>,
    // 过滤哪些节点之间的消息
    dropm: HashMap<Connem, f64>,
    // 过滤哪些消息类型
    ignorem: HashMap<MessageType, bool>,
}

/// 管理节点、网络
///     模拟网络故障
impl Network {
    // initializes a network from peers.
    // A nil node will be replaced with a new *stateMachine.
    // A *stateMachine will get its k, id.
    // When using stateMachine, the address list is always [1, n].
    pub fn new(peers: Vec<Option<Interface>>) -> Network {
        Network::new_with_config(peers, false)
    }

    // 这里生成 raft，并给 raft 添加 上peer信息（progress）

    // new_with_config is like new but sets the configuration pre_vote explicitly
    // for any state machines it creates.
    pub fn new_with_config(mut peers: Vec<Option<Interface>>, pre_vote: bool) -> Network {
        setup();
        let size = peers.len();
        let peer_addrs: Vec<u64> = (1..size as u64 + 1).collect();
        let mut nstorage = HashMap::new();
        let mut npeers = HashMap::new();
        for (p, id) in peers.drain(..).zip(peer_addrs.clone()) {
            match p {
                None => {
                    nstorage.insert(id, new_storage());
                    let r = new_test_raft_with_prevote(
                        id,
                        peer_addrs.clone(),
                        10,
                        1,
                        nstorage[&id].clone(),
                        pre_vote,
                    );
                    npeers.insert(id, r);
                }
                Some(mut p) => {
                    p.initial(id, &peer_addrs);
                    npeers.insert(id, p);
                }
            }
        }
        Network {
            peers: npeers,
            storage: nstorage,
            ..Default::default()
        }
    }

    fn ignore(&mut self, t: MessageType) {
        self.ignorem.insert(t, true);
    }

    fn filter(&self, mut msgs: Vec<Message>) -> Vec<Message> {
        msgs.drain(..)
            .filter(|m| {
                if self
                    .ignorem
                    .get(&m.get_msg_type())
                    .cloned()
                    .unwrap_or(false)
                {
                    return false;
                }
                // hups never go over the network, so don't drop them but panic
                assert_ne!(m.get_msg_type(), MessageType::MsgHup, "unexpected msgHup");
                let perc = self
                    .dropm
                    .get(&Connem {
                        from: m.get_from(),
                        to: m.get_to(),
                    })
                    .cloned()
                    .unwrap_or(0f64);

                // 比较概率，如果是1就完全过滤掉消息
                rand::random::<f64>() >= perc
            })
            .collect()
    }

    pub fn send(&mut self, msgs: Vec<Message>) {
        let mut msgs = msgs;
        while !msgs.is_empty() {
            let mut new_msgs = vec![];
            for m in msgs.drain(..) {
                let resp = {
                    let p = self.peers.get_mut(&m.get_to()).unwrap();
                    let _ = p.step(m);
                    p.read_messages()
                };
                // 这里会过滤掉，预先设置好的那些消息
                new_msgs.append(&mut self.filter(resp));
            }
            msgs.append(&mut new_msgs);
        }
    }

    fn drop(&mut self, from: u64, to: u64, perc: f64) {
        self.dropm.insert(Connem { from: from, to: to }, perc);
    }

    fn cut(&mut self, one: u64, other: u64) {
        self.drop(one, other, 1f64);
        self.drop(other, one, 1f64);
    }

    // 阻断一个节点与其他所有节点的连接
    fn isolate(&mut self, id: u64) {
        for i in 0..self.peers.len() as u64 {
            let nid = i + 1;
            if nid != id {
                self.drop(id, nid, 1.0);
                self.drop(nid, id, 1.0);
            }
        }
    }

    fn recover(&mut self) {
        self.dropm = HashMap::new();
        self.ignorem = HashMap::new();
    }
}

/// 变成 probe状态， next = match + 1
///     Snapshot：  max(snapshot + 1, match + 1)
#[test]
fn test_progress_become_probe() {
    let matched = 1u64;
    let mut tests = vec![
        (
            new_progress(ProgressState::Replicate, matched, 5, 0, 256),
            2,
        ),
        // snapshot finish
        (
            new_progress(ProgressState::Snapshot, matched, 5, 10, 256),
            11,
        ),
        // snapshot failure
        (new_progress(ProgressState::Snapshot, matched, 5, 0, 256), 2),
    ];
    for (i, &mut (ref mut p, wnext)) in tests.iter_mut().enumerate() {
        p.become_probe();
        if p.state != ProgressState::Probe {
            panic!(
                "#{}: state = {:?}, want {:?}",
                i,
                p.state,
                ProgressState::Probe
            );
        }
        if p.matched != matched {
            panic!("#{}: match = {:?}, want {:?}", i, p.matched, matched);
        }
        if p.next_idx != wnext {
            panic!("#{}: next = {}, want {}", i, p.next_idx, wnext);
        }
    }
}

#[test]
fn test_progress_become_replicate() {
    let mut p = new_progress(ProgressState::Probe, 1, 5, 0, 256);
    p.become_replicate();

    assert_eq!(p.state, ProgressState::Replicate);
    assert_eq!(p.matched, 1);
    assert_eq!(p.matched + 1, p.next_idx);
}

#[test]
fn test_progress_become_snapshot() {
    let mut p = new_progress(ProgressState::Probe, 1, 5, 0, 256);
    p.become_snapshot(10);
    assert_eq!(p.state, ProgressState::Snapshot);
    assert_eq!(p.matched, 1);
    assert_eq!(p.pending_snapshot, 10);
}

/// 设置新的update值之后，检查 progress的 match、next_idx
#[test]
fn test_progress_update() {
    let (prev_m, prev_n) = (3u64, 5u64);

    let tests = vec![
        (prev_m - 1, prev_m, prev_n, false),
        (prev_m, prev_m, prev_n, false),
        (prev_m + 1, prev_m + 1, prev_n, true),
        (prev_m + 2, prev_m + 2, prev_n + 1, true),
    ];
    for (i, &(update, wm, wn, wok)) in tests.iter().enumerate() {
        let mut p = Progress {
            matched: prev_m,
            next_idx: prev_n,
            ..Default::default()
        };
        let ok = p.maybe_update(update);
        if ok != wok {
            panic!("#{}: ok= {}, want {}", i, ok, wok);
        }
        if p.matched != wm {
            panic!("#{}: match= {}, want {}", i, p.matched, wm);
        }
        if p.next_idx != wn {
            panic!("#{}: next= {}, want {}", i, p.next_idx, wn);
        }
    }
}

/// 冲突回退之后，检查 progress的 match、index
///     其中：match 是不会改变的，改变 next_idx
///
///     reject的，必然是自己上次发送的位置，即 progress.next_idx - 1，这个是上次发送的 prev 位置
///
///     回退到reject的位置，下次将发送 reject - 1
#[test]
fn test_progress_maybe_decr() {
    let tests = vec![
        // state replicate and rejected is not greater than match
        (ProgressState::Replicate, 5, 10, 5, 5, false, 10),
        // state replicate and rejected is not greater than match
        (ProgressState::Replicate, 5, 10, 4, 4, false, 10),
        // state replicate and rejected is greater than match
        // directly decrease to match+1
        (ProgressState::Replicate, 5, 10, 9, 9, true, 6),
        // next-1 != rejected is always false
        (ProgressState::Probe, 0, 0, 0, 0, false, 0),
        // next-1 != rejected is always false
        (ProgressState::Probe, 0, 10, 5, 5, false, 10),
        // next>1 = decremented by 1
        (ProgressState::Probe, 0, 10, 9, 9, true, 9),
        // next>1 = decremented by 1
        (ProgressState::Probe, 0, 2, 1, 1, true, 1),
        // next<=1 = reset to 1
        (ProgressState::Probe, 0, 1, 0, 0, true, 1),
        // decrease to min(rejected, last+1)
        (ProgressState::Probe, 0, 10, 9, 2, true, 3),
        // rejected < 1, reset to 1
        (ProgressState::Probe, 0, 10, 9, 0, true, 1),
    ];
    for (i, &(state, m, n, rejected, last, w, wn)) in tests.iter().enumerate() {
        let mut p = new_progress(state, m, n, 0, 0);
        if p.maybe_decr_to(rejected, last) != w {
            panic!("#{}: maybeDecrTo= {}, want {}", i, !w, w);
        }
        if p.matched != m {
            panic!("#{}: match= {}, want {}", i, p.matched, m);
        }
        if p.next_idx != wn {
            panic!("#{}: next= {}, want {}", i, p.next_idx, wn);
        }
    }
}

#[test]
fn test_progress_is_paused() {
    let tests = vec![
        (ProgressState::Probe, false, false),
        (ProgressState::Probe, true, true),
        (ProgressState::Replicate, false, false),
        (ProgressState::Replicate, true, false),
        (ProgressState::Snapshot, false, true),
        (ProgressState::Snapshot, true, true),
    ];
    for (i, &(state, paused, w)) in tests.iter().enumerate() {
        let p = Progress {
            state: state,
            paused: paused,
            ins: Inflights::new(256),
            ..Default::default()
        };
        if p.is_paused() != w {
            panic!("#{}: shouldwait = {}, want {}", i, p.is_paused(), w)
        }
    }
}

/// 两个函数将重置 pause 状态
// test_progress_resume ensures that progress.maybeUpdate and progress.maybeDecrTo
// will reset progress.paused.
#[test]
fn test_progress_resume() {
    let mut p = Progress {
        next_idx: 2,
        paused: true,
        ..Default::default()
    };
    p.maybe_decr_to(1, 1);
    assert!(!p.paused, "paused= true, want false");
    p.paused = true;
    p.maybe_update(2);
    assert!(!p.paused, "paused= true, want false");
}

// test_progress_resume_by_heartbeat_resp ensures raft.heartbeat reset progress.paused by
// heartbeat response.
#[test]
fn test_progress_resume_by_heartbeat_resp() {
    let mut raft = new_test_raft(1, vec![1, 2], 5, 1, new_storage());
    raft.become_candidate();
    raft.become_leader();
    raft.mut_prs().get_mut(2).unwrap().paused = true;

    // 心跳包的回应，会执行pr.resume();
    //  send 完成后变成 paused
    raft.step(new_message(1, 1, MessageType::MsgBeat, 0))
        .expect("");
    assert!(raft.prs().voters()[&2].paused);

    raft.mut_prs().get_mut(2).unwrap().become_replicate();
    raft.step(new_message(2, 1, MessageType::MsgHeartbeatResponse, 0))
        .expect("");
    assert!(!raft.prs().voters()[&2].paused);
}

/// 消息没回应之前，probe 状态只能发送一个消息，即选举成功的空消息
#[test]
fn test_progress_paused() {
    setup();
    let mut raft = new_test_raft(1, vec![1, 2], 5, 1, new_storage());
    raft.become_candidate();
    raft.become_leader();
    let mut m = Message::new();
    m.set_from(1);
    m.set_to(1);
    m.set_msg_type(MessageType::MsgPropose);
    let mut e = Entry::new();
    e.set_data(b"some_data".to_vec());
    m.set_entries(RepeatedField::from_vec(vec![e]));
    raft.step(m.clone()).expect("");
    raft.step(m.clone()).expect("");
    raft.step(m.clone()).expect("");
    let ms = read_messages(&mut raft);
    assert_eq!(ms.len(), 1);
}

#[test]
fn test_leader_election1() {
    test_leader_election_with_config(false);
}

#[test]
fn test_leader_election_pre_vote() {
    test_leader_election_with_config(true);
}

/// 测试不同 peer set，不同 peer 状态下的选举；
///     1）设置为 none，会自动生成 raft；
///     2) 设置为 NOP_STEPPER将不会生成 raft，因此将不会对消息做出回应，相当于节点掉线
///
///     只看 peer 1 的状态
fn test_leader_election_with_config(pre_vote: bool) {
    let mut tests = vec![
        (
            Network::new_with_config(vec![None, None, None], pre_vote),
            StateRole::Leader,
            1,
        ),
        (
            Network::new_with_config(vec![None, None, NOP_STEPPER], pre_vote),
            StateRole::Leader,
            1,
        ),
        (
            Network::new_with_config(vec![None, NOP_STEPPER, NOP_STEPPER], pre_vote),
            StateRole::Candidate,
            1,
        ),
        (
            Network::new_with_config(vec![None, NOP_STEPPER, NOP_STEPPER, None], pre_vote),
            StateRole::Candidate,
            1,
        ),
        (
            Network::new_with_config(vec![None, NOP_STEPPER, NOP_STEPPER, None, None], pre_vote),
            StateRole::Leader,
            1,
        ),
        // three logs further along than 0, but in the same term so rejection
        // are returned instead of the votes being ignored.
        (
            // 预先设置好已有数据的raft
            //
            // 这里并不会选出leader，因为有数据的 node，没有启动定时机制；只有主动要求，才会变成leader
            // node1
            //      因为quorum都否决了自己，因此自己便成为follower，但并不知道leader的值
            //      node1 无法从其他节点获取数据，因为他们都还不是leader
            Network::new_with_config(
                vec![
                    None,
                    Some(ents_with_config(&[1], pre_vote)),
                    Some(ents_with_config(&[1], pre_vote)),
                    Some(ents_with_config(&[1, 1], pre_vote)),
                    None,
                ],
                pre_vote,
            ),
            StateRole::Follower,
            1,
        ),
    ];

    for (i, &mut (ref mut network, state, term)) in tests.iter_mut().enumerate() {
        info!("\n--------------------------------------------------------------------");

        // 只有node 1回启动选举过程，相当于触发了超时机制，因此引起了一系列的动作
        let mut m = Message::new();
        m.set_from(1);
        m.set_to(1);
        m.set_msg_type(MessageType::MsgHup);
        network.send(vec![m]);
        let raft = &network.peers[&1];
        let (exp_state, exp_term) = if state == StateRole::Candidate && pre_vote {
            // In pre-vote mode, an election that fails to complete
            // leaves the node in pre-candidate state without advancing
            // the term.
            (StateRole::PreCandidate, 0)
        } else {
            (state, term)
        };
        if raft.state != exp_state {
            panic!("#{}: state = {:?}, want {:?}", i, raft.state, exp_state);
        }
        if raft.term != exp_term {
            panic!("#{}: term = {}, want {}", i, raft.term, exp_term)
        }
    }
}

#[test]
fn test_leader_cycle1() {
    test_leader_cycle_with_config(false)
}

#[test]
fn test_leader_cycle_pre_vote() {
    test_leader_cycle_with_config(true)
}

// test_leader_cycle verifies that each node in a cluster can campaign
// and be elected in turn. This ensures that elections (including
// pre-vote) work when not starting from a clean state (as they do in
// test_leader_election)
/// 相当于每个node依次触发超时，启动选举过程
///     依次成为 leader
fn test_leader_cycle_with_config(pre_vote: bool) {
    let mut network = Network::new_with_config(vec![None, None, None], pre_vote);
    for campaigner_id in 1..4 {
        info!("\n-------------------------------------------------------------------- {}", campaigner_id);
        network.send(vec![new_message(
            campaigner_id,
            campaigner_id,
            MessageType::MsgHup,
            0,
        )]);

        for sm in network.peers.values() {
            if sm.id == campaigner_id && sm.state != StateRole::Leader {
                panic!(
                    "pre_vote={}: campaigning node {} state = {:?}, want Leader",
                    pre_vote, sm.id, sm.state
                );
            } else if sm.id != campaigner_id && sm.state != StateRole::Follower {
                panic!(
                    "pre_vote={}: after campaign of node {}, node {} had state = {:?}, want \
                     Follower",
                    pre_vote, campaigner_id, sm.id, sm.state
                );
            }
        }
    }
}


#[test]
fn test_leader_election_overwrite_newer_logs1() {
    test_leader_election_overwrite_newer_logs_with_config(false);
}

#[test]
fn test_leader_election_overwrite_newer_logs_pre_vote() {
    test_leader_election_overwrite_newer_logs_with_config(true);
}

/**
 * 构造几个peer，每个上边存在不同的数据，测试最终数据状态
 *
 *     node1 没有最新数据，这里只有它处于活动状态（成为candidate）
 *          同步不断增加term，做经过几个周期之后，成为leader
 *          其他 peer上的新数据将被覆盖
 */
// test_leader_election_overwrite_newer_logs tests a scenario in which a
// newly-elected leader does *not* have the newest (i.e. highest term)
// log entries, and must overwrite higher-term log entries with
// lower-term ones.
fn test_leader_election_overwrite_newer_logs_with_config(pre_vote: bool) {
    // This network represents the results of the following sequence of
    // events:
    // - Node 1 won the election in term 1.
    // - Node 1 replicated a log entry to node 2 but died before sending
    //   it to other nodes.
    // - Node 3 won the second election in term 2.
    // - Node 3 wrote an entry to its logs but died without sending it
    //   to any other nodes.
    //
    // At this point, nodes 1, 2, and 3 all have uncommitted entries in
    // their logs and could win an election at term 3. The winner's log
    // entry overwrites the loser's. (test_leader_sync_follower_log tests
    // the case where older log entries are overwritten, so this test
    // focuses on the case where the newer entries are lost).
    let mut network = Network::new_with_config(
        vec![
            Some(ents_with_config(&[1], pre_vote)), // Node 1: Won first election
            Some(ents_with_config(&[1], pre_vote)), // Node 2: Get logs from node 1
            Some(ents_with_config(&[2], pre_vote)), // Node 3: Won second election
            Some(voted_with_config(3, 2, pre_vote)), // Node 4: Voted but didn't get logs
            Some(voted_with_config(3, 2, pre_vote)), // Node 5: Voted but didn't get logs
        ],
        pre_vote,
    );

    // 两次 tick peer1，第一次更新自己的 term；第二次成为leader

    // Node 1 campaigns. The election fails because a quorum of nodes
    // know about the election that already happened at term 2. Node 1's
    // term is pushed ahead to 2.
    // node3、4、5不同意node1成为leader
    network.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    assert_eq!(network.peers[&1].state, StateRole::Follower);
    assert_eq!(network.peers[&1].term, 2);

    // Node 1 campaigns again with a higher term. this time it succeeds.
    network.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    assert_eq!(network.peers[&1].state, StateRole::Leader);
    assert_eq!(network.peers[&1].term, 3);

    // 检查每个 peer上的 log是否是一致的
    //  一个日志是 peer1本来的，一个日志是 peer1 选主时生成的
    // Now all nodes agree on a log entry with term 1 at index 1 (and
    // term 3 at index 2).
    //  并没有测试注释中提到的 peer3成为leader的场景

    for (id, sm) in &network.peers {
        let entries = sm.raft_log.all_entries();
        assert_eq!(
            entries.len(),
            2,
            "node {}: entries.len() == {}, want 2",
            id,
            entries.len()
        );
        assert_eq!(
            entries[0].get_term(),
            1,
            "node {}: term at index 1 == {}, want 1",
            id,
            entries[0].get_term()
        );
        assert_eq!(
            entries[1].get_term(),
            3,
            "node {}: term at index 2 == {}, want 3",
            id,
            entries[1].get_term()
        );
    }
}

#[test]
fn test_vote_from_any_state() {
    test_vote_from_any_state_for_type(MessageType::MsgRequestVote);
}

#[test]
fn test_prevote_from_any_state() {
    test_vote_from_any_state_for_type(MessageType::MsgRequestPreVote);
}

/**
 * peer1 在任何状态下，收到比自己大的term，node1 都会变成 follower

        收到从 peer2 发送的 vote 消息
 */
fn test_vote_from_any_state_for_type(vt: MessageType) {
    let all_states = vec![
        StateRole::Follower,
        StateRole::Candidate,
        StateRole::PreCandidate,
        StateRole::Leader,
    ];
    for state in all_states {
        let mut r = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
        r.term = 1;
        match state {
            // 设置 peer3 为leader
            StateRole::Follower => {
                let term = r.term;
                r.become_follower(term, 3);
            }
            StateRole::PreCandidate => r.become_pre_candidate(),
            // 成为候选者，同时 term 变成2
            StateRole::Candidate => r.become_candidate(),
            StateRole::Leader => {
                r.become_candidate();
                r.become_leader();
            }
        }
        // Note that setting our state above may have advanced r.term
        // past its initial value.
        let orig_term = r.term;
        let new_term = r.term + 1;

        let mut msg = new_message(2, 1, vt, 0);
        msg.set_term(new_term);
        msg.set_log_term(new_term);
        msg.set_index(42);
        r.step(msg)
            .expect(&format!("{:?},{:?}: step failed", vt, state));
        assert_eq!(
            r.msgs.len(),
            1,
            "{:?},{:?}: {} response messages, want 1: {:?}",
            vt,
            state,
            r.msgs.len(),
            r.msgs
        );
        let resp = &r.msgs[0];
        assert_eq!(
            resp.get_msg_type(),
            vote_resp_msg_type(vt),
            "{:?},{:?}: response message is {:?}, want {:?}",
            vt,
            state,
            resp.get_msg_type(),
            vote_resp_msg_type(vt)
        );
        assert!(
            !resp.get_reject(),
            "{:?},{:?}: unexpected rejection",
            vt,
            state
        );

        // If this was a real vote, we reset our state and term.
        if vt == MessageType::MsgRequestVote {
            assert_eq!(
                r.state,
                StateRole::Follower,
                "{:?},{:?}, state {:?}, want {:?}",
                vt,
                state,
                r.state,
                StateRole::Follower
            );
            assert_eq!(
                r.term, new_term,
                "{:?},{:?}, term {}, want {}",
                vt, state, r.term, new_term
            );
            assert_eq!(r.vote, 2, "{:?},{:?}, vote {}, want 2", vt, state, r.vote);
        } else {
            // In a pre-vote, nothing changes.
            assert_eq!(
                r.state, state,
                "{:?},{:?}, state {:?}, want {:?}",
                vt, state, r.state, state
            );
            assert_eq!(
                r.term, orig_term,
                "{:?},{:?}, term {}, want {}",
                vt, state, r.term, orig_term
            );
            // If state == Follower or PreCandidate, r hasn't voted yet.
            // In Candidate or Leader, it's voted for itself.
            assert!(
                r.vote == INVALID_ID || r.vote == 1,
                "{:?},{:?}, vote {}, want {:?} or 1",
                vt,
                state,
                r.vote,
                INVALID_ID
            );
        }
    }
}

#[test]
fn test_log_replicatioin() {
    let mut tests = vec![
        (
            Network::new(vec![None, None, None]),
            vec![new_message(1, 1, MessageType::MsgPropose, 1)],
            2,
        ),
        (
            Network::new(vec![None, None, None]),
            vec![
                // 1 成为leader，写入一条记录
                // 2 成为leader，写入一条记录
                new_message(1, 1, MessageType::MsgPropose, 1),
                new_message(1, 2, MessageType::MsgHup, 0),
                new_message(1, 2, MessageType::MsgPropose, 1),
            ],
            4,
        ),
    ];

    for (i, &mut (ref mut network, ref msgs, wcommitted)) in tests.iter_mut().enumerate() {
        // 首先 唤醒 node1，然后发送指定的消息， 1 成为 leader
        //      执行完成后，检查各个 peer 上 log commit 的状态
        network.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
        for m in msgs {
            network.send(vec![m.clone()]);
        }

        for (j, x) in &mut network.peers {
            if x.raft_log.committed != wcommitted {
                panic!(
                    "#{}.{}: committed = {}, want {}",
                    i, j, x.raft_log.committed, wcommitted
                );
            }

            let mut ents = next_ents(x, &network.storage[j]);
            // 过滤掉选举成功的那条空消息
            let ents: Vec<Entry> = ents
                .drain(..)
                .filter(|e| !e.get_data().is_empty())
                .collect();
            for (k, m) in msgs
                .iter()

                // 比较所有的 MsgPropose 消息，所有node的内容与预定义的消息是一致的
                .filter(|m| m.get_msg_type() == MessageType::MsgPropose)
                .enumerate()
            {
                if ents[k].get_data() != m.get_entries()[0].get_data() {
                    panic!(
                        "#{}.{}: data = {:?}, want {:?}",
                        i,
                        j,
                        ents[k].get_data(),
                        m.get_entries()[0].get_data()
                    );
                }
            }
        }
    }
}

#[test]
fn test_single_node_commit() {
    let mut tt = Network::new(vec![None]);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    assert_eq!(tt.peers[&1].raft_log.committed, 3);
}

// test_cannot_commit_without_new_term_entry tests the entries cannot be committed
// when leader changes, no new proposal comes in and ChangeTerm proposal is
// filtered.
#[test]
fn test_cannot_commit_without_new_term_entry() {
    let mut tt = Network::new(vec![None, None, None, None, None]);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // 0 cannot reach 2, 3, 4
    tt.cut(1, 3);
    tt.cut(1, 4);
    tt.cut(1, 5);

    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    assert_eq!(tt.peers[&1].raft_log.committed, 1);

    // network recovery
    tt.recover();
    // avoid committing ChangeTerm proposal
    tt.ignore(MessageType::MsgAppend);

    // 2 成为leader，2 有最新数据，但是银族阻断了 append消息，无法发送到其他人
    //      此时 2 有3个消息：1 选举成功，以及随后发的2个消息
    //      只有第一个消息是commit的
    // elect 2 as the new leader with term 2
    tt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);

    // no log entries from previous term should be committed
    assert_eq!(tt.peers[&2].raft_log.committed, 1);

    tt.recover();
    // send heartbeat; reset wait
    tt.send(vec![new_message(2, 2, MessageType::MsgBeat, 0)]);

    // 主动触发超时，leader能够收到各个peer的回应，会想各peer重发消息
    //      处于probe阶段，只要没收到回应，就会一直发送相同的消息
    // 此时，2发送数据，发现其他人都有冲突，重新发送，到此各个peer数据将保持一致

    // append an entry at current term
    tt.send(vec![new_message(2, 2, MessageType::MsgPropose, 1)]);
    // expect the committed to be advanced
    assert_eq!(tt.peers[&2].raft_log.committed, 5);
}

// test_commit_without_new_term_entry tests the entries could be committed
// when leader changes, no new proposal comes in.
#[test]
fn test_commit_without_new_term_entry() {
    let mut tt = Network::new(vec![None, None, None, None, None]);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // 0 cannot reach 2, 3, 4
    tt.cut(1, 3);
    tt.cut(1, 4);
    tt.cut(1, 5);

    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    assert_eq!(tt.peers[&1].raft_log.committed, 1);

    // network recovery
    tt.recover();

    // elect 1 as the new leader with term 2
    // after append a ChangeTerm entry from the current term, all entries
    // should be committed
    tt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);

    // node1两次成为leader，又写入了两条消息
    assert_eq!(tt.peers[&1].raft_log.committed, 4);
}

/// 模拟网络中断，因为大多数的存在，因此不会出现两个leader
#[test]
fn test_dueling_candidates() {
    let a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)]);
    nt.cut(1, 3);

    // 网络分区，1、3 都成为候选者；只有1成为leader
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // 1 becomes leader since it receives votes from 1 and 2
    assert_eq!(nt.peers[&1].state, StateRole::Leader);

    // 3 stays as candidate since it receives a vote from 3 and a rejection from 2
    assert_eq!(nt.peers[&3].state, StateRole::Candidate);

    nt.recover();

    // node3 两次尝试成为leader，有最高的term，但是因为 数据较少，因此被拒绝，无法成为 leader

    // Candidate 3 now increases its term and tries to vote again, we except it to
    // disrupt the leader 1 since it has a higher term, 3 will be follower again
    // since both 1 and 2 rejects its vote request since 3 does not have a long
    // enough log.
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    let wlog = new_raft_log(&[empty_entry(1, 1)], 2, 1);
    let wlog2 = new_raft_log_with_storage(new_storage());

    // 将比较每个node的状态，term、raft_log
    let tests = vec![
        (StateRole::Follower, 2, &wlog),
        (StateRole::Follower, 2, &wlog),
        (StateRole::Follower, 2, &wlog2),
    ];

    for (i, &(state, term, raft_log)) in tests.iter().enumerate() {
        let id = i as u64 + 1;
        if nt.peers[&id].state != state {
            panic!(
                "#{}: state = {:?}, want {:?}",
                i, nt.peers[&id].state, state
            );
        }
        if nt.peers[&id].term != term {
            panic!("#{}: term = {}, want {}", i, nt.peers[&id].term, term);
        }

        // 为了比较复杂一些的结构，现将其转换为字符串，然后再比较
        let base = ltoa(raft_log);
        let l = ltoa(&nt.peers[&(1 + i as u64)].raft_log);
        if base != l {
            panic!("#{}: raft_log:\n {}, want:\n {}", i, l, base);
        }
    }
}

#[test]
fn test_dueling_pre_candidates() {
    let a = new_test_raft_with_prevote(1, vec![1, 2, 3], 10, 1, new_storage(), true);
    let b = new_test_raft_with_prevote(2, vec![1, 2, 3], 10, 1, new_storage(), true);
    let c = new_test_raft_with_prevote(3, vec![1, 2, 3], 10, 1, new_storage(), true);

    let mut nt = Network::new_with_config(vec![Some(a), Some(b), Some(c)], true);
    nt.cut(1, 3);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // 1 becomes leader since it receives votes from 1 and 2
    assert_eq!(nt.peers[&1].state, StateRole::Leader);

    // 3 campaigns then reverts to follower when its pre_vote is rejected
    assert_eq!(nt.peers[&3].state, StateRole::Follower);

    nt.recover();

    // Candidate 3 now increases its term and tries to vote again.
    // With pre-vote, it does not disrupt the leader.
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // 3 发起了一次无效选举，它的term最大，造成了 1、2都成为了follower；
    //      但是因为没有最新数据，因此无法成为leader

    let wlog = new_raft_log(&[empty_entry(0, 0), empty_entry(1, 1)], 2, 1);
    let wlog2 = new_raft_log_with_storage(new_storage());
    let tests = vec![
        (1, StateRole::Leader, 1, &wlog),
        (2, StateRole::Follower, 1, &wlog),
        (3, StateRole::Follower, 1, &wlog2),
    ];
    for (i, &(id, state, term, raft_log)) in tests.iter().enumerate() {
        if nt.peers[&id].state != state {
            panic!(
                "#{}: state = {:?}, want {:?}",
                i, nt.peers[&id].state, state
            );
        }
        if nt.peers[&id].term != term {
            panic!("#{}: term = {}, want {}", i, nt.peers[&id].term, term);
        }
        let base = ltoa(raft_log);
        let l = ltoa(&nt.peers[&(1 + i as u64)].raft_log);
        if base != l {
            panic!("#{}: raft_log:\n {}, want:\n {}", i, l, base);
        }
    }
}

/// beat消息将使得node1变成node3的follower
#[test]
fn test_candidate_concede() {
    let mut tt = Network::new(vec![None, None, None]);
    tt.isolate(1);

    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    tt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // 1被孤立，3成为leader
    // heal the partition
    tt.recover();
    // send heartbeat; reset wait
    tt.send(vec![new_message(3, 3, MessageType::MsgBeat, 0)]);

    // send a proposal to 3 to flush out a MsgAppend to 1
    let data = "force follower";
    let mut m = new_message(3, 3, MessageType::MsgPropose, 0);
    m.set_entries(RepeatedField::from_vec(vec![new_entry(0, 0, Some(data))]));
    tt.send(vec![m]);
    // send heartbeat; flush out commit

    // 3发送beat，1接收beat消息后成为其follower
    tt.send(vec![new_message(3, 3, MessageType::MsgBeat, 0)]);

    assert_eq!(tt.peers[&1].state, StateRole::Follower);
    assert_eq!(tt.peers[&1].term, 1);

    // 构造内部消息的值，每个peer都将包含这些消息
    //      注意：构造的日志都写入到了storage中，因此 unstable 中的 offset = last + 1
    let ents = vec![empty_entry(1, 1), new_entry(1, 2, Some(data))];
    let want_log = ltoa(&new_raft_log(&ents, 3, 2));
    for (id, p) in &tt.peers {
        let l = ltoa(&p.raft_log);
        if l != want_log {
            panic!("#{}: raft_log: {}, want: {}", id, l, want_log);
        }
    }
}

#[test]
fn test_single_node_candidate() {
    let mut tt = Network::new(vec![None]);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(tt.peers[&1].state, StateRole::Leader);
}

#[test]
fn test_sinle_node_pre_candidate() {
    let mut tt = Network::new_with_config(vec![None], true);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(tt.peers[&1].state, StateRole::Leader);
}

/// leader收到旧leader的消息，因为term较小，将被直接忽略
#[test]
fn test_old_messages() {
    let mut tt = Network::new(vec![None, None, None]);
    // make 0 leader @ term 3
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    tt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);
    tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // 1 成为leader；2成为leader；1成为leader，term变成3
    // pretend we're an old leader trying to make progress; this entry is expected to be ignored.
    let mut m = new_message(2, 1, MessageType::MsgAppend, 0);
    m.set_term(2);
    // 旧leader本次发起的消息，将直接被忽略，term较小
    m.set_entries(RepeatedField::from_vec(vec![empty_entry(2, 3)]));
    tt.send(vec![m]);

    // commit a new entry
    tt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    // 前3个是选举消息，第4个是leader1写入的新消息，包含数据 SOME_DATA
    let ents = vec![
        empty_entry(1, 1),
        empty_entry(2, 2),
        empty_entry(3, 3),
        new_entry(3, 4, SOME_DATA),
    ];
    let ilog = new_raft_log(&ents, 5, 4);
    let base = ltoa(&ilog);
    for (id, p) in &tt.peers {
        let l = ltoa(&p.raft_log);
        if l != base {
            panic!("#{}: raft_log: {}, want: {}", id, l, base);
        }
    }
}

// test_old_messages_reply - optimization - reply with new term.

/// 不同的node场景下，检查数据是否写入成功
///     选主成功就能写入成功
#[test]
fn test_proposal() {
    let mut tests = vec![
        (Network::new(vec![None, None, None]), true),
        (Network::new(vec![None, None, NOP_STEPPER]), true),
        (Network::new(vec![None, NOP_STEPPER, NOP_STEPPER]), false),
        (
            Network::new(vec![None, NOP_STEPPER, NOP_STEPPER, None]),
            false,
        ),
        (
            Network::new(vec![None, NOP_STEPPER, NOP_STEPPER, None, None]),
            true,
        ),
    ];

    for (j, (mut nw, success)) in tests.drain(..).enumerate() {

        // 或者写如成功，或者预先设置了失败 success=false，这里将抛出异常
        let send = |nw: &mut Network, m| {
            let res = panic::catch_unwind(AssertUnwindSafe(|| nw.send(vec![m])));
            assert!(res.is_ok() || !success);
        };

        // 1 成为leader，并发起一个写入请求
        // promote 0 the leader
        send(&mut nw, new_message(1, 1, MessageType::MsgHup, 0));
        send(&mut nw, new_message(1, 1, MessageType::MsgPropose, 1));

        // 构造成功、失败情况下，各自的raft log应该有的数据状况
        let want_log = if success {
            new_raft_log(&[empty_entry(1, 1), new_entry(1, 2, SOME_DATA)], 3, 2)
        } else {
            new_raft_log_with_storage(new_storage())
        };
        let base = ltoa(&want_log);

        // 检测只要节点存在，就应该有一样的 raft log
        for (id, p) in &nw.peers {
            if p.raft.is_some() {
                let l = ltoa(&p.raft_log);
                if l != base {
                    panic!("#{}: raft_log: {}, want {}", id, l, base);
                }
            }
        }
        if nw.peers[&1].term != 1 {
            panic!("#{}: term = {}, want: {}", j, nw.peers[&1].term, 1);
        }
    }
}

/// 在有leader的情况下，follower收到的MsgPropose消息，都会发送给leader
#[test]
fn test_proposal_by_proxy() {
    let mut tests = vec![
        Network::new(vec![None, None, None]),
        Network::new(vec![None, None, NOP_STEPPER]),
    ];
    for (j, tt) in tests.iter_mut().enumerate() {
        // promote 0 the leader
        tt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

        // propose via follower
        tt.send(vec![new_message(2, 2, MessageType::MsgPropose, 1)]);

        let want_log = new_raft_log(&[empty_entry(1, 1), new_entry(1, 2, SOME_DATA)], 3, 2);
        let base = ltoa(&want_log);
        for (id, p) in &tt.peers {
            if p.raft.is_none() {
                continue;
            }
            let l = ltoa(&p.raft_log);
            if l != base {
                panic!("#{}: raft_log: {}, want: {}", id, l, base);
            }
        }
        if tt.peers[&1].term != 1 {
            panic!("#{}: term = {}, want {}", j, tt.peers[&1].term, 1);
        }
    }
}

/// 设置各个node的match位置，查看commit是否可以成功
///
///     上一个term的消息，是不能commit的；所以commit位置保持为 0
#[test]
fn test_commit1() {

    // 第一项，值是node所match的位置；第一项有多少个值就有多少个node
    // 第三项，是设置当前的term，与log中的term相比较
    // 最后一项是最终commit的结果
    let mut tests = vec![
        // single
        (vec![1u64], vec![empty_entry(1, 1)], 1u64, 1u64),
        // term不匹配，node1的term还只是1
        (vec![1], vec![empty_entry(1, 1)], 2, 0),
        (vec![2], vec![empty_entry(1, 1), empty_entry(2, 2)], 2, 2),

        (vec![1], vec![empty_entry(2, 1)], 2, 1),
        // odd
        (
            vec![2, 1, 1],
            vec![empty_entry(1, 1), empty_entry(2, 2)],
            1,
            1,
        ),
        (
            vec![2, 1, 1],
            vec![empty_entry(1, 1), empty_entry(1, 2)],
            2,
            0,
        ),
        (
            vec![2, 1, 2],
            vec![empty_entry(1, 1), empty_entry(2, 2)],
            2,
            2,
        ),
        (
            vec![2, 1, 2],
            vec![empty_entry(1, 1), empty_entry(1, 2)],
            2,
            0,
        ),
        // even
        (
            vec![2, 1, 1, 1],
            vec![empty_entry(1, 1), empty_entry(2, 2)],
            1,
            1,
        ),
        (
            vec![2, 1, 1, 1],
            vec![empty_entry(1, 1), empty_entry(1, 2)],
            2,
            0,
        ),
        (
            vec![2, 1, 1, 2],
            vec![empty_entry(1, 1), empty_entry(2, 2)],
            1,
            1,
        ),
        (
            vec![2, 1, 1, 2],
            vec![empty_entry(1, 1), empty_entry(1, 2)],
            2,
            0,
        ),
        (
            vec![2, 1, 2, 2],
            vec![empty_entry(1, 1), empty_entry(2, 2)],
            2,
            2,
        ),
        (
            vec![2, 1, 2, 2],
            vec![empty_entry(1, 1), empty_entry(1, 2)],
            2,
            0,
        ),
    ];

    // sm_term 最终设置的是 raft 的term
    for (i, (matches, logs, sm_term, w)) in tests.drain(..).enumerate() {

        // 构造storage，
        let store = MemStorage::new();
        store.wl().append(&logs).expect("");
        let mut hs = HardState::new();
        hs.set_term(sm_term);
        store.wl().set_hardstate(hs);

        let mut sm = new_test_raft(1, vec![1], 5, 1, store);
        for (j, &v) in matches.iter().enumerate() {
            let id = j as u64 + 1;
            if !sm.prs().voters().contains_key(&id) {
                sm.set_progress(id, v, v + 1, false);
            }
        }
        sm.maybe_commit();
        if sm.raft_log.committed != w {
            panic!("#{}: committed = {}, want {}", i, sm.raft_log.committed, w);
        }
    }
}

#[test]
fn test_pass_election_timeout() {
    // election_elapsed的次数，超时上限是 10
    //      默认：min 10，max 20
    let tests = vec![
        (5, 0f64, false),
        (10, 0.1, true),
        (13, 0.4, true),
        (15, 0.6, true),
        (18, 0.9, true),
        (20, 1.0, false),
    ];

    for (i, &(elapse, wprobability, round)) in tests.iter().enumerate() {
        let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage());
        sm.election_elapsed = elapse;
        let mut c = 0;
        for _ in 0..10_000 {
            sm.reset_randomized_election_timeout();
            if sm.pass_election_timeout() {
                c += 1;
            }
        }
        let mut got = f64::from(c) / 10000.0;
        if round {
            got = (got * 10.0 + 0.5).floor() / 10.0;
        }
        if (got - wprobability).abs() > 0.000001 {
            panic!("#{}: probability = {}, want {}", i, got, wprobability);
        }
    }
}

/// 确保不会处理 term=1 的消息
// ensure that the Step function ignores the message from old term and does not pass it to the
// actual stepX function.
#[test]
fn test_step_ignore_old_term_msg() {
    let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage());

    // 设置一个用于测试的handler，在step处理消息前触发
    let panic_before_step_state =
        Box::new(|_: &Message| panic!("before step state function hook called unexpectedly"));
    sm.before_step_state = Some(panic_before_step_state);

    sm.term = 2;
    let mut m = new_message(0, 0, MessageType::MsgAppend, 0);
    m.set_term(1);
    sm.step(m).expect("");
}


/// 测试follower接收新消息
///     根据自己当前日志的状态，接收不同的 prev log + new log，并执行 commit
///
// test_handle_msg_append ensures:
// 1. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
// 2. If an existing entry conflicts with a new one (same index but different terms),
//    delete the existing entry and all that follow it; append any new entries not already in the
//    log.
// 3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
#[test]
fn test_handle_msg_append() {
    // 用于生成message，前边的内容是message的prev log信息，后边vec是准备新添加的log
    let nm = |term, log_term, index, commit, ents: Option<Vec<(u64, u64)>>| {
        let mut m = Message::new();
        m.set_msg_type(MessageType::MsgAppend);
        m.set_term(term);
        m.set_log_term(log_term);
        m.set_index(index);
        m.set_commit(commit);
        if let Some(ets) = ents {
            m.set_entries(RepeatedField::from_vec(
                ets.iter().map(|&(i, t)| empty_entry(t, i)).collect(),
            ));
        }
        m
    };
    let mut tests = vec![
        // Ensure 1
        // 已有数据：（1,1）（2,2）

        // 查看nm的2、3项，prev log term = 2), log index = 3), commit = 4)
        // tests的2: last_index, 3: commit

        // prev log不匹配
        (nm(2, 3, 2, 3, None), 2, 0, true), // previous log mismatch
        (nm(2, 3, 3, 3, None), 2, 0, true), // previous log non-exist

        // Ensure 2
        // 数据为空，prev log存在于本地，接受对方的commit位置
        (nm(2, 1, 1, 1, None), 2, 1, false),
        // 数据不匹配，用新数据覆盖，并commit
        (nm(2, 0, 0, 1, Some(vec![(1, 2)])), 1, 1, false),
        // 数据匹配，并写入两个新数据，commit到3
        (nm(2, 2, 2, 3, Some(vec![(3, 2), (4, 2)])), 4, 3, false),
        // 数据匹配，写入新数据，commit的位置超过本地最大数据，去 min(commit, last_index)
        (nm(2, 2, 2, 4, Some(vec![(3, 2)])), 3, 3, false),
        (nm(2, 1, 1, 4, Some(vec![(2, 2)])), 2, 2, false),

        // Ensure 3
        // prev log匹配，接收其commit值
        (nm(1, 1, 1, 3, None), 2, 1, false), // match entry 1, commit up to last new entry 1
        // prev log匹配，新数据与本地一致，接收器commit值
        (nm(1, 1, 1, 3, Some(vec![(2, 2)])), 2, 2, false), // match entry 1, commit up to last new
        // entry 2
        (nm(2, 2, 2, 3, None), 2, 2, false), // match entry 2, commit up to last new entry 2
        (nm(2, 2, 2, 4, None), 2, 2, false), // commit up to log.last()
    ];

    for (j, (m, w_index, w_commit, w_reject)) in tests.drain(..).enumerate() {
        let store = new_storage();
        store
            .wl()
            .append(&[empty_entry(1, 1), empty_entry(2, 2)])
            .expect("");
        let mut sm = new_test_raft(1, vec![1], 10, 1, store);
        sm.become_follower(2, INVALID_ID);

        sm.handle_append_entries(&m);
        if sm.raft_log.last_index() != w_index {
            panic!(
                "#{}: last_index = {}, want {}",
                j,
                sm.raft_log.last_index(),
                w_index
            );
        }
        if sm.raft_log.committed != w_commit {
            panic!(
                "#{}: committed = {}, want {}",
                j, sm.raft_log.committed, w_commit
            );
        }
        let m = sm.read_messages();
        if m.len() != 1 {
            panic!("#{}: msg count = {}, want 1", j, m.len());
        }
        if m[0].get_reject() != w_reject {
            panic!("#{}: reject = {}, want {}", j, m[0].get_reject(), w_reject);
        }
    }
}

/// follower通过beat来更新自己的commit
// test_handle_heartbeat ensures that the follower commits to the commit in the message.
#[test]
fn test_handle_heartbeat() {
    // 生成beat消息
    let commit = 2u64;
    let nw = |f, to, term, commit| {
        let mut m = new_message(f, to, MessageType::MsgHeartbeat, 0);
        m.set_term(term);
        m.set_commit(commit);
        m
    };

    // node的 commit位置是2
    let mut tests = vec![
        (nw(2, 1, 2, commit + 1), commit + 1),

        // follower收到的beat消息中，commit的值比自己的还小
        (nw(2, 1, 2, commit - 1), commit), // do not decrease commit
    ];


    for (i, (m, w_commit)) in tests.drain(..).enumerate() {
        let store = new_storage();
        store
            .wl()
            .append(&[empty_entry(1, 1), empty_entry(2, 2), empty_entry(3, 3)])
            .expect("");
        let mut sm = new_test_raft(1, vec![1, 2], 5, 1, store);
        sm.become_follower(2, 2);
        sm.raft_log.commit_to(commit);
        sm.handle_heartbeat(m);
        if sm.raft_log.committed != w_commit {
            panic!(
                "#{}: committed = {}, want = {}",
                i, sm.raft_log.committed, w_commit
            );
        }
        let m = sm.read_messages();
        // 产生的beat的回应消息？
        if m.len() != 1 {
            panic!("#{}: msg count = {}, want 1", i, m.len());
        }
        if m[0].get_msg_type() != MessageType::MsgHeartbeatResponse {
            panic!(
                "#{}: type = {:?}, want MsgHeartbeatResponse",
                i,
                m[0].get_msg_type()
            );
        }
    }
}

/// 网络故障的follower，将通过beat消息触发 leader向其重发消息
// test_handle_heartbeat_resp ensures that we re-send log entries when we get a heartbeat response.
#[test]
fn test_handle_heartbeat_resp() {
    // 两个node，3个log全部提交
    let store = new_storage();
    store
        .wl()
        .append(&[empty_entry(1, 1), empty_entry(2, 2), empty_entry(3, 3)])
        .expect("");
    let mut sm = new_test_raft(1, vec![1, 2], 5, 1, store);
    sm.become_candidate();
    sm.become_leader();
    let last_index = sm.raft_log.last_index();
    sm.raft_log.commit_to(last_index);

    // 1 的消息没有send给2
    //      只要1收到MsgHeartbeatResponse，就会重新发送append
    // A heartbeat response from a node that is behind; re-send MsgApp
    sm.step(new_message(2, 0, MessageType::MsgHeartbeatResponse, 0))
        .expect("");
    let mut msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgAppend);

    // A second heartbeat response generates another MsgApp re-send
    sm.step(new_message(2, 0, MessageType::MsgHeartbeatResponse, 0))
        .expect("");
    msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgAppend);

    // 模拟2回应给1，修改了1中记录的指针
    //      再次收到MsgHeartbeatResponse将不再发送
    // Once we have an MsgAppResp, heartbeats no longer send MsgApp.
    let mut m = new_message(2, 0, MessageType::MsgAppendResponse, 0);
    m.set_index(msgs[0].get_index() + msgs[0].get_entries().len() as u64);
    sm.step(m).expect("");
    // Consume the message sent in response to MsgAppResp
    sm.read_messages();

    sm.step(new_message(2, 0, MessageType::MsgHeartbeatResponse, 0))
        .expect("");
    msgs = sm.read_messages();
    assert!(msgs.is_empty());
}

/// 测试 readonly 消息
///     2向1发送 readindex
///     1等待peer的回应，大多数回应后，将结果发送给2
///     2在内存中保存 read_state
// test_raft_frees_read_only_mem ensures raft will free read request from
// ReadOnly read_index_queue and pending_read_index map.
// related issue: https://github.com/coreos/etcd/issues/7571
#[test]
fn test_raft_frees_read_only_mem() {
    setup();
    let mut sm = new_test_raft(1, vec![1, 2], 5, 1, new_storage());
    sm.become_candidate();
    sm.become_leader();
    let last_index = sm.raft_log.last_index();
    sm.raft_log.commit_to(last_index);

    let ctx = "ctx";
    let vec_ctx = ctx.as_bytes().to_vec();

    // leader starts linearizable read request.
    // more info: raft dissertation 6.4, step 2.
    let m = new_message_with_entries(
        2,
        1,
        MessageType::MsgReadIndex,
        vec![new_entry(0, 0, Some(ctx))],
    );
    sm.step(m).expect("");
    let msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgHeartbeat);
    assert_eq!(msgs[0].get_context(), &vec_ctx[..]);
    assert_eq!(sm.read_only.read_index_queue.len(), 1);
    assert_eq!(sm.read_only.pending_read_index.len(), 1);
    assert!(sm.read_only.pending_read_index.contains_key(&vec_ctx));

    // heartbeat responses from majority of followers (1 in this case)
    // acknowledge the authority of the leader.
    // more info: raft dissertation 6.4, step 3.
    let mut m = new_message(2, 1, MessageType::MsgHeartbeatResponse, 0);
    m.set_context(vec_ctx.clone());
    sm.step(m).expect("");
    assert_eq!(sm.read_only.read_index_queue.len(), 0);
    assert_eq!(sm.read_only.pending_read_index.len(), 0);
    assert!(!sm.read_only.pending_read_index.contains_key(&vec_ctx));
}

/// node3处于pause状态: 发送了append后没有回应
///
///     即使有新的消息产生，处于pause状态的node，不会收到新的msg
///     一旦回应，后续消息会继续发送出去
///
/// 只构建了一个node；没有使用网络
// test_msg_append_response_wait_reset verifies the waitReset behavior of a leader
// MsgAppResp.
#[test]
fn test_msg_append_response_wait_reset() {
    let mut sm = new_test_raft(1, vec![1, 2, 3], 5, 1, new_storage());
    sm.become_candidate();
    sm.become_leader();

    // The new leader has just emitted a new Term 4 entry; consume those messages
    // from the outgoing queue.
    sm.bcast_append();
    sm.read_messages();

    // Node 2 acks the first entry, making it committed.
    let mut m = new_message(2, 0, MessageType::MsgAppendResponse, 0);
    m.set_index(1);
    sm.step(m).expect("");
    assert_eq!(sm.raft_log.committed, 1);
    // Also consume the MsgApp messages that update Commit on the followers.
    sm.read_messages();

    // A new command is now proposed on node 1.
    m = new_message(1, 0, MessageType::MsgPropose, 0);
    m.set_entries(RepeatedField::from_vec(vec![empty_entry(0, 0)]));
    sm.step(m).expect("");

    // The command is broadcast to all nodes not in the wait state.
    // Node 2 left the wait state due to its MsgAppResp, but node 3 is still waiting.
    let mut msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgAppend);
    assert_eq!(msgs[0].get_to(), 2);
    assert_eq!(msgs[0].get_entries().len(), 1);
    assert_eq!(msgs[0].get_entries()[0].get_index(), 2);

    // Now Node 3 acks the first entry. This releases the wait and entry 2 is sent.
    m = new_message(3, 0, MessageType::MsgAppendResponse, 0);
    m.set_index(1);
    sm.step(m).expect("");
    msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgAppend);
    assert_eq!(msgs[0].get_to(), 3);
    assert_eq!(msgs[0].get_entries().len(), 1);
    assert_eq!(msgs[0].get_entries()[0].get_index(), 2);
}

#[test]
fn test_recv_msg_request_vote() {
    test_recv_msg_request_vote_for_type(MessageType::MsgRequestVote);
}


/// 测试 message的term与raft 的term一致时
///     根据message中的log的新旧来判断是否接受vote
///     log的term更大，或者term相同，index更大
///
///     判断node1的状态，node2向1发送消息
fn test_recv_msg_request_vote_for_type(msg_type: MessageType) {
    // role，votemsg（log index， term），votefor，reject
    let mut tests = vec![
        // role，（log index，term），vote， reject

        // log 数据太旧
        (StateRole::Follower, 0, 0, INVALID_ID, true),
        (StateRole::Follower, 0, 1, INVALID_ID, true),
        (StateRole::Follower, 0, 2, INVALID_ID, true),

        (StateRole::Follower, 0, 3, INVALID_ID, false),
        (StateRole::Follower, 1, 0, INVALID_ID, true),
        (StateRole::Follower, 1, 1, INVALID_ID, true),
        (StateRole::Follower, 1, 2, INVALID_ID, true),
        (StateRole::Follower, 1, 3, INVALID_ID, false),
        (StateRole::Follower, 2, 0, INVALID_ID, true),
        (StateRole::Follower, 2, 1, INVALID_ID, true),
        // log的数据与raft一样
        (StateRole::Follower, 2, 2, INVALID_ID, false),
        (StateRole::Follower, 2, 3, INVALID_ID, false),
        (StateRole::Follower, 3, 0, INVALID_ID, true),
        (StateRole::Follower, 3, 1, INVALID_ID, true),
        (StateRole::Follower, 3, 2, INVALID_ID, false),
        (StateRole::Follower, 3, 3, INVALID_ID, false),
        (StateRole::Follower, 3, 2, 2, false),

        // 已经投票给其他人了; 因为本次测试的是 message的term与 raft term一致，因此会被拒绝
        (StateRole::Follower, 3, 2, 1, true),
        // 自己是leader会拒绝
        (StateRole::Leader, 3, 3, 1, true),

        // 自己的选票已经投给了自己
        (StateRole::PreCandidate, 3, 3, 1, true),
        (StateRole::Candidate, 3, 3, 1, true),
    ];

    for (j, (state, index, log_term, vote_for, w_reject)) in tests.drain(..).enumerate() {
        let raft_log = new_raft_log(
            &[empty_entry(0, 0), empty_entry(2, 1), empty_entry(2, 2)],
            3,
            0,
        );
        let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage());
        sm.state = state;
        sm.vote = vote_for;
        sm.raft_log = raft_log;

        let mut m = new_message(2, 0, msg_type, 0);
        m.set_index(index);
        m.set_log_term(log_term);
        // raft.Term is greater than or equal to raft.raftLog.lastTerm. In this
        // test we're only testing MsgVote responses when the campaigning node
        // has a different raft log compared to the recipient node.
        // Additionally we're verifying behaviour when the recipient node has
        // already given out its vote for its current term. We're not testing
        // what the recipient node does when receiving a message with a
        // different term number, so we simply initialize both term numbers to
        // be the same.

        // message 的term始终设置为与 raft 一致，这里只测试 log 内容 不同造成的影响
        let term = cmp::max(sm.raft_log.last_term(), log_term);
        m.set_term(term);
        sm.term = term;
        sm.step(m).expect("");

        let msgs = sm.read_messages();
        if msgs.len() != 1 {
            panic!("#{}: msgs count = {}, want 1", j, msgs.len());
        }
        if msgs[0].get_msg_type() != vote_resp_msg_type(msg_type) {
            panic!(
                "#{}: m.type = {:?}, want {:?}",
                j,
                msgs[0].get_msg_type(),
                vote_resp_msg_type(msg_type)
            );
        }
        if msgs[0].get_reject() != w_reject {
            panic!(
                "#{}: m.get_reject = {}, want {}",
                j,
                msgs[0].get_reject(),
                w_reject
            );
        }
    }
}

/// role 从一个状态向另一个状态转换，是否允许，以及结果
#[test]
fn test_state_transition() {
    // from，to，result，term，leaderid
    let mut tests = vec![
        (
            StateRole::Follower,
            StateRole::Follower,
            true,
            1,
            INVALID_ID,
        ),
        (
            StateRole::Follower,
            StateRole::PreCandidate,
            true,
            0,
            INVALID_ID,
        ),
        (
            StateRole::Follower,
            StateRole::Candidate,
            true,
            1,
            INVALID_ID,
        ),
        (StateRole::Follower, StateRole::Leader, false, 0, INVALID_ID),
        (
            StateRole::PreCandidate,
            StateRole::Follower,
            true,
            0,
            INVALID_ID,
        ),
        (
            StateRole::PreCandidate,
            StateRole::PreCandidate,
            true,
            0,
            INVALID_ID,
        ),
        (
            StateRole::PreCandidate,
            StateRole::Candidate,
            true,
            1,
            INVALID_ID,
        ),
        (StateRole::PreCandidate, StateRole::Leader, true, 0, 1),
        (
            StateRole::Candidate,
            StateRole::Follower,
            true,
            0,
            INVALID_ID,
        ),
        (
            StateRole::Candidate,
            StateRole::PreCandidate,
            true,
            0,
            INVALID_ID,
        ),
        // 每次变成candidate，term都会 +1
        (
            StateRole::Candidate,
            StateRole::Candidate,
            true,
            1,
            INVALID_ID,
        ),
        (StateRole::Candidate, StateRole::Leader, true, 0, 1),
        (StateRole::Leader, StateRole::Follower, true, 1, INVALID_ID),
        (
            StateRole::Leader,
            StateRole::PreCandidate,
            false,
            0,
            INVALID_ID,
        ),
        (
            StateRole::Leader,
            StateRole::Candidate,
            false,
            1,
            INVALID_ID,
        ),
        // 此时term不变
        (StateRole::Leader, StateRole::Leader, true, 0, 1),
    ];
    for (i, (from, to, wallow, wterm, wlead)) in tests.drain(..).enumerate() {
        let sm: &mut Raft<MemStorage> = &mut new_test_raft(1, vec![1], 10, 1, new_storage());
        sm.state = from;

        let res = panic::catch_unwind(AssertUnwindSafe(|| match to {
            // 变成follower时，才会设置term
            StateRole::Follower => sm.become_follower(wterm, wlead),
            StateRole::PreCandidate => sm.become_pre_candidate(),
            // term + 1
            StateRole::Candidate => sm.become_candidate(),
            // follower不能直接变成leader
            StateRole::Leader => sm.become_leader(),
        }));
        if res.is_ok() ^ wallow {
            panic!("#{}: allow = {}, want {}", i, res.is_ok(), wallow);
        }
        if res.is_err() {
            continue;
        }

        if sm.term != wterm {
            panic!("#{}: term = {}, want {}", i, sm.term, wterm);
        }
        if sm.leader_id != wlead {
            panic!("#{}: lead = {}, want {}", i, sm.leader_id, wlead);
        }
    }
}

/// 从标题看不出执行内容
///     实际还是测试状态转换，node1从不同状态，都收到 node2的选举消息的反应
#[test]
fn test_all_server_stepdown() {
    let mut tests = vec![
        // node1 role，new role，term，last index
        (StateRole::Follower, StateRole::Follower, 3, 0),
        (StateRole::PreCandidate, StateRole::Follower, 3, 0),
        (StateRole::Candidate, StateRole::Follower, 3, 0),
        (StateRole::Leader, StateRole::Follower, 3, 1),
    ];

    let tmsg_types = vec![MessageType::MsgRequestVote, MessageType::MsgAppend];
    let tterm = 3u64;

    // 只启动 node1；
    //      node2向 node1发送 vote、append
    //  node1 收到的消息 term为3，node1立即更新到term3，并 执行 become_follower

    for (i, (state, wstate, wterm, windex)) in tests.drain(..).enumerate() {
        let mut sm = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
        match state {
            StateRole::Follower => sm.become_follower(1, INVALID_ID),
            StateRole::PreCandidate => sm.become_pre_candidate(),
            StateRole::Candidate => sm.become_candidate(),
            StateRole::Leader => {
                sm.become_candidate();
                sm.become_leader();
            }
        }

        for (j, &msg_type) in tmsg_types.iter().enumerate() {
            let mut m = new_message(2, 0, msg_type, 0);
            m.set_term(tterm);
            m.set_log_term(tterm);
            sm.step(m).expect("");

            if sm.state != wstate {
                panic!("{}.{} state = {:?}, want {:?}", i, j, sm.state, wstate);
            }
            if sm.term != wterm {
                panic!("{}.{} term = {}, want {}", i, j, sm.term, wterm);
            }
            if sm.raft_log.last_index() != windex {
                panic!(
                    "{}.{} index = {}, want {}",
                    i,
                    j,
                    sm.raft_log.last_index(),
                    windex
                );
            }
            let entry_count = sm.raft_log.all_entries().len() as u64;
            if entry_count != windex {
                panic!("{}.{} ents count = {}, want {}", i, j, entry_count, windex);
            }
            let wlead = if msg_type == MessageType::MsgRequestVote {
                INVALID_ID
            } else {
                2
            };
            if sm.leader_id != wlead {
                panic!("{}, sm.lead = {}, want {}", i, sm.leader_id, INVALID_ID);
            }
        }
    }
}

#[test]
fn test_candidate_reset_term_msg_heartbeat() {
    test_candidate_reset_term(MessageType::MsgHeartbeat)
}

#[test]
fn test_candidate_reset_term_msg_append() {
    test_candidate_reset_term(MessageType::MsgAppend)
}

/// 三个节点，隔离3
///     模拟3，出现超时，成为candidate
///     网络恢复后，3从1分别收到 append，或者beat消息；都将导致 3 从1获取到了最新数据
// test_candidate_reset_term tests when a candidate receives a
// MsgHeartbeat or MsgAppend from leader, "step" resets the term
// with leader's and reverts back to follower.
fn test_candidate_reset_term(message_type: MessageType) {
    let a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)]);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Follower);

    // isolate 3 and increase term in rest
    nt.isolate(3);
    nt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Follower);

    // trigger campaign in isolated c
    nt.peers
        .get_mut(&3)
        .unwrap()
        .reset_randomized_election_timeout();
    let timeout = nt.peers[&3].get_randomized_election_timeout();
    for _ in 0..timeout {
        nt.peers.get_mut(&3).unwrap().tick();
    }

    assert_eq!(nt.peers[&3].state, StateRole::Candidate);

    nt.recover();

    // leader sends to isolated candidate
    // and expects candidate to revert to follower
    let mut msg = new_message(1, 3, message_type, 0);
    msg.set_term(nt.peers[&1].term);
    nt.send(vec![msg]);

    assert_eq!(nt.peers[&3].state, StateRole::Follower);

    // follower c term is reset with leader's
    assert_eq!(
        nt.peers[&3].term, nt.peers[&1].term,
        "follower term expected same term as leader's {}, got {}",
        nt.peers[&1].term, nt.peers[&3].term,
    )
}

/// 租约方式，leader定期检查（给自己发送MsgCheckQuorum）
///     记录的progress是否是active的
#[test]
fn test_leader_stepdown_when_quorum_active() {
    let mut sm = new_test_raft(1, vec![1, 2, 3], 5, 1, new_storage());
    sm.check_quorum = true;
    sm.become_candidate();
    sm.become_leader();

    for _ in 0..(sm.get_election_timeout() + 1) {
        let mut m = new_message(2, 0, MessageType::MsgHeartbeatResponse, 0);
        m.set_term(sm.term);
        sm.step(m).expect("");
        sm.tick();
    }

    assert_eq!(sm.state, StateRole::Leader);
}

/// 超时没有收到回应，leader释放租约，变成follower
#[test]
fn test_leader_stepdown_when_quorum_lost() {
    let mut sm = new_test_raft(1, vec![1, 2, 3], 5, 1, new_storage());

    sm.check_quorum = true;

    sm.become_candidate();
    sm.become_leader();

    for _ in 0..(sm.get_election_timeout() + 1) {
        sm.tick();
    }

    assert_eq!(sm.state, StateRole::Follower);
}

/// 3node，1成为leader
///     3期望成为leader：2检测超时，发现还在租约期间，仍然拥护1；
///                     2超时后，认可3成为leader
#[test]
fn test_leader_superseding_with_check_quorum() {
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());

    a.check_quorum = true;
    b.check_quorum = true;
    c.check_quorum = true;

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)]);

    let b_election_timeout = nt.peers[&2].get_election_timeout();

    // prevent campaigning from b
    nt.peers
        .get_mut(&2)
        .unwrap()
        .set_randomized_election_timeout(b_election_timeout + 1);

    // 此部分可有可无？
    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&3].state, StateRole::Follower);

    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // Peer b rejected c's vote since its electionElapsed had not reached to electionTimeout
    assert_eq!(nt.peers[&3].state, StateRole::Candidate);

    // Letting b's electionElapsed reach to electionTimeout
    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }

    // 再次发送选举请求
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&3].state, StateRole::Leader);
}

/// 测试租约期间，选举状况
///
/// node1 是leader，然后 1、2分别超时；3提出成为leader
///     因为leader感知超时要2个周期，在收到3的 vote 请求后，1认为自己还处于租约期间，拒绝
///     2因为已经超过租约，同意3成为leader
///     1 收到3的append消息，term更大，因此成为followr
#[test]
fn test_leader_election_with_check_quorum() {
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());

    a.check_quorum = true;
    b.check_quorum = true;
    c.check_quorum = true;

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)]);

    // we can not let system choosing the value of randomizedElectionTimeout
    // otherwise it will introduce some uncertainty into this test case
    // we need to ensure randomizedElectionTimeout > electionTimeout here
    let a_election_timeout = nt.peers[&1].get_election_timeout();
    let b_election_timeout = nt.peers[&2].get_election_timeout();

    //
    // 设置这些，是为了防止 2 在tick超时后，发现达到了random的值，就成为 candidate
    //
    nt.peers
        .get_mut(&1)
        .unwrap()
        .set_randomized_election_timeout(a_election_timeout + 1);
    nt.peers
        .get_mut(&2)
        .unwrap()
        .set_randomized_election_timeout(b_election_timeout + 2);

    // Immediately after creation, votes are cast regardless of the election timeout

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&3].state, StateRole::Follower);

    // need to reset randomizedElectionTimeout larger than electionTimeout again,
    // because the value might be reset to electionTimeout since the last state changes
    let a_election_timeout = nt.peers[&1].get_election_timeout();
    let b_election_timeout = nt.peers[&2].get_election_timeout();

    // 状态变更后，random值可能被重置了，这里重新改回来
    //  实际上，之前那次是可以不用设置的？
    nt.peers
        .get_mut(&1)
        .unwrap()
        .set_randomized_election_timeout(a_election_timeout + 1);
    nt.peers
        .get_mut(&2)
        .unwrap()
        .set_randomized_election_timeout(b_election_timeout + 2);

    // 1 是leader，超时后才会重置 recent_active；要等到下一个周期才会发现自己超时了，即达到2个超时周期，才会超时

    // 2 是follower，多次tick之后，1的leader租约已经超时，可以接受其他leader了
    for _ in 0..a_election_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }
    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }

    // 3成为leader
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Leader);
}

/// 即使leader自身还在租约之内，仍然可能被candidate打断（因为其有更高的term，而且回复的消息，是之前leader发送给他的）
///             还可参见：test_disruptive_follower，专门讲解此场景
///     出现的概率应该比较低？是否有解决方案？
///
///     1是leader；隔离1,
///     3尝试成为leader，但是被卡主：2还在1的租约时间内
///     3再次尝试成为leader，此时term已经很大了，但是被卡主，2还在1的租约时间内
///
///     网络恢复，1发送beat给3，3回应请求给1
///     1 收到term更大的请求，而且消息类型是
///
// test_free_stuck_candidate_with_check_quorum ensures that a candidate with a higher term
// can disrupt the leader even if the leader still "officially" holds the lease, The
// leader is expected to step down and adopt the candidate's term
#[test]
fn test_free_stuck_candidate_with_check_quorum() {
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());

    a.check_quorum = true;
    b.check_quorum = true;
    c.check_quorum = true;

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)]);

    // we can not let system choosing the value of randomizedElectionTimeout
    // otherwise it will introduce some uncertainty into this test case
    // we need to ensure randomizedElectionTimeout > electionTimeout here
    let b_election_timeout = nt.peers[&2].get_election_timeout();
    nt.peers
        .get_mut(&2)
        .unwrap()
        .set_randomized_election_timeout(b_election_timeout + 1);

    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    nt.isolate(1);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Candidate);
    assert_eq!(nt.peers[&3].term, &nt.peers[&2].term + 1);

    // Vote again for safety
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Candidate);
    assert_eq!(nt.peers[&3].term, &nt.peers[&2].term + 2);

    nt.recover();
    let mut msg = new_message(1, 3, MessageType::MsgHeartbeat, 0);
    msg.set_term(nt.peers[&1].term);
    nt.send(vec![msg]);

    // Disrupt the leader so that the stuck peer is freed
    assert_eq!(nt.peers[&1].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].term, nt.peers[&1].term);

    // Vote again, should become leader this time
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&3].state, StateRole::Leader);
}

/// 2个节点，2 不再 peer set中；但是 1认为2还在set中
///     1成为leader；2成为follower
#[test]
fn test_non_promotable_voter_which_check_quorum() {
    let mut a = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    let mut b = new_test_raft(2, vec![1], 10, 1, new_storage());

    a.check_quorum = true;
    b.check_quorum = true;

    // 这里传入已经配置好的 raft，因此不会再次设置 raft的 peer set
    let mut nt = Network::new(vec![Some(a), Some(b)]);

    // we can not let system choosing the value of randomizedElectionTimeout
    // otherwise it will introduce some uncertainty into this test case
    // we need to ensure randomizedElectionTimeout > electionTimeout here
    let b_election_timeout = nt.peers[&2].get_election_timeout();
    nt.peers
        .get_mut(&2)
        .unwrap()
        .set_randomized_election_timeout(b_election_timeout + 1);

    // Need to remove 2 again to make it a non-promotable node since newNetwork
    // overwritten some internal states
    nt.peers.get_mut(&2).unwrap().mut_prs().remove(2).unwrap();

    assert_eq!(nt.peers[&2].promotable(), false);

    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&2].leader_id, 1);
}

/// 测试: 隔离的follower，结果类似于：test_free_stuck_candidate_with_check_quorum
///     1是leader；3发生超时，成为candidate
///     消息队列中残留消息，因此3收到了1的心跳包；因为消息的term较小，因此3返回了 appendrespons消息
///     1收到此消息之后，发现有更大的term，而且消息类型不是vote；自己就变成follower
///
/// 这个消息是否不用回应，效果更好？
///
/// `test_disruptive_follower` tests isolated follower,
/// with slow network incoming from leader, election times out
/// to become a candidate with an increased term. Then, the
/// candiate's response to late leader heartbeat forces the leader
/// to step down.
#[test]
fn test_disruptive_follower() {
    let mut n1 = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let mut n2 = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let mut n3 = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());

    n1.check_quorum = true;
    n2.check_quorum = true;
    n3.check_quorum = true;

    n1.become_follower(1, INVALID_ID);
    n2.become_follower(1, INVALID_ID);
    n3.become_follower(1, INVALID_ID);

    let mut nt = Network::new(vec![Some(n1), Some(n2), Some(n3)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // check state
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Follower);

    // etcd server "advanceTicksForElection" on restart;
    // this is to expedite campaign trigger when given larger
    // election timeouts (e.g. multi-datacenter deploy)
    // Or leader messages are being delayed while ticks elapse

    // 确保3超时，但是3不会成为candidate
    let timeout = nt.peers[&3].get_election_timeout();
    nt.peers
        .get_mut(&3)
        .unwrap()
        .set_randomized_election_timeout(timeout + 2);
    let timeout = nt.peers[&3].get_randomized_election_timeout();
    for _ in 0..timeout - 1 {
        nt.peers.get_mut(&3).unwrap().tick();
    }

    // ideally, before last election tick elapses,
    // the follower n3 receives "pb.MsgApp" or "pb.MsgHeartbeat"
    // from leader n1, and then resets its "electionElapsed"
    // however, last tick may elapse before receiving any
    // messages from leader, thus triggering campaign
    nt.peers.get_mut(&3).unwrap().tick();

    // n1 is still leader yet
    // while its heartbeat to candidate n3 is being delayed
    // check state
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Candidate);

    // check term
    // n1.Term == 2
    // n2.Term == 2
    // n3.Term == 3
    assert_eq!(nt.peers[&1].term, 2);
    assert_eq!(nt.peers[&2].term, 2);
    assert_eq!(nt.peers[&3].term, 3);

    // while outgoing vote requests are still queued in n3,
    // leader heartbeat finally arrives at candidate n3
    // however, due to delayed network from leader, leader
    // heartbeat was sent with lower term than candidate's
    let mut msg = new_message(1, 3, MessageType::MsgHeartbeat, 0);
    msg.set_term(nt.peers[&1].term);
    nt.send(vec![msg]);

    // then candidate n3 responds with "pb.MsgAppResp" of higher term
    // and leader steps down from a message with higher term
    // this is to disrupt the current leader, so that candidate
    // with higher term can be freed with following election

    // check state
    assert_eq!(nt.peers[&1].state, StateRole::Follower);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Candidate);

    // check term
    // n1.Term == 3
    // n2.Term == 2
    // n3.Term == 3
    assert_eq!(nt.peers[&1].term, 3);
    assert_eq!(nt.peers[&2].term, 2);
    assert_eq!(nt.peers[&3].term, 3);
}

/// `test_disruptive_follower_pre_vote` tests isolated follower,
/// with slow network incoming from leader, election times out
/// to become a pre-candidate with less log than current leader.
/// Then pre-vote phase prevents this isolated node from forcing
/// current leader to step down, thus less disruptions.
#[test]
fn test_disruptive_follower_pre_vote() {
    let mut n1 = new_test_raft_with_prevote(1, vec![1, 2, 3], 10, 1, new_storage(), true);
    let mut n2 = new_test_raft_with_prevote(2, vec![1, 2, 3], 10, 1, new_storage(), true);
    let mut n3 = new_test_raft_with_prevote(3, vec![1, 2, 3], 10, 1, new_storage(), true);

    n1.check_quorum = true;
    n2.check_quorum = true;
    n3.check_quorum = true;

    n1.become_follower(1, INVALID_ID);
    n2.become_follower(1, INVALID_ID);
    n3.become_follower(1, INVALID_ID);

    let mut nt = Network::new(vec![Some(n1), Some(n2), Some(n3)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // check state
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Follower);

    nt.isolate(3);
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    nt.recover();
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // check state
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::PreCandidate);

    // check term
    // n1.Term == 2
    // n2.Term == 2
    // n3.Term == 2
    assert_eq!(nt.peers[&1].term, 2);
    assert_eq!(nt.peers[&2].term, 2);
    assert_eq!(nt.peers[&3].term, 2);

    // delayed leader heartbeat does not force current leader to step down
    let mut msg = new_message(1, 3, MessageType::MsgHeartbeat, 0);
    msg.set_term(nt.peers[&1].term);
    nt.send(vec![msg]);
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
}

/// safe 模式下访问 readonly
#[test]
fn test_read_only_option_safe() {
    let a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)]);

    // we can not let system choose the value of randomizedElectionTimeout
    // otherwise it will introduce some uncertainty into this test case
    // we need to ensure randomizedElectionTimeout > electionTimeout here
    let b_election_timeout = nt.peers[&2].get_election_timeout();
    nt.peers
        .get_mut(&2)
        .unwrap()
        .set_randomized_election_timeout(b_election_timeout + 1);

    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);

    let mut tests = vec![
        // nodeid，请求count，index，ctx
        (1, 10, 11, "ctx1"),
        (2, 10, 21, "ctx2"),
        (3, 10, 31, "ctx3"),
        (1, 10, 41, "ctx4"),
        (2, 10, 51, "ctx5"),
        (3, 10, 61, "ctx6"),
    ];

    for (i, (id, proposals, wri, wctx)) in tests.drain(..).enumerate() {
        // leader接收10个消息
        for _ in 0..proposals {
            nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
        }

        let e = new_entry(0, 0, Some(wctx));
        nt.send(vec![new_message_with_entries(
            id,
            id,
            MessageType::MsgReadIndex,
            vec![e],
        )]);

        let read_states: Vec<ReadState> = nt
            .peers
            .get_mut(&id)
            .unwrap()
            .read_states
            .drain(..)
            .collect();
        if read_states.is_empty() {
            panic!("#{}: read_states is empty, want non-empty", i);
        }
        let rs = &read_states[0];
        if rs.index != wri {
            panic!("#{}: read_index = {}, want {}", i, rs.index, wri)
        }
        let vec_wctx = wctx.as_bytes().to_vec();
        if rs.request_ctx != vec_wctx {
            panic!(
                "#{}: request_ctx = {:?}, want {:?}",
                i, rs.request_ctx, vec_wctx
            )
        }
    }
}

// lease方式下，访问 readonly
#[test]
fn test_read_only_option_lease() {
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());
    a.read_only.option = ReadOnlyOption::LeaseBased;
    b.read_only.option = ReadOnlyOption::LeaseBased;
    c.read_only.option = ReadOnlyOption::LeaseBased;
    a.check_quorum = true;
    b.check_quorum = true;
    c.check_quorum = true;

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)]);

    // we can not let system choose the value of randomizedElectionTimeout
    // otherwise it will introduce some uncertainty into this test case
    // we need to ensure randomizedElectionTimeout > electionTimeout here
    let b_election_timeout = nt.peers[&2].get_election_timeout();
    nt.peers
        .get_mut(&2)
        .unwrap()
        .set_randomized_election_timeout(b_election_timeout + 1);

    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);

    let mut tests = vec![
        (1, 10, 11, "ctx1"),
        (2, 10, 21, "ctx2"),
        (3, 10, 31, "ctx3"),
        (1, 10, 41, "ctx4"),
        (2, 10, 51, "ctx5"),
        (3, 10, 61, "ctx6"),
    ];

    for (i, (id, proposals, wri, wctx)) in tests.drain(..).enumerate() {
        for _ in 0..proposals {
            nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
        }

        let e = new_entry(0, 0, Some(wctx));
        nt.send(vec![new_message_with_entries(
            id,
            id,
            MessageType::MsgReadIndex,
            vec![e],
        )]);

        let read_states: Vec<ReadState> = nt
            .peers
            .get_mut(&id)
            .unwrap()
            .read_states
            .drain(..)
            .collect();
        if read_states.is_empty() {
            panic!("#{}: read_states is empty, want non-empty", i);
        }
        let rs = &read_states[0];
        if rs.index != wri {
            panic!("#{}: read_index = {}, want {}", i, rs.index, wri);
        }
        let vec_wctx = wctx.as_bytes().to_vec();
        if rs.request_ctx != vec_wctx {
            panic!(
                "#{}: request_ctx = {:?}, want {:?}",
                i, rs.request_ctx, vec_wctx
            );
        }
    }
}

/// 不使用check_quorum时，readonly 为 lease
///
///     2 将请求发送给 2，将转发给leader 1，然后收到1的回应
///     将直接获得结果，并且 index为 INVALID_ID
#[test]
fn test_read_only_option_lease_without_check_quorum() {
    let mut a = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
    let mut b = new_test_raft(2, vec![1, 2, 3], 10, 1, new_storage());
    let mut c = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());
    a.read_only.option = ReadOnlyOption::LeaseBased;
    b.read_only.option = ReadOnlyOption::LeaseBased;
    c.read_only.option = ReadOnlyOption::LeaseBased;

    let mut nt = Network::new(vec![Some(a), Some(b), Some(c)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    let ctx = "ctx1";
    let e = new_entry(0, 0, Some(ctx));
    nt.send(vec![new_message_with_entries(
        2,
        2,
        MessageType::MsgReadIndex,
        vec![e],
    )]);

    let read_states = &nt.peers[&2].read_states;
    assert!(!read_states.is_empty());
    let rs = &read_states[0];
    assert_eq!(rs.index, INVALID_ID);
    let vec_ctx = ctx.as_bytes().to_vec();
    assert_eq!(rs.request_ctx, vec_ctx);
}

/// 还未comit之前，leader直接忽略readindex消息
///
///     关闭append消息，请求无法发送到其他node；此时无法处理 readindex 消息
///     网络恢复后，leader可以接受readindex
// `test_read_only_for_new_leader` ensures that a leader only accepts MsgReadIndex message
// when it commits at least one log entry at it term.
#[test]
fn test_read_only_for_new_leader() {
    let heartbeat_ticks = 1;
    let node_configs = vec![(1, 1, 1, 0), (2, 2, 2, 2), (3, 2, 2, 2)];
    let mut peers = vec![];

    // 模拟应用，设置apply、commit
    for (id, committed, applied, compact_index) in node_configs {
        let mut cfg = new_test_config(id, vec![1, 2, 3], 10, heartbeat_ticks);
        cfg.applied = applied;
        let storage = new_storage();
        let entries = vec![empty_entry(1, 1), empty_entry(1, 2)];
        storage.wl().append(&entries).unwrap();
        let mut hs = HardState::new();
        hs.set_term(1);
        hs.set_commit(committed);
        storage.wl().set_hardstate(hs);
        if compact_index != 0 {
            storage.wl().compact(compact_index).unwrap();
        }
        let i = Interface::new(Raft::new(&cfg, storage));
        peers.push(Some(i));
    }
    let mut nt = Network::new(peers);

    // Drop MsgAppend to forbid peer 1 to commit any log entry at its term
    // after it becomes leader.
    nt.ignore(MessageType::MsgAppend);
    // Force peer 1 to become leader
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&1].state, StateRole::Leader);

    // Ensure peer 1 drops read only request.
    let windex = 4;
    let wctx = "ctx";
    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgReadIndex,
        vec![new_entry(0, 0, Some(wctx))],
    )]);
    assert_eq!(nt.peers[&1].read_states.len(), 0);

    nt.recover();

    // 触发超时，向各个node发送beat消息
    //      但是因为不是通过network发送出去的，因此消息都积攒在 node1的队列中
    // Force peer 1 to commit a log entry at its term.
    for _ in 0..heartbeat_ticks {
        nt.peers.get_mut(&1).unwrap().tick();
    }

    // 这里会释放上述的beat消息，并发送propose消息
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    assert_eq!(nt.peers[&1].raft_log.committed, 4);
    assert_eq!(
        nt.peers[&1]
            .raft_log
            .term(nt.peers[&1].raft_log.committed)
            .unwrap_or(0),
        nt.peers[&1].term
    );

    // Ensure peer 1 accepts read only request after it commits a entry at its term.
    nt.send(vec![new_message_with_entries(
        1,
        1,
        MessageType::MsgReadIndex,
        vec![new_entry(0, 0, Some(wctx))],
    )]);
    let read_states: Vec<ReadState> = nt
        .peers
        .get_mut(&1)
        .unwrap()
        .read_states
        .drain(..)
        .collect();
    assert_eq!(read_states.len(), 1);
    let rs = &read_states[0];
    assert_eq!(rs.index, windex);
    assert_eq!(rs.request_ctx, wctx.as_bytes().to_vec());
}

/// leader发送append，收到follower的回应；
///     follower从probe到repliac；回应消息的有效性，可能不会产生任何效果（没有增加progress的match值）
///
///     leader收到的回应，先与自己记录的progress的数据指针比较；
///         reject的位置，必然是 progress的 next_idx - 1，这个是数据发送的起始位置
///         因为数据发送之后，progress的 next_idx 并不改变；每次都是从这个位置发送一系列日志出去
///
#[test]
fn test_leader_append_response() {
    // initial progress: match = 0; next = 3
    let mut tests = vec![

        // node上已经包含的数据是：1, 2, 3(选举成功)
        // log index， reject | progress match， nex | resp index，reps commit
        // prev log不匹配
        (3, true, 0, 3, 0, 0, 0), // stale resp; no replies
        // next_idx = reject, 发送append，从 next_idx - 1 开始
        (2, true, 0, 2, 1, 1, 0), // denied resp; leader does not commit; descrease next and send

        // probing msg
        // probe阶段数据成功匹配，变成replicat，将index3发送出去，增加progress的next_idx；回应消息包括2个，->2, ->3
        (2, false, 2, 4, 2, 2, 2), // accept resp; leader commits; broadcast with commit index

        // 回应消息是match的，但是没有增加match的位置，因此不会触发新的resp消息
        (0, false, 0, 3, 0, 0, 0),
    ];

    for (i, (index, reject, wmatch, wnext, wmsg_num, windex, wcommitted)) in
        tests.drain(..).enumerate()
    {
        // 1成为leader，并包含一些数据；消息不发送到其他node，直接清理

        // sm term is 1 after it becomes the leader.
        // thus the last log term must be 1 to be committed.
        let mut sm = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
        sm.raft_log = new_raft_log(&[empty_entry(0, 1), empty_entry(1, 2)], 3, 0);
        sm.become_candidate();
        sm.become_leader();
        sm.read_messages();

        // 模拟node2的回应，选择是否设置 reject
        let mut m = new_message(2, 0, MessageType::MsgAppendResponse, 0);
        m.set_index(index);
        // 这里不测试term，消息的term与leader一致
        m.set_term(sm.term);
        m.set_reject(reject);
        m.set_reject_hint(index);
        sm.step(m).expect("");

        // 检查node1中记录的2 progress的状态
        if sm.prs().voters()[&2].matched != wmatch {
            panic!(
                "#{}: match = {}, want {}",
                i,
                sm.prs().voters()[&2].matched,
                wmatch
            );
        }
        if sm.prs().voters()[&2].next_idx != wnext {
            panic!(
                "#{}: next = {}, want {}",
                i,
                sm.prs().voters()[&2].next_idx,
                wnext
            );
        }

        // 检查1对2的每个回应消息中，term、index是否一致
        let mut msgs = sm.read_messages();
        if msgs.len() != wmsg_num {
            panic!("#{} msg_num = {}, want {}", i, msgs.len(), wmsg_num);
        }
        for (j, msg) in msgs.drain(..).enumerate() {
            if msg.get_index() != windex {
                panic!("#{}.{} index = {}, want {}", i, j, msg.get_index(), windex);
            }
            if msg.get_commit() != wcommitted {
                panic!(
                    "#{}.{} commit = {}, want {}",
                    i,
                    j,
                    msg.get_commit(),
                    wcommitted
                );
            }
        }
    }
}

/// leader 发送heartbeat
///
///     消息中不会设置 log term、log index
///     消息中记录的commit消息，是 min(leader commit，progress match)
// When the leader receives a heartbeat tick, it should
// send a MsgApp with m.Index = 0, m.LogTerm=0 and empty entries.
#[test]
fn test_bcast_beat() {

    // 使用快照构建 raft； peer set是写在快照里边的，config中不能再设置
    let offset = 1000u64;
    // make a state machine with log.offset = 1000
    let s = new_snapshot(offset, 1, vec![1, 2, 3]);
    let store = new_storage();
    store.wl().apply_snapshot(s).expect("");
    let mut sm = new_test_raft(1, vec![], 10, 1, store);
    sm.term = 1;

    sm.become_candidate();
    sm.become_leader();
    for i in 0..10 {
        sm.append_entry(&mut [empty_entry(0, i as u64 + 1)]);
    }

    // 设置progress中，数据指针的位置
    // slow follower
    let mut_pr = |sm: &mut Interface, n, matched, next_idx| {
        let m = sm.mut_prs().get_mut(n).unwrap();
        m.matched = matched;
        m.next_idx = next_idx;
    };
    // slow follower
    mut_pr(&mut sm, 2, 5, 6);
    // normal follower
    let last_index = sm.raft_log.last_index();
    mut_pr(&mut sm, 3, last_index, last_index + 1);

    sm.step(new_message(0, 0, MessageType::MsgBeat, 0))
        .expect("");
    let mut msgs = sm.read_messages();
    assert_eq!(msgs.len(), 2);
    let mut want_commit_map = HashMap::new();
    want_commit_map.insert(
        2,
        cmp::min(sm.raft_log.committed, sm.prs().voters()[&2].matched),
    );
    want_commit_map.insert(
        3,
        cmp::min(sm.raft_log.committed, sm.prs().voters()[&3].matched),
    );
    for (i, m) in msgs.drain(..).enumerate() {
        if m.get_msg_type() != MessageType::MsgHeartbeat {
            panic!(
                "#{}: type = {:?}, want = {:?}",
                i,
                m.get_msg_type(),
                MessageType::MsgHeartbeat
            );
        }
        if m.get_index() != 0 {
            panic!("#{}: prev_index = {}, want {}", i, m.get_index(), 0);
        }
        if m.get_log_term() != 0 {
            panic!("#{}: prev_term = {}, want {}", i, m.get_log_term(), 0);
        }
        if want_commit_map[&m.get_to()] == 0 {
            panic!("#{}: unexpected to {}", i, m.get_to())
        } else {
            if m.get_commit() != want_commit_map[&m.get_to()] {
                panic!(
                    "#{}: commit = {}, want {}",
                    i,
                    m.get_commit(),
                    want_commit_map[&m.get_to()]
                );
            }
            want_commit_map.remove(&m.get_to());
        }
        if !m.get_entries().is_empty() {
            panic!("#{}: entries count = {}, want 0", i, m.get_entries().len());
        }
    }
}

/// 即使收到本地的 MessageType::MsgBeat，只有leader会发送beta消息出去，其他角色自己讲忽略
///
// tests the output of the statemachine when receiving MsgBeat
#[test]
fn test_recv_msg_beat() {
    let mut tests = vec![
        (StateRole::Leader, 2),
        // candidate and follower should ignore MsgBeat
        (StateRole::Candidate, 0),
        (StateRole::Follower, 0),
    ];

    for (i, (state, w_msg)) in tests.drain(..).enumerate() {
        let mut sm = new_test_raft(1, vec![1, 2, 3], 10, 1, new_storage());
        sm.raft_log = new_raft_log(&[empty_entry(0, 1), empty_entry(1, 2)], 0, 0);
        sm.term = 1;
        sm.state = state;
        sm.step(new_message(1, 1, MessageType::MsgBeat, 0))
            .expect("");

        let msgs = sm.read_messages();
        if msgs.len() != w_msg {
            panic!("#{}: msg count = {}, want {}", i, msgs.len(), w_msg);
        }
        for m in msgs {
            if m.get_msg_type() != MessageType::MsgHeartbeat {
                panic!(
                    "#{}: msg.type = {:?}, want {:?}",
                    i,
                    m.get_msg_type(),
                    MessageType::MsgHeartbeat
                );
            }
        }
    }
}

#[test]
fn test_leader_increase_next() {
    let previous_ents = vec![empty_entry(1, 1), empty_entry(1, 2), empty_entry(1, 3)];
    let mut tests = vec![
        // state replicate; optimistically increase next
        // previous entries + noop entry + propose + 1
        (
            ProgressState::Replicate,
            2,
            // replica状态下，将自己所有消息发送出去，再设置 next =（old msg 3， 选举成功，新写入消息）+ 1， optimistically
            previous_ents.len() as u64 + 1 + 1 + 1,
        ),

        // probe状态，不会修改 next指针
        // state probe, not optimistically increase next
        (ProgressState::Probe, 2, 2),
    ];
    for (i, (state, next_idx, wnext)) in tests.drain(..).enumerate() {
        let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
        sm.raft_log.append(&previous_ents);
        sm.become_candidate();
        sm.become_leader();
        sm.mut_prs().get_mut(2).unwrap().state = state;
        sm.mut_prs().get_mut(2).unwrap().next_idx = next_idx;
        sm.step(new_message(1, 1, MessageType::MsgPropose, 1))
            .expect("");

        if sm.prs().voters()[&2].next_idx != wnext {
            panic!(
                "#{}: next = {}, want {}",
                i,
                sm.prs().voters()[&2].next_idx,
                wnext
            );
        }
    }
}

/// leader在 follower处于probe状态下，只会发送一次消息
///
///        除非收到回应，或者本地检测到beat超时
#[test]
fn test_send_append_for_progress_probe() {
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    r.become_candidate();
    r.become_leader();
    r.read_messages();
    r.mut_prs().get_mut(2).unwrap().become_probe();

    // each round is a heartbeat
    for i in 0..3 {
        if i == 0 {
            // we expect that raft will only send out one msgAPP on the first
            // loop. After that, the follower is paused until a heartbeat response is
            // received.
            r.append_entry(&mut [new_entry(0, 0, SOME_DATA)]);
            do_send_append(&mut r, 2);
            let msg = r.read_messages();
            assert_eq!(msg.len(), 1);
            assert_eq!(msg[0].get_index(), 0);
        }

        assert!(r.prs().voters()[&2].paused);
        for _ in 0..10 {
            r.append_entry(&mut [new_entry(0, 0, SOME_DATA)]);
            do_send_append(&mut r, 2);
            assert_eq!(r.read_messages().len(), 0);
        }

        // beat超时，将重置 probe的pause状态
        // do a heartbeat
        for _ in 0..r.get_heartbeat_timeout() {
            r.step(new_message(1, 1, MessageType::MsgBeat, 0))
                .expect("");
        }
        assert!(r.prs().voters()[&2].paused);

        // 1 向 2发送一个beat消息
        // consume the heartbeat
        let msg = r.read_messages();
        assert_eq!(msg.len(), 1);
        assert_eq!(msg[0].get_msg_type(), MessageType::MsgHeartbeat);
    }

    // 收到2的回应之后，一次性将所有消息全部发送出去， 10 * 3 + 1（i=0时） + 1（选举消息）
    // a heartbeat response will allow another message to be sent
    r.step(new_message(2, 1, MessageType::MsgHeartbeatResponse, 0))
        .expect("");
    let msg = r.read_messages();
    assert_eq!(msg.len(), 1);
    assert_eq!(msg[0].get_index(), 0);
    assert!(r.prs().voters()[&2].paused);
}

#[test]
fn test_send_append_for_progress_replicate() {
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    r.become_candidate();
    r.become_leader();
    r.read_messages();
    r.mut_prs().get_mut(2).unwrap().become_replicate();

    for _ in 0..10 {
        r.append_entry(&mut [new_entry(0, 0, SOME_DATA)]);
        do_send_append(&mut r, 2);
        assert_eq!(r.read_messages().len(), 1);
    }
}

#[test]
fn test_send_append_for_progress_snapshot() {
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    r.become_candidate();
    r.become_leader();
    r.read_messages();
    r.mut_prs().get_mut(2).unwrap().become_snapshot(10);

    for _ in 0..10 {
        r.append_entry(&mut [new_entry(0, 0, SOME_DATA)]);
        do_send_append(&mut r, 2);
        assert_eq!(r.read_messages().len(), 0);
    }
}

#[test]
fn test_recv_msg_unreachable() {
    let previous_ents = vec![empty_entry(1, 1), empty_entry(1, 2), empty_entry(1, 3)];
    let s = new_storage();
    s.wl().append(&previous_ents).expect("");
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, s);
    r.become_candidate();
    r.become_leader();
    r.read_messages();
    // set node 2 to state replicate
    r.mut_prs().get_mut(2).unwrap().matched = 3;
    r.mut_prs().get_mut(2).unwrap().become_replicate();
    r.mut_prs().get_mut(2).unwrap().optimistic_update(5);

    r.step(new_message(2, 1, MessageType::MsgUnreachable, 0))
        .expect("");

    assert_eq!(r.prs().voters()[&2].state, ProgressState::Probe);
    assert_eq!(
        r.prs().voters()[&2].matched + 1,
        r.prs().voters()[&2].next_idx
    );
}

#[test]
fn test_restore1() {
    setup();
    // magic number
    let s = new_snapshot(11, 11, vec![1, 2, 3]);

    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    assert!(sm.restore(s.clone()));
    assert_eq!(sm.raft_log.last_index(), s.get_metadata().get_index());
    assert_eq!(
        sm.raft_log.term(s.get_metadata().get_index()).unwrap(),
        s.get_metadata().get_term()
    );
    assert_eq!(
        sm.prs().nodes(),
        s.get_metadata().get_conf_state().get_nodes()
    );

    // 第二次restore，检测到本地已经包含快照中的日志，直接忽略
    assert!(!sm.restore(s));
}

/// 不接受比commit值更小的快照
/// restore快照之后，调整commit的位置
#[test]
fn test_restore_ignore_snapshot() {
    let previous_ents = vec![empty_entry(1, 1), empty_entry(1, 2), empty_entry(1, 3)];
    let commit = 1u64;
    let mut sm = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    sm.raft_log.append(&previous_ents);
    sm.raft_log.commit_to(commit);

    let mut s = new_snapshot(commit, 1, vec![1, 2]);

    // ingore snapshot
    assert!(!sm.restore(s.clone()));
    assert_eq!(sm.raft_log.committed, commit);

    // ignore snapshot and fast forward commit
    s.mut_metadata().set_index(commit + 1);
    assert!(!sm.restore(s));
    assert_eq!(sm.raft_log.committed, commit + 1);
}

/// 模拟2的回应，2中没有1给出的prev log，需要发送快照
#[test]
fn test_provide_snap() {
    // restore the state machine from a snapshot so it has a compacted log and a snapshot
    let s = new_snapshot(11, 11, vec![1, 2]); // magic number

    let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage());
    sm.restore(s);

    sm.become_candidate();
    sm.become_leader();


    // force set the next of node 2, so that node 2 needs a snapshot
    sm.mut_prs().get_mut(2).unwrap().next_idx = sm.raft_log.first_index();
    let mut m = new_message(2, 1, MessageType::MsgAppendResponse, 0);
    m.set_index(sm.prs().voters()[&2].next_idx - 1);
    m.set_reject(true);
    sm.step(m).expect("");

    let msgs = sm.read_messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].get_msg_type(), MessageType::MsgSnapshot);
}

/// leader不会将snap发送给一个，超市周期内都没收到消息的peer
#[test]
fn test_ignore_providing_snapshot() {
    // restore the state machine from a snapshot so it has a compacted log and a snapshot
    let s = new_snapshot(11, 11, vec![1, 2]); // magic number
    let mut sm = new_test_raft(1, vec![1], 10, 1, new_storage());
    sm.restore(s);

    sm.become_candidate();
    sm.become_leader();

    // force set the next of node 2, so that node 2 needs a snapshot
    // change node 2 to be inactive, expect node 1 ignore sending snapshot to 2
    sm.mut_prs().get_mut(2).unwrap().next_idx = sm.raft_log.first_index() - 1;
    sm.mut_prs().get_mut(2).unwrap().recent_active = false;

    sm.step(new_message(1, 1, MessageType::MsgPropose, 1))
        .expect("");

    assert_eq!(sm.read_messages().len(), 0);
}

#[test]
fn test_restore_from_snap_msg() {
    let s = new_snapshot(11, 11, vec![1, 2]); // magic number
    let mut sm = new_test_raft(2, vec![1, 2], 10, 1, new_storage());
    let mut m = new_message(1, 0, MessageType::MsgSnapshot, 0);
    m.set_term(2);
    m.set_snapshot(s);

    sm.step(m).expect("");

    assert_eq!(sm.leader_id, 1);

    // TODO: port the remaining if upstream completed this test.
}

/// 1 是leader，follower目前同步
///
///     隔离3之后，1继续写入100个消息，此时3因为是replica状态，因此信息都记录在 progress 的 ins中
///     模拟1生成了 snap，并且将log都清理掉
///     网络恢复后，通过beat之后，3开始与1同步
#[test]
fn test_slow_node_restore() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);
    for _ in 0..100 {
        nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    }
    // 这里是将 raft 的缓存中的日志，移动到 raft 的storage中去
    //      生成快照，并将log都 compact 删除
    next_ents(&mut nt.peers.get_mut(&1).unwrap(), &nt.storage[&1]);
    let mut cs = ConfState::new();
    cs.set_nodes(nt.peers[&1].prs().nodes());
    nt.storage[&1]
        .wl()
        .create_snapshot(nt.peers[&1].raft_log.applied, Some(cs), vec![])
        .expect("");
    nt.storage[&1]
        .wl()
        .compact(nt.peers[&1].raft_log.applied)
        .expect("");

    nt.recover();
    // send heartbeats so that the leader can learn everyone is active.
    // node 3 will only be considered as active when node 1 receives a reply from it.
    loop {
        nt.send(vec![new_message(1, 1, MessageType::MsgBeat, 0)]);
        if nt.peers[&1].prs().voters()[&3].recent_active {
            break;
        }
    }

    // trigger a snapshot
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    // 本行可以注释
    // trigger a commit
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    assert_eq!(
        nt.peers[&3].raft_log.committed,
        nt.peers[&1].raft_log.committed
    );
}

// test_step_config tests that when raft step msgProp in EntryConfChange type,
// it appends the entry to log and sets pendingConf to be true.
#[test]
fn test_step_config() {
    // a raft that cannot make progress
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    r.become_candidate();
    r.become_leader();
    let index = r.raft_log.last_index();
    let mut m = new_message(1, 1, MessageType::MsgPropose, 0);
    let mut e = Entry::new();
    e.set_entry_type(EntryType::EntryConfChange);
    m.mut_entries().push(e);
    r.step(m).expect("");
    assert_eq!(r.raft_log.last_index(), index + 1);
}

/// 增加两次 confchange消息
///
///     第二次的conf消息，类型被自动改成了EntryNormal
// test_step_ignore_config tests that if raft step the second msgProp in
// EntryConfChange type when the first one is uncommitted, the node will set
// the proposal to noop and keep its original state.
#[test]
fn test_step_ignore_config() {
    // a raft that cannot make progress
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    r.become_candidate();
    r.become_leader();

    let mut m = new_message(1, 1, MessageType::MsgPropose, 0);
    let mut e = Entry::new();
    e.set_entry_type(EntryType::EntryConfChange);
    m.mut_entries().push(e);
    assert!(!r.has_pending_conf());
    r.step(m.clone()).expect("");
    assert!(r.has_pending_conf());

    let index = r.raft_log.last_index();
    let pending_conf_index = r.pending_conf_index;
    r.step(m.clone()).expect("");

    let mut we = empty_entry(1, 3);
    we.set_entry_type(EntryType::EntryNormal);
    let wents = vec![we];
    let entries = r.raft_log.entries(index + 1, NO_LIMIT).expect("");

    assert_eq!(entries, wents);
    assert_eq!(r.pending_conf_index, pending_conf_index);
}


/// become_leader时，将pending_conf_index设置为 last_log，即选举成功的空消息之前的 index
// test_new_leader_pending_config tests that new leader sets its pending_conf_index
// based on uncommitted entries.
#[test]
fn test_new_leader_pending_config() {
    let mut tests = vec![(false, 0), (true, 1)];
    for (i, (add_entry, wpending_index)) in tests.drain(..).enumerate() {
        let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
        let mut e = Entry::new();
        if add_entry {
            e.set_entry_type(EntryType::EntryNormal);
            r.append_entry(&mut [e]);
        }
        r.become_candidate();
        r.become_leader();
        if r.pending_conf_index != wpending_index {
            panic!(
                "#{}: pending_conf_index = {}, want {}",
                i, r.pending_conf_index, wpending_index
            );
        }
        assert_eq!(r.has_pending_conf(), add_entry, "#{}: ", i);
    }
}

// test_add_node tests that add_node could update nodes correctly.
#[test]
fn test_add_node() {
    let mut r = new_test_raft(1, vec![1], 10, 1, new_storage());
    r.add_node(2);
    assert_eq!(r.prs().nodes(), vec![1, 2]);
}

/// beat超时，两个周期内没有收到 2的回应
#[test]
fn test_add_node_check_quorum() {
    let mut r = new_test_raft(1, vec![1], 10, 1, new_storage());
    r.check_quorum = true;

    r.become_candidate();
    r.become_leader();

    for _ in 0..r.get_election_timeout() - 1 {
        r.tick();
    }

    r.add_node(2);

    // This tick will reach electionTimeout, which triggers a quorum check.
    r.tick();

    // Node 1 should still be the leader after a single tick.
    assert_eq!(r.state, StateRole::Leader);

    // After another electionTimeout ticks without hearing from node 2,
    // node 1 should step down.
    for _ in 0..r.get_election_timeout() {
        r.tick();
    }

    assert_eq!(r.state, StateRole::Follower);
}

// test_remove_node tests that removeNode could update pendingConf, nodes and
// and removed list correctly.
#[test]
fn test_remove_node() {
    let mut r = new_test_raft(1, vec![1, 2], 10, 1, new_storage());
    r.remove_node(2);
    assert_eq!(r.prs().nodes(), vec![1]);

    // remove all nodes from cluster
    r.remove_node(1);
    assert!(r.prs().nodes().is_empty());
}

/// id 不在节点列表的，不能 promotable
#[test]
fn test_promotable() {
    let id = 1u64;
    let mut tests = vec![
        (vec![1], true),
        (vec![1, 2, 3], true),
        (vec![], false),
        (vec![2, 3], false),
    ];
    for (i, (peers, wp)) in tests.drain(..).enumerate() {
        let r = new_test_raft(id, peers, 5, 1, new_storage());
        if r.promotable() != wp {
            panic!("#{}: promotable = {}, want {}", i, r.promotable(), wp);
        }
    }
}

#[test]
fn test_raft_nodes() {
    let mut tests = vec![
        (vec![1, 2, 3], vec![1, 2, 3]),
        (vec![3, 2, 1], vec![1, 2, 3]),
    ];
    for (i, (ids, wids)) in tests.drain(..).enumerate() {
        let r = new_test_raft(1, ids, 10, 1, new_storage());
        if r.prs().nodes() != wids {
            panic!("#{}: nodes = {:?}, want {:?}", i, r.prs().nodes(), wids);
        }
    }
}

#[test]
fn test_campaign_while_leader1() {
    test_campaign_while_leader_with_pre_vote(false);
}

#[test]
fn test_pre_campaign_while_leader() {
    test_campaign_while_leader_with_pre_vote(true);
}

fn test_campaign_while_leader_with_pre_vote(pre_vote: bool) {
    let mut r = new_test_raft_with_prevote(1, vec![1], 5, 1, new_storage(), pre_vote);
    assert_eq!(r.state, StateRole::Follower);
    // We don't call campaign() directly because it comes after the check
    // for our current state.
    r.step(new_message(1, 1, MessageType::MsgHup, 0)).expect("");
    assert_eq!(r.state, StateRole::Leader);


    // 已经是leader了，忽略MsgHup消息
    let term = r.term;
    r.step(new_message(1, 1, MessageType::MsgHup, 0)).expect("");

    assert_eq!(r.state, StateRole::Leader);
    assert_eq!(r.term, term);
}

/// 移除node后，之前其未回复的log自动commit了
///     1是leader；三个log：选主成功、移除2、写入新数据
///
///     2回复消息 到index2；
///     然后 1执行 remove_node，这时第三个log自动commit了，可以直接从 raft log 的 next_entries 中提取出来了
///
///
// test_commit_after_remove_node verifies that pending commands can become
// committed when a config change reduces the quorum requirements.
#[test]
fn test_commit_after_remove_node() {
    // Create a cluster with two nodes.
    let s = new_storage();
    let mut r = new_test_raft(1, vec![1, 2], 5, 1, s.clone());
    r.become_candidate();
    r.become_leader();

    // Begin to remove the second node.
    let mut m = new_message(0, 0, MessageType::MsgPropose, 0);
    let mut e = Entry::new();
    e.set_entry_type(EntryType::EntryConfChange);
    let mut cc = ConfChange::new();
    cc.set_change_type(ConfChangeType::RemoveNode);
    cc.set_node_id(2);
    e.set_data(protobuf::Message::write_to_bytes(&cc).unwrap());
    m.mut_entries().push(e);
    r.step(m).expect("");

    // Stabilize the log and make sure nothing is committed yet.
    assert_eq!(next_ents(&mut r, &s).len(), 0);
    let cc_index = r.raft_log.last_index();

    // While the config change is pending, make another proposal.
    let mut m = new_message(0, 0, MessageType::MsgPropose, 0);
    let mut e = new_entry(0, 0, Some("hello"));
    e.set_entry_type(EntryType::EntryNormal);
    m.mut_entries().push(e);
    r.step(m).expect("");

    // Node 2 acknowledges the config change, committing it.
    let mut m = new_message(2, 0, MessageType::MsgAppendResponse, 0);
    m.set_index(cc_index);
    r.step(m).expect("");
    let ents = next_ents(&mut r, &s);
    assert_eq!(ents.len(), 2);
    assert_eq!(ents[0].get_entry_type(), EntryType::EntryNormal);
    assert!(ents[0].get_data().is_empty());
    assert_eq!(ents[1].get_entry_type(), EntryType::EntryConfChange);

    // Apply the config change. This reduces quorum requirements so the
    // pending command can now commit.
    r.remove_node(2);
    let ents = next_ents(&mut r, &s);
    assert_eq!(ents.len(), 1);
    assert_eq!(ents[0].get_entry_type(), EntryType::EntryNormal);
    assert_eq!(ents[0].get_data(), b"hello");
}


///     node2发起transfer到1， node2有最新数据，transfer 会立即成功
///     node收到propose消息，转发给node2；node1发起transfer到2
///
// test_leader_transfer_to_uptodate_node verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
#[test]
fn test_leader_transfer_to_uptodate_node1() {
    setup();
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    let lead_id = nt.peers[&1].leader_id;
    assert_eq!(lead_id, 1);

    // Transfer leadership to peer 2.
    nt.send(vec![new_message(2, 1, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 2);

    // After some log replication, transfer leadership back to peer 1.
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    nt.send(vec![new_message(1, 2, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

/// 非租约模式：transfer消息发送给follower后，会立即转发给当前leader
///
// test_leader_transfer_to_uptodate_node_from_follower verifies transferring should succeed
// if the transferee has the most up-to-date log entries when transfer starts.
// Not like test_leader_transfer_to_uptodate_node, where the leader transfer message
// is sent to the leader, in this test case every leader transfer message is sent
// to the follower.
#[test]
fn test_leader_transfer_to_uptodate_node_from_follower() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    let lead_id = nt.peers[&1].leader_id;
    assert_eq!(lead_id, 1);

    // transfer leadership to peer 2.
    nt.send(vec![new_message(2, 2, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 2);

    // After some log replication, transfer leadership back to peer 1.
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

/// 租约模式下：transferee也是正常工作, 即使follower的上一个leader还未超时
// TestLeaderTransferWithCheckQuorum ensures transferring leader still works
// even the current leader is still under its leader lease
#[test]
fn test_leader_transfer_with_check_quorum() {

    // 设置为租约模式，调整选主随机值，有作用？
    let mut nt = Network::new(vec![None, None, None]);
    for i in 1..4 {
        let r = &mut nt.peers.get_mut(&i).unwrap();
        r.check_quorum = true;
        let election_timeout = r.get_election_timeout();
        r.set_randomized_election_timeout(election_timeout + i as usize);
    }

    // 这一段代码没什么用？
    // 将node2的选举随机值，推迟一个周期
    let b_election_timeout = nt.peers[&2].get_election_timeout();
    nt.peers
        .get_mut(&2)
        .unwrap()
        .set_randomized_election_timeout(b_election_timeout + 1);

    // 实际上这个不需要？1直接可以成为leader...
    // node2超时，可以投票给其他人了，但不会参与选举
    // Letting peer 2 electionElapsed reach to timeout so that it can vote for peer 1
    for _ in 0..b_election_timeout {
        nt.peers.get_mut(&2).unwrap().tick();
    }

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].leader_id, 1);

    // Transfer leadership to 2.
    nt.send(vec![new_message(2, 1, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 2);

    // After some log replication, transfer leadership back to 1.
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    nt.send(vec![new_message(1, 2, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

/// 先将3 隔离，导致3的数据少于leader
///
/// transferee时，如果目标的数据少于当前leader，leader会立即将数据发送过去
///     需要等待数据全部发送完成，在leader的handle append resp时，检查到此状态，会继续启动transferee过程
///
#[test]
fn test_leader_transfer_to_slow_follower() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    nt.recover();
    assert_eq!(nt.peers[&1].prs().voters()[&3].matched, 1);

    // Transfer leadership to 3 when node 3 is lack of log.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);

    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 3);
}

/// transferee时，follower需要从leader获取数据，以snap方式获取
#[test]
fn test_leader_transfer_after_snapshot() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    // 将leader的数据，合并到snap中去
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    next_ents(&mut nt.peers.get_mut(&1).unwrap(), &nt.storage[&1]);
    let mut cs = ConfState::new();
    cs.set_nodes(nt.peers[&1].prs().nodes());
    nt.storage[&1]
        .wl()
        .create_snapshot(nt.peers[&1].raft_log.applied, Some(cs), vec![])
        .expect("");
    nt.storage[&1]
        .wl()
        .compact(nt.peers[&1].raft_log.applied)
        .expect("");

    nt.recover();
    assert_eq!(nt.peers[&1].prs().voters()[&3].matched, 1);

    // Transfer leadership to 3 when node 3 is lack of snapshot.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);

    // Send pb.MsgHeartbeatResp to leader to trigger a snapshot for node 3.
    // 代码不太对，不需要发送beat resp，就可以成功
    nt.send(vec![new_message(
        3,
        1,
        MessageType::MsgHeartbeatResponse,
        0,
    )]);

    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 3);
}

/// transferee给leader自己，直接忽略
#[test]
fn test_leader_transfer_to_self() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // Transfer leadership to self, there will be noop.
    nt.send(vec![new_message(1, 1, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

#[test]
fn test_leader_transfer_to_non_existing_node() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // Transfer leadership to non-existing node, there will be noop.
    nt.send(vec![new_message(4, 1, MessageType::MsgTransferLeader, 0)]);
    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

/// 隔离3，执行transferee到3；
///     leader等待，直到发生election_timeout超时，过程将 abort
#[test]
fn test_leader_transfer_timeout() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    // Transfer leadership to isolated node, wait for timeout.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);
    let heartbeat_timeout = nt.peers[&1].get_heartbeat_timeout();
    let election_timeout = nt.peers[&1].get_election_timeout();
    for _ in 0..heartbeat_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);
    for _ in 0..election_timeout - heartbeat_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }

    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

/// leader已经转换成transferee状态期间，不会接受新的propose请求
#[test]
fn test_leader_transfer_ignore_proposal() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    // Transfer leadership to isolated node to let transfer pending, then send proposal.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);

    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    assert_eq!(
        nt.peers
            .get_mut(&1)
            .unwrap()
            .step(new_message(1, 1, MessageType::MsgPropose, 1)),
        Err(Error::ProposalDropped),
        "should return drop proposal error while transferring"
    );

    assert_eq!(nt.peers[&1].prs().voters()[&1].matched, 1);
}

/// 隔离3；1收到3的transferee；
/// 2成为candidate，有更大的term，当前leader将接收新的leader
#[test]
fn test_leader_transfer_receive_higher_term_vote() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    // Transfer leadership to isolated node to let transfer pending.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);

    nt.send(vec![new_message_with_entries(
        2,
        2,
        MessageType::MsgHup,
        vec![new_entry(1, 2, None)],
    )]);

    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 2);
}

/// leader在transferee期间，remove了目标node，abort
#[test]
fn test_leader_transfer_remove_node() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.ignore(MessageType::MsgTimeoutNow);

    // The lead_transferee is removed when leadship transferring.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);

    nt.peers.get_mut(&1).unwrap().remove_node(3);

    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

/// 隔离3，transferee到3（尚未成功），然后transferee回到1
// test_leader_transfer_back verifies leadership can transfer
// back to self when last transfer is pending.
#[test]
fn test_leader_transfer_back() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);

    // Transfer leadership back to self.
    nt.send(vec![new_message(1, 1, MessageType::MsgTransferLeader, 0)]);

    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

/// 隔离3，transferee到3（尚未成功），再transferee到2
// test_leader_transfer_second_transfer_to_another_node verifies leader can transfer to another node
// when last transfer is pending.
#[test]
fn test_leader_transfer_second_transfer_to_another_node() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);

    // Transfer leadership to another node.
    nt.send(vec![new_message(2, 1, MessageType::MsgTransferLeader, 0)]);

    check_leader_transfer_state(&nt.peers[&1], StateRole::Follower, 2);
}

/// 隔离3，transferee到3（尚未成功），触发node1 的超时
// test_leader_transfer_second_transfer_to_same_node verifies second transfer leader request
// to the same node should not extend the timeout while the first one is pending.
#[test]
fn test_leader_transfer_second_transfer_to_same_node() {
    let mut nt = Network::new(vec![None, None, None]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    nt.isolate(3);

    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);
    assert_eq!(nt.peers[&1].lead_transferee.unwrap(), 3);

    let heartbeat_timeout = nt.peers[&1].get_heartbeat_timeout();
    for _ in 0..heartbeat_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }

    // Second transfer leadership request to the same node.
    nt.send(vec![new_message(3, 1, MessageType::MsgTransferLeader, 0)]);

    let election_timeout = nt.peers[&1].get_election_timeout();
    for _ in 0..election_timeout - heartbeat_timeout {
        nt.peers.get_mut(&1).unwrap().tick();
    }

    check_leader_transfer_state(&nt.peers[&1], StateRole::Leader, 1);
}

fn check_leader_transfer_state(r: &Raft<MemStorage>, state: StateRole, lead: u64) {
    if r.state != state || r.leader_id != lead {
        panic!(
            "after transferring, node has state {:?} lead {}, want state {:?} lead {}",
            r.state, r.leader_id, state, lead
        );
    }
    assert_eq!(r.lead_transferee, None);
}

/// node1不在 list中
///     它收到了timeoutnow请求；消息将被node1忽略
///     node1,再收到了其他node的vote回应，因为是follower状态，也会忽略
// test_transfer_non_member verifies that when a MsgTimeoutNow arrives at
// a node that has been removed from the group, nothing happens.
// (previously, if the node also got votes, it would panic as it
// transitioned to StateRole::Leader)
#[test]
fn test_transfer_non_member() {
    let mut raft = new_test_raft(1, vec![2, 3, 4], 5, 1, new_storage());
    raft.step(new_message(2, 1, MessageType::MsgTimeoutNow, 0))
        .expect("");;

    raft.step(new_message(2, 1, MessageType::MsgRequestVoteResponse, 0))
        .expect("");;
    raft.step(new_message(3, 1, MessageType::MsgRequestVoteResponse, 0))
        .expect("");;
    assert_eq!(raft.state, StateRole::Follower);
}

/// PreVote情况下
///     隔离3；1、2轮流成为leader，3尝试，成为pre-candidate；因为次数更多，所以1、2 有较新数据；
///     隔离2；3尝试成为leader，因为term较小，被1拒绝了，同时使得3收到回应变成了 follower
///           1尝试成为leader，成功
// TestNodeWithSmallerTermCanCompleteElection tests the scenario where a node
// that has been partitioned away (and fallen behind) rejoins the cluster at
// about the same time the leader node gets partitioned away.
// Previously the cluster would come to a standstill when run with PreVote
// enabled.
#[test]
fn test_node_with_smaller_term_can_complete_election() {
    let mut n1 = new_test_raft_with_prevote(1, vec![1, 2, 3], 10, 1, new_storage(), true);
    let mut n2 = new_test_raft_with_prevote(2, vec![1, 2, 3], 10, 1, new_storage(), true);
    let mut n3 = new_test_raft_with_prevote(3, vec![1, 2, 3], 10, 1, new_storage(), true);

    n1.become_follower(1, INVALID_ID);
    n2.become_follower(1, INVALID_ID);
    n3.become_follower(1, INVALID_ID);

    // cause a network partition to isolate node 3
    let mut nt = Network::new_with_config(vec![Some(n1), Some(n2), Some(n3)], true);
    nt.cut(1, 3);
    nt.cut(2, 3);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);

    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&3].state, StateRole::PreCandidate);

    nt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);

    // check whether the term values are expected
    // a.Term == 3
    // b.Term == 3
    // c.Term == 1
    assert_eq!(nt.peers[&1].term, 3);
    assert_eq!(nt.peers[&2].term, 3);
    assert_eq!(nt.peers[&3].term, 1);

    // check state
    // a == follower
    // b == leader
    // c == pre-candidate
    assert_eq!(nt.peers[&1].state, StateRole::Follower);
    assert_eq!(nt.peers[&2].state, StateRole::Leader);
    assert_eq!(nt.peers[&3].state, StateRole::PreCandidate);

    // recover the network then immediately isolate b which is currently
    // the leader, this is to emulate the crash of b.
    nt.recover();
    nt.cut(2, 1);
    nt.cut(2, 3);

    // call for election
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // do we have a leader?
    assert!(
        nt.peers[&1].state == StateRole::Leader || nt.peers[&3].state == StateRole::Leader,
        "no leader"
    );
}

pub fn new_test_learner_raft(
    id: u64,
    peers: Vec<u64>,
    learners: Vec<u64>,
    election: usize,
    heartbeat: usize,
    storage: MemStorage,
) -> Interface {
    let mut cfg = new_test_config(id, peers, election, heartbeat);
    cfg.learners = learners;
    Interface::new(Raft::new(&cfg, storage))
}

/// leaner 不会成为 leader
// TestLearnerElectionTimeout verfies that the leader should not start election
// even when times out.
#[test]
fn test_learner_election_timeout() {
    let mut n1 = new_test_learner_raft(1, vec![1], vec![2], 10, 1, new_storage());
    n1.become_follower(1, INVALID_ID);

    let mut n2 = new_test_learner_raft(2, vec![1], vec![2], 10, 1, new_storage());
    n2.become_follower(1, INVALID_ID);

    let timeout = n2.get_election_timeout();
    n2.set_randomized_election_timeout(timeout);

    // n2 is a learner. Learner should not start election even when time out.
    for _ in 0..timeout {
        n2.tick();
    }
    assert_eq!(n2.state, StateRole::Follower);
}

/// 2个node，2是learner；1成为leader，将2加入到node set中，2超时后成为leader
// TestLearnerPromotion verifies that the leaner should not election until
// it is promoted to a normal peer.
#[test]
fn test_learner_promotion() {
    // 1是node，2是leaner
    //      1 超时成为leader
    let mut n1 = new_test_learner_raft(1, vec![1], vec![2], 10, 1, new_storage());
    n1.become_follower(1, INVALID_ID);

    let mut n2 = new_test_learner_raft(2, vec![1], vec![2], 10, 1, new_storage());
    n2.become_follower(1, INVALID_ID);

    let mut network = Network::new(vec![Some(n1), Some(n2)]);
    assert_eq!(network.peers[&1].state, StateRole::Follower);

    // n1 should become leader.
    let timeout = network.peers[&1].get_election_timeout();
    network
        .peers
        .get_mut(&1)
        .unwrap()
        .set_randomized_election_timeout(timeout);
    for _ in 0..timeout {
        network.peers.get_mut(&1).unwrap().tick();
    }
    assert_eq!(network.peers[&1].state, StateRole::Leader);
    assert_eq!(network.peers[&2].state, StateRole::Follower);

    let mut heart_beat = new_message(1, 1, MessageType::MsgBeat, 0);
    network.send(vec![heart_beat.clone()]);

    // Promote n2 from learner to follower.
    network.peers.get_mut(&1).unwrap().add_node(2);
    network.peers.get_mut(&2).unwrap().add_node(2);
    assert_eq!(network.peers[&2].state, StateRole::Follower);
    assert!(!network.peers[&2].is_learner);

    let timeout = network.peers[&2].get_election_timeout();
    network
        .peers
        .get_mut(&2)
        .unwrap()
        .set_randomized_election_timeout(timeout);
    for _ in 0..timeout {
        network.peers.get_mut(&2).unwrap().tick();
    }

    heart_beat.set_to(2);
    heart_beat.set_from(2);
    network.send(vec![heart_beat]);
    assert_eq!(network.peers[&1].state, StateRole::Follower);
    assert_eq!(network.peers[&2].state, StateRole::Leader);
}

/// 成为leader之后，只有一个node，campaign就直接成为了leader；
///     node1不会进行广播，因此2不会收到消息；通过beat消息，2从1收到了日志
// TestLearnerLogReplication tests that a learner can receive entries from the leader.
#[test]
fn test_learner_log_replication() {
    let n1 = new_test_learner_raft(1, vec![1], vec![2], 10, 1, new_storage());
    let n2 = new_test_learner_raft(2, vec![1], vec![2], 10, 1, new_storage());
    let mut network = Network::new(vec![Some(n1), Some(n2)]);

    network
        .peers
        .get_mut(&1)
        .unwrap()
        .become_follower(1, INVALID_ID);
    network
        .peers
        .get_mut(&2)
        .unwrap()
        .become_follower(1, INVALID_ID);

    let timeout = network.peers[&1].get_election_timeout();
    network
        .peers
        .get_mut(&1)
        .unwrap()
        .set_randomized_election_timeout(timeout);

    for _ in 0..timeout {
        network.peers.get_mut(&1).unwrap().tick();
    }

    let heart_beat = new_message(1, 1, MessageType::MsgBeat, 0);
    network.send(vec![heart_beat.clone()]);

    assert_eq!(network.peers[&1].state, StateRole::Leader);
    assert_eq!(network.peers[&2].state, StateRole::Follower);
    assert!(network.peers[&2].is_learner);

    let next_committed = network.peers[&1].raft_log.committed + 1;

    let msg = new_message(1, 1, MessageType::MsgPropose, 1);
    network.send(vec![msg]);

    assert_eq!(network.peers[&1].raft_log.committed, next_committed);
    assert_eq!(network.peers[&2].raft_log.committed, next_committed);

    let matched = network
        .peers
        .get_mut(&1)
        .unwrap()
        .prs()
        .get(2)
        .unwrap()
        .matched;
    assert_eq!(matched, network.peers[&2].raft_log.committed);
}

/// 构建一个快照，其中的配置信息包括了learner信息
///     通过快照恢复learner的状态
// TestRestoreWithLearner restores a snapshot which contains learners.
#[test]
fn test_restore_with_learner() {
    let mut s = new_snapshot(11, 11, vec![1, 2]);
    s.mut_metadata().mut_conf_state().mut_learners().push(3);

    let mut sm = new_test_learner_raft(3, vec![1, 2], vec![3], 10, 1, new_storage());
    assert!(sm.is_learner);
    assert!(sm.restore(s.clone()));
    assert_eq!(sm.raft_log.last_index(), 11);
    assert_eq!(sm.raft_log.term(11).unwrap(), 11);
    assert_eq!(sm.prs().voters().len(), 2);
    assert_eq!(sm.prs().learners().len(), 1);

    for node in s.get_metadata().get_conf_state().get_nodes() {
        assert!(sm.prs().voters().get(node).is_some());
        assert!(!sm.prs().voters()[node].is_learner);
    }

    for node in s.get_metadata().get_conf_state().get_learners() {
        assert!(sm.prs().learners().get(node).is_some());
        assert!(sm.prs().learners()[node].is_learner);
    }

    assert!(!sm.restore(s));
}

/// restore时，只要自己之前启动时已经有配置了，而且自己不是learner；
///     restore中的配置，如果自己是learner，那么就会拒绝snap
///     fn restore_raft
// TestRestoreInvalidLearner verfies that a normal peer can't become learner again
// when restores snapshot.
#[test]
fn test_restore_invalid_learner() {
    let mut s = new_snapshot(11, 11, vec![1, 2]);
    s.mut_metadata().mut_conf_state().mut_learners().push(3);

    let mut sm = new_test_raft(3, vec![1, 2, 3], 10, 1, new_storage());
    assert!(!sm.is_learner);
    assert!(!sm.restore(s));
}

/// 启动时配置为learner，但是可以从snap的配置中，变成follower
///     大多数情况下，会根据snap中的配置，来配置本地状态
// TestRestoreLearnerPromotion checks that a learner can become to a follower after
// restoring snapshot.
#[test]
fn test_restore_learner_promotion() {
    let s = new_snapshot(11, 11, vec![1, 2, 3]);
    let mut sm = new_test_learner_raft(3, vec![1, 2], vec![3], 10, 1, new_storage());
    assert!(sm.is_learner);
    assert!(sm.restore(s));
    assert!(!sm.is_learner);
}

/// learner可以从snap恢复
// TestLearnerReceiveSnapshot tests that a learner can receive a snpahost from leader.
#[test]
fn test_learner_receive_snapshot() {
    let mut s = new_snapshot(11, 11, vec![1]);
    s.mut_metadata().mut_conf_state().mut_learners().push(2);

    let mut n1 = new_test_learner_raft(1, vec![1], vec![2], 10, 1, new_storage());
    let n2 = new_test_learner_raft(2, vec![1], vec![2], 10, 1, new_storage());

    n1.restore(s);
    let committed = n1.raft_log.committed;
    n1.raft_log.applied_to(committed);

    let mut network = Network::new(vec![Some(n1), Some(n2)]);

    let timeout = network.peers[&1].get_election_timeout();
    network
        .peers
        .get_mut(&1)
        .unwrap()
        .set_randomized_election_timeout(timeout);

    for _ in 0..timeout {
        network.peers.get_mut(&1).unwrap().tick();
    }

    let mut msg = Message::new();
    msg.set_from(1);
    msg.set_to(1);
    msg.set_msg_type(MessageType::MsgBeat);
    network.send(vec![msg]);

    let n1_committed = network.peers[&1].raft_log.committed;
    let n2_committed = network.peers[&2].raft_log.committed;
    assert_eq!(n1_committed, n2_committed);
}

/// 添加、删除learner
// TestAddLearner tests that addLearner could update nodes correctly.
#[test]
fn test_add_learner() {
    let mut n1 = new_test_raft(1, vec![1], 10, 1, new_storage());
    n1.add_learner(2);

    assert_eq!(n1.prs().learner_nodes(), vec![2]);
    assert!(n1.prs().learners()[&2].is_learner);
}

// TestRemoveLearner tests that removeNode could update nodes and
// and removed list correctly.
#[test]
fn test_remove_learner() {
    let mut n1 = new_test_learner_raft(1, vec![1], vec![2], 10, 1, new_storage());
    n1.remove_node(2);
    assert_eq!(n1.prs().nodes(), vec![1]);
    assert_eq!(n1.prs().learner_nodes(), vec![]);

    n1.remove_node(1);
    assert!(n1.prs().nodes().is_empty());
    assert!(n1.prs().learner_nodes().is_empty());
}

#[test]
fn test_pre_vote_normal_with_new_data() {
    let mut nt = test_pre_vote_normal_by_dong(true);

    info!("------------------------------------");
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);
//    let mut to_send = new_message(1, 3, MessageType::MsgHeartbeat, 0);
//    to_send.set_term(nt.peers[&1].term);
//    nt.send(vec![to_send]);

}

#[test]
fn test_pre_vote_normal_without_new_data() {
    let mut nt = test_pre_vote_normal_by_dong(false);

    info!("------------------------------------");
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&3].state, StateRole::Leader);
}


fn test_pre_vote_normal_by_dong(withdata: bool) -> Network {
    let mut n1 = new_test_raft_with_prevote(1, vec![1, 2, 3], 10, 1, new_storage(), true);
    let mut n2 = new_test_raft_with_prevote(2, vec![1, 2, 3], 10, 1, new_storage(), true);
    let mut n3 = new_test_raft_with_prevote(3, vec![1, 2, 3], 10, 1, new_storage(), true);

    n1.become_follower(1, INVALID_ID);
    n2.become_follower(1, INVALID_ID);
    n3.become_follower(1, INVALID_ID);

    let mut nt = Network::new(vec![Some(n1), Some(n2), Some(n3)]);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // Cause a network partition to isolate n3.
    nt.isolate(3);

    if withdata {
        nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);
    }
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // check state
    // n1.state == Leader
    // n2.state == Follower
    // n3.state == Candidate
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::PreCandidate);

    // check term
    // n1.Term == 2
    // n2.Term == 2
    // n3.Term == 4
    assert_eq!(nt.peers[&1].term, 2);
    assert_eq!(nt.peers[&2].term, 2);
    assert_eq!(nt.peers[&3].term, 2);

    nt.recover();

    nt
}
/// 模拟场景：
///     node1、2配置了prevote，node3没有（为了是3的term更大）
///     1成为leader后，隔离3，在1中写入新数据；
///     3两次试图成为leader；因此term比1、2大；
///     恢复网络
///
// simulate rolling update a cluster for Pre-Vote. cluster has 3 nodes [n1, n2, n3].
// n1 is leader with term 2
// n2 is follower with term 2
// n3 is partitioned, with term 4 and less log, state is candidate
fn new_prevote_migration_cluster() -> Network {
    // We intentionally do not enable pre_vote for n3, this is done so in order
    // to simulate a rolling restart process where it's possible to have a mixed
    // version cluster with replicas with pre_vote enabled, and replicas without.
    let mut n1 = new_test_raft_with_prevote(1, vec![1, 2, 3], 10, 1, new_storage(), true);
    let mut n2 = new_test_raft_with_prevote(2, vec![1, 2, 3], 10, 1, new_storage(), true);
    let mut n3 = new_test_raft_with_prevote(3, vec![1, 2, 3], 10, 1, new_storage(), false);

    n1.become_follower(1, INVALID_ID);
    n2.become_follower(1, INVALID_ID);
    n3.become_follower(1, INVALID_ID);

    let mut nt = Network::new(vec![Some(n1), Some(n2), Some(n3)]);

    nt.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // Cause a network partition to isolate n3.
    nt.isolate(3);
    nt.send(vec![new_message(1, 1, MessageType::MsgPropose, 1)]);

    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    // check state
    // n1.state == Leader
    // n2.state == Follower
    // n3.state == Candidate
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::Candidate);

    // check term
    // n1.Term == 2
    // n2.Term == 2
    // n3.Term == 4
    assert_eq!(nt.peers[&1].term, 2);
    assert_eq!(nt.peers[&2].term, 2);
    assert_eq!(nt.peers[&3].term, 4);

    // Enable prevote on n3, then recover the network
    nt.peers.get_mut(&3).unwrap().pre_vote = true;
    nt.recover();

    nt
}

/// 上述环境中，隔离1, 之后2、3同时尝试成为leader
///     3发送的prevote：2的数据更新，因此会拒绝此请求；2的term消息中term较小，因此3也会忽略2的回应
///     2发送的prevote：因为3的term更大，但因为是MsgRequestPreVote，会恢复拒绝消息，同时带上最新的term
///                     2收到更大term的prevote resp，就更新自己的term，成为follower
///
///     再发起一轮，2将成为leader
///
///     step中的一处处理，3收到低term的prevote消息后，回应一个高term的reject的prevote resp，帮助2提升term
#[test]
fn test_prevote_migration_can_complete_election() {
    // n1 is leader with term 2
    // n2 is follower with term 2
    // n3 is pre-candidate with term 4, and less log
    let mut nt = new_prevote_migration_cluster();

    // simulate leader down
    nt.isolate(1);

    // Call for elections from both n2 and n3.
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);

    // check state
    // n2.state == Follower
    // n3.state == PreCandidate
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::PreCandidate);

    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    nt.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);

    // Do we have a leader?
    assert!(
        (nt.peers[&2].state == StateRole::Leader) || (nt.peers[&3].state == StateRole::Follower)
    );
}

/// 3恢复后尝试成为leader，但数据较旧忽略
///     1发送beat消息过去，3的数据得到同步，回复append消息
///     因为appendresp的消息中，term更高，因此leader 1被打断，成为follower
#[test]
fn test_prevote_migration_with_free_stuck_pre_candidate() {
    let mut nt = new_prevote_migration_cluster();

    // n1 is leader with term 2
    // n2 is follower with term 2
    // n3 is pre-candidate with term 4, and less log
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::PreCandidate);

    // Pre-Vote again for safety
    nt.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);
    assert_eq!(nt.peers[&1].state, StateRole::Leader);
    assert_eq!(nt.peers[&2].state, StateRole::Follower);
    assert_eq!(nt.peers[&3].state, StateRole::PreCandidate);

    let mut to_send = new_message(1, 3, MessageType::MsgHeartbeat, 0);
    to_send.set_term(nt.peers[&1].term);
    nt.send(vec![to_send]);

    // Disrupt the leader so that the stuck peer is freed
    assert_eq!(nt.peers[&1].state, StateRole::Follower);

    assert_eq!(nt.peers[&3].term, nt.peers[&1].term);
}

/// 3是learner，隔离2
///     1无法成为leader，因为没有大多数
///     将3升级为peer，leader选主成功
#[test]
fn test_learner_respond_vote() {
    let mut n1 = new_test_learner_raft(1, vec![1, 2], vec![3], 10, 1, new_storage());
    n1.become_follower(1, INVALID_ID);
    n1.reset_randomized_election_timeout();

    let mut n3 = new_test_learner_raft(3, vec![1, 2], vec![3], 10, 1, new_storage());
    n3.become_follower(1, INVALID_ID);
    n3.reset_randomized_election_timeout();

    let do_campaign = |nw: &mut Network| {
        let msg = new_message(1, 1, MessageType::MsgHup, 0);
        nw.send(vec![msg]);
    };

    let mut network = Network::new(vec![Some(n1), None, Some(n3)]);
    network.isolate(2);

    // Can't elect new leader because 1 won't send MsgRequestVote to 3.
    do_campaign(&mut network);
    assert_eq!(network.peers[&1].state, StateRole::Candidate);

    // After promote 3 to voter, election should success.
    network.peers.get_mut(&1).unwrap().add_node(3);
    do_campaign(&mut network);
    assert_eq!(network.peers[&1].state, StateRole::Leader);
}

/// tick配置检查
///     heartbeat_tick < election_tick <= min_election_tick <= random  < max_election_tick
#[test]
fn test_election_tick_range() {
    let mut cfg = new_test_config(1, vec![1, 2, 3], 10, 1);
    let mut raft = Raft::new(&cfg, new_storage());
    for _ in 0..1000 {
        raft.reset_randomized_election_timeout();
        let randomized_timeout = raft.get_randomized_election_timeout();
        assert!(
            cfg.election_tick <= randomized_timeout && randomized_timeout < 2 * cfg.election_tick
        );
    }

    cfg.min_election_tick = cfg.election_tick;
    cfg.validate().unwrap();

    // Too small election tick.
    cfg.min_election_tick = cfg.election_tick - 1;
    cfg.validate().unwrap_err();

    // max_election_tick should be larger than min_election_tick
    cfg.min_election_tick = cfg.election_tick;
    cfg.max_election_tick = cfg.election_tick;
    cfg.validate().unwrap_err();

    cfg.max_election_tick = cfg.election_tick + 1;
    raft = Raft::new(&cfg, new_storage());
    for _ in 0..100 {
        raft.reset_randomized_election_timeout();
        let randomized_timeout = raft.get_randomized_election_timeout();
        assert_eq!(randomized_timeout, cfg.election_tick);
    }
}

/// prevote，1是leader，隔离1
///
///     2、3同时发送prevote消息给对方，都可以成功（检测时只要是prevote，而且对方数据比自己新，就返回prevote的成功请求）
///     2、3同时成为candidate，发送vote消息给对方；因为2、3都已经有了vote对象，因此拒绝对方请求；
///
///     依赖于后续的请求过程：2再次超时，发起请求后成为leader
///
// TestPreVoteWithSplitVote verifies that after split vote, cluster can complete
// election in next round.
#[test]
fn test_prevote_with_split_vote() {
    let peers = (1..=3).map(|id| {
        let mut raft = new_test_raft_with_prevote(id, vec![1, 2, 3], 10, 1, new_storage(), true);
        raft.become_follower(1, INVALID_ID);
        Some(raft)
    });
    let mut network = Network::new(peers.collect());
    network.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // simulate leader down. followers start split vote.
    network.isolate(1);
    network.send(vec![
        new_message(2, 2, MessageType::MsgHup, 0),
        new_message(3, 3, MessageType::MsgHup, 0),
    ]);

    // check whether the term values are expected
    assert_eq!(network.peers[&2].term, 3, "peer 2 term",);
    assert_eq!(network.peers[&3].term, 3, "peer 3 term",);

    // check state
    assert_eq!(
        network.peers[&2].state,
        StateRole::Candidate,
        "peer 2 state",
    );
    assert_eq!(
        network.peers[&3].state,
        StateRole::Candidate,
        "peer 3 state",
    );

    // node 2 election timeout first
    network.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);

    // check whether the term values are expected
    assert_eq!(network.peers[&2].term, 4, "peer 2 term",);
    assert_eq!(network.peers[&3].term, 4, "peer 3 term",);

    // check state
    assert_eq!(network.peers[&2].state, StateRole::Leader, "peer 2 state",);
    assert_eq!(network.peers[&3].state, StateRole::Follower, "peer 3 state",);
}

/// prevote + checkquorum
///     1是leader，隔离3；1、2的term增加到4；
///     3恢复后，2、3同时向对方发送pre vote消息
///     2的term更高，3接收了2的prevote以及后续的vote请求；3的term较低，2不会给它的prevote投票
///
///
// ensure that after a node become pre-candidate, it will checkQuorum correctly.
#[test]
fn test_prevote_with_check_quorum() {
    let bootstrap = |id| {
        let mut cfg = new_test_config(id, vec![1, 2, 3], 10, 1);
        cfg.pre_vote = true;
        cfg.check_quorum = true;
        let mut raft = Raft::new(&cfg, new_storage());
        raft.become_follower(1, INVALID_ID);
        Interface::new(raft)
    };
    let (peer1, peer2, peer3) = (bootstrap(1), bootstrap(2), bootstrap(3));

    let mut network = Network::new(vec![Some(peer1), Some(peer2), Some(peer3)]);
    network.send(vec![new_message(1, 1, MessageType::MsgHup, 0)]);

    // cause a network partition to isolate node 3. node 3 has leader info
    network.cut(1, 3);
    network.cut(2, 3);

    assert_eq!(network.peers[&1].state, StateRole::Leader, "peer 1 state",);
    assert_eq!(network.peers[&2].state, StateRole::Follower, "peer 2 state",);

    network.send(vec![new_message(3, 3, MessageType::MsgHup, 0)]);

    assert_eq!(
        network.peers[&3].state,
        StateRole::PreCandidate,
        "peer 3 state",
    );

    // transfer两次，使得term增加两次
    // term + 2, so that node 2 will ignore node 3's PreVote
    network.send(vec![new_message(2, 1, MessageType::MsgTransferLeader, 0)]);
    network.send(vec![new_message(1, 2, MessageType::MsgTransferLeader, 0)]);

    // check whether the term values are expected
    assert_eq!(network.peers[&1].term, 4, "peer 1 term",);
    assert_eq!(network.peers[&2].term, 4, "peer 2 term",);
    assert_eq!(network.peers[&3].term, 2, "peer 3 term",);

    // check state
    assert_eq!(network.peers[&1].state, StateRole::Leader, "peer 1 state",);
    assert_eq!(network.peers[&2].state, StateRole::Follower, "peer 2 state",);
    assert_eq!(
        network.peers[&3].state,
        StateRole::PreCandidate,
        "peer 3 state",
    );

    // recover the network then immediately isolate node 1 which is currently
    // the leader, this is to emulate the crash of node 1.
    network.recover();
    network.cut(1, 2);
    network.cut(1, 3);

    // 这里只是触发了超时，消息并未发送到2，只是在3 的队列中
    // call for election. node 3 shouldn't ignore node 2's PreVote
    let timeout = network.peers[&3].get_randomized_election_timeout();
    for _ in 0..timeout {
        network.peers.get_mut(&3).unwrap().tick();
    }
    network.send(vec![new_message(2, 2, MessageType::MsgHup, 0)]);

    // check state
    assert_eq!(network.peers[&2].state, StateRole::Leader, "peer 2 state",);
    assert_eq!(network.peers[&3].state, StateRole::Follower, "peer 3 state",);
}
