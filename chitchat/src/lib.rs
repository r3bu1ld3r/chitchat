pub mod server;

mod delta;
mod digest;
mod failure_detector;
mod message;
pub(crate) mod serialize;
mod state;

use std::collections::HashSet;
use std::net::SocketAddr;

use delta::Delta;
use failure_detector::FailureDetector;
pub use failure_detector::FailureDetectorConfig;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tracing::{debug, error, warn};

use crate::digest::Digest;
use crate::message::ChitchatMessage;
use crate::serialize::Serializable;
use crate::state::NodeState;
pub use crate::state::{ClusterState, SerializableClusterState};

/// Map key for the heartbeat node value.
pub(crate) const HEARTBEAT_KEY: &str = "heartbeat";

pub type Version = u64;

/// [`NodeId`] represents a Chitchat Node identifier.
///
/// For the lifetime of a cluster, nodes can go down and back up, they may
/// permanently die. These are couple of issues we want to solve with [`NodeId`] struct:
/// - We want a fresh local chitchat state for every run of a node.
/// - We don’t want other nodes to override a newly started node state with an obsolete state.
/// - We want other running nodes to detect that a newly started node’s state prevails all its
///   previous state.
/// - We want a node to advertise its own gossip address.
/// - We want a node to have an id that is the same across subsequent runs for keeping cache data
///   around as long as possible.
///
/// Our solution to this is:
/// - The `id` attribute which represents the node's unique identifier in the cluster should be
///   dynamic on every run. This easily solves our first three requirements. The tradeoff is that
///   starting node need to always propagate their fresh state and old states are never reclaimed.
/// - Having `gossip_public_address` attribute fulfils our fourth requirements, its value is
///   expected to be from a config item or an environnement variable.
/// - Making part of the `id` attribute static and related to the node solves the last requirement.
///
/// Because Chitchat instance is not concerned about caching strategy and what needs to be
/// cached, We let the client decide what makes up the `id` attribute and how to extract its
/// components.
///
/// One such client is Quickwit where the `id` is made of
/// `{node_unique_id}/{node_generation}/`.
/// - node_unique_id: a static unique name for the node.
/// - node_generation: a monotonically increasing value (timestamp on every run)
/// More details at https://github.com/quickwit-oss/chitchat/issues/1#issuecomment-1059029051
///
/// Note: using timestamp to make the `id` dynamic has the potential of reusing
/// a previously used `id` in cases where the clock is reset in the past. We believe this
/// very rare and things should just work fine.
#[derive(Clone, Hash, Eq, PartialEq, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct NodeId {
    // The unique identifier of this node in the cluster.
    pub id: String,
    // The SocketAddr other peers should use to communicate.
    pub gossip_public_address: SocketAddr,
}

impl NodeId {
    pub fn new(id: String, gossip_public_address: SocketAddr) -> Self {
        Self {
            id,
            gossip_public_address,
        }
    }

    #[cfg(test)]
    pub fn for_test_localhost(port: u16) -> Self {
        NodeId::new(
            format!("node-{port}"),
            ([127u8, 0u8, 0u8, 0u8], port).into(),
        )
    }

    /// Returns the gossip public port. Useful for test assert only.
    #[cfg(test)]
    pub fn public_port(&self) -> u16 {
        self.gossip_public_address.port()
    }
}

/// A versioned value for a given Key-value pair.
#[derive(Serialize, Deserialize, Clone, Eq, PartialEq, Debug)]
pub struct VersionedValue {
    pub value: String,
    pub version: Version,
}

pub struct Chitchat {
    mtu: usize,
    listen_address: SocketAddr,
    self_node_id: NodeId,
    cluster_id: String,
    cluster_state: ClusterState,
    heartbeat: u64,
    /// The failure detector instance.
    failure_detector: FailureDetector,
    /// A notification channel (sender) for sending live nodes change feed.
    live_nodes_watcher_tx: watch::Sender<HashSet<NodeId>>,
    /// A notification channel (receiver) for receiving live nodes change feed.
    live_nodes_watcher_rx: watch::Receiver<HashSet<NodeId>>,
}

impl Chitchat {
    pub fn with_node_id_and_seeds(
        self_node_id: NodeId,
        seed_addrs: watch::Receiver<HashSet<SocketAddr>>,
        listen_address: SocketAddr,
        cluster_id: String,
        initial_key_values: Vec<(impl ToString, impl ToString)>,
        failure_detector_config: FailureDetectorConfig,
    ) -> Self {
        let (live_nodes_watcher_tx, live_nodes_watcher_rx) = watch::channel(HashSet::new());
        let mut chitchat = Chitchat {
            mtu: 60_000,
            listen_address,
            self_node_id,
            cluster_id,
            cluster_state: ClusterState::with_seed_addrs(seed_addrs),
            heartbeat: 0,
            failure_detector: FailureDetector::new(failure_detector_config),
            live_nodes_watcher_tx,
            live_nodes_watcher_rx,
        };

        let self_node_state = chitchat.self_node_state();

        // Immediately mark node as alive to ensure it responds to SYNs.
        self_node_state.set(HEARTBEAT_KEY, 0);

        // Set initial key/value pairs.
        for (key, value) in initial_key_values {
            self_node_state.set(key, value);
        }

        chitchat
    }

    pub fn set_mtu(&mut self, mtu: usize) {
        self.mtu = mtu;
    }

    pub fn create_syn_message(&mut self) -> ChitchatMessage {
        let digest = self.compute_digest();
        ChitchatMessage::Syn {
            cluster_id: self.cluster_id.clone(),
            digest,
        }
    }

    pub fn process_message(&mut self, msg: ChitchatMessage) -> Option<ChitchatMessage> {
        match msg {
            ChitchatMessage::Syn { cluster_id, digest } => {
                if cluster_id != self.cluster_id {
                    warn!(
                        cluster_id = %cluster_id,
                        "rejecting syn message with mismatching cluster name"
                    );
                    return Some(ChitchatMessage::BadCluster);
                }

                let self_digest = self.compute_digest();
                let dead_nodes = self.dead_nodes().collect::<HashSet<_>>();
                let delta = self.cluster_state.compute_delta(
                    &digest,
                    self.mtu - 1 - self_digest.serialized_len(),
                    dead_nodes,
                );
                self.report_to_failure_detector(&delta);
                Some(ChitchatMessage::SynAck {
                    delta,
                    digest: self_digest,
                })
            }
            ChitchatMessage::SynAck { digest, delta } => {
                self.report_to_failure_detector(&delta);
                self.cluster_state.apply_delta(delta);
                let dead_nodes = self.dead_nodes().collect::<HashSet<_>>();
                let delta = self
                    .cluster_state
                    .compute_delta(&digest, self.mtu - 1, dead_nodes);
                Some(ChitchatMessage::Ack { delta })
            }
            ChitchatMessage::Ack { delta } => {
                self.report_to_failure_detector(&delta);
                self.cluster_state.apply_delta(delta);
                None
            }
            ChitchatMessage::BadCluster => {
                warn!("message rejected by peer: cluster name mismatch");
                None
            }
        }
    }

    fn report_to_failure_detector(&mut self, delta: &Delta) {
        for (node_id, node_delta) in &delta.node_deltas {
            let local_max_version = self
                .cluster_state
                .node_states
                .get(node_id)
                .map(|node_state| node_state.max_version)
                .unwrap_or(0);

            let delta_max_version = node_delta.max_version();
            if local_max_version < delta_max_version {
                self.failure_detector.report_heartbeat(node_id);
            }
        }
    }

    /// Checks and marks nodes as dead or live.
    pub fn update_nodes_liveliness(&mut self) {
        let live_nodes_before = self.live_nodes().cloned().collect::<HashSet<_>>();
        let cluster_nodes = self
            .cluster_state
            .nodes()
            .filter(|node_id| *node_id != &self.self_node_id)
            .collect::<Vec<_>>();
        for node_id in &cluster_nodes {
            self.failure_detector.update_node_liveliness(node_id);
        }

        let live_nodes_after = self.live_nodes().cloned().collect::<HashSet<_>>();
        if live_nodes_before != live_nodes_after {
            debug!(current_node = ?self.self_node_id, live_nodes = ?live_nodes_after, "nodes status changed");
            if self.live_nodes_watcher_tx.send(live_nodes_after).is_err() {
                error!(current_node = ?self.self_node_id, "error while reporting membership change event.")
            }
        }

        // Perform garbage collection.
        let garbage_collected_nodes = self.failure_detector.garbage_collect();
        for node_id in garbage_collected_nodes.iter() {
            self.cluster_state.remove_node(node_id)
        }
    }

    pub fn node_state(&self, node_id: &NodeId) -> Option<&NodeState> {
        self.cluster_state.node_state(node_id)
    }

    pub fn self_node_state(&mut self) -> &mut NodeState {
        self.cluster_state.node_state_mut(&self.self_node_id)
    }

    /// Retrieves the list of all live nodes.
    pub fn live_nodes(&self) -> impl Iterator<Item = &NodeId> {
        self.failure_detector.live_nodes()
    }

    /// Retrieve the list of all dead nodes.
    pub fn dead_nodes(&self) -> impl Iterator<Item = &NodeId> {
        self.failure_detector.dead_nodes()
    }

    /// Retrieve a list of seed nodes.
    pub fn seed_nodes(&self) -> HashSet<SocketAddr> {
        self.cluster_state.seed_addrs()
    }

    pub fn self_node_id(&self) -> &NodeId {
        &self.self_node_id
    }

    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// Computes digest.
    ///
    /// This method also increments the heartbeat, to force the presence
    /// of at least one update, and have the node liveliness propagated
    /// through the cluster.
    fn compute_digest(&mut self) -> Digest {
        // Ensure for every reply from this node, at least the heartbeat is changed.
        self.heartbeat += 1;
        let heartbeat = self.heartbeat;
        self.self_node_state().set(HEARTBEAT_KEY, heartbeat);
        let dead_nodes: HashSet<_> = self.dead_nodes().collect();
        self.cluster_state.compute_digest(dead_nodes)
    }

    pub fn cluster_state(&self) -> &ClusterState {
        &self.cluster_state
    }

    /// Returns a watch stream for monitoring changes on the cluster's live nodes.
    pub fn live_nodes_watcher(&self) -> WatchStream<HashSet<NodeId>> {
        WatchStream::new(self.live_nodes_watcher_rx.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::ops::{Add, RangeInclusive};
    use std::sync::Arc;
    use std::time::Duration;

    use mock_instant::MockClock;
    use tokio::sync::Mutex;
    use tokio::time;
    use tokio_stream::wrappers::IntervalStream;
    use tokio_stream::StreamExt;

    use super::*;
    use crate::server::ChitchatServer;

    const DEAD_NODE_GRACE_PERIOD: Duration = Duration::from_secs(25);

    fn run_chitchat_handshake(initiating_node: &mut Chitchat, peer_node: &mut Chitchat) {
        let syn_message = initiating_node.create_syn_message();
        let syn_ack_message = peer_node.process_message(syn_message).unwrap();
        let ack_message = initiating_node.process_message(syn_ack_message).unwrap();
        assert!(peer_node.process_message(ack_message).is_none());
    }

    fn assert_cluster_state_eq(lhs: &NodeState, rhs: &NodeState) {
        assert_eq!(lhs.key_values.len(), rhs.key_values.len());
        for (key, value) in &lhs.key_values {
            if key == HEARTBEAT_KEY {
                // we ignore the heartbeat key
                continue;
            }
            assert_eq!(rhs.key_values.get(key), Some(value));
        }
    }

    fn assert_nodes_sync(nodes: &[&Chitchat]) {
        let first_node_states = &nodes[0].cluster_state.node_states;
        for other_node in nodes.iter().skip(1) {
            let node_states = &other_node.cluster_state.node_states;
            assert_eq!(first_node_states.len(), node_states.len());
            for (key, value) in first_node_states {
                assert_cluster_state_eq(value, node_states.get(key).unwrap());
            }
        }
    }

    async fn start_node(node_id: NodeId, seeds: &[String]) -> ChitchatServer {
        ChitchatServer::spawn(
            node_id.clone(),
            seeds,
            node_id.gossip_public_address,
            "test-cluster".to_string(),
            Vec::<(&str, &str)>::new(),
            FailureDetectorConfig {
                dead_node_grace_period: DEAD_NODE_GRACE_PERIOD,
                ..Default::default()
            },
        )
        .await
    }

    async fn setup_nodes(port_range: RangeInclusive<u16>) -> Vec<ChitchatServer> {
        let node_ids: Vec<NodeId> = port_range.map(NodeId::for_test_localhost).collect();
        let node_without_seed = start_node(node_ids[0].clone(), &[]).await;
        let mut chitchat_servers: Vec<ChitchatServer> = vec![node_without_seed];
        for node_id in &node_ids[1..] {
            let seeds = node_ids
                .iter()
                .filter(|&peer_id| peer_id != node_id)
                .map(|peer_id| peer_id.gossip_public_address.to_string())
                .collect::<Vec<_>>();
            chitchat_servers.push(start_node(node_id.clone(), &seeds).await);
        }
        // Make sure the failure detector's fake clock moves forward.
        tokio::spawn(async {
            let mut ticker = IntervalStream::new(time::interval(Duration::from_millis(50)));
            while ticker.next().await.is_some() {
                MockClock::advance(Duration::from_millis(50));
            }
        });
        chitchat_servers
    }

    async fn shutdown_nodes(nodes: Vec<ChitchatServer>) -> anyhow::Result<()> {
        for node in nodes {
            node.shutdown().await?;
        }
        Ok(())
    }

    async fn wait_for_chitchat_state(
        chitchat: Arc<Mutex<Chitchat>>,
        expected_node_count: usize,
        expected_nodes: &[NodeId],
    ) {
        let mut live_nodes_watcher = chitchat
            .lock()
            .await
            .live_nodes_watcher()
            .skip_while(|live_nodes| live_nodes.len() != expected_node_count);
        tokio::time::timeout(Duration::from_secs(50), async move {
            let live_nodes = live_nodes_watcher.next().await.unwrap();
            assert_eq!(
                live_nodes,
                expected_nodes.iter().cloned().collect::<HashSet<_>>()
            );
        })
        .await
        .unwrap();
    }

    #[test]
    fn test_chitchat_handshake() {
        let node_id_1 = NodeId::for_test_localhost(10_001);
        let empty_seeds = watch::channel(Default::default()).1;
        let mut node1 = Chitchat::with_node_id_and_seeds(
            node_id_1.clone(),
            empty_seeds.clone(),
            node_id_1.gossip_public_address,
            "test-cluster".to_string(),
            vec![("key1a", "1"), ("key2a", "2")],
            FailureDetectorConfig::default(),
        );
        let node_id_2 = NodeId::for_test_localhost(10_002);
        let mut node2 = Chitchat::with_node_id_and_seeds(
            node_id_2.clone(),
            empty_seeds,
            node_id_2.gossip_public_address,
            "test-cluster".to_string(),
            vec![("key1b", "1"), ("key2b", "2")],
            FailureDetectorConfig::default(),
        );
        run_chitchat_handshake(&mut node1, &mut node2);
        assert_nodes_sync(&[&node1, &node2]);
        // useless handshake
        run_chitchat_handshake(&mut node1, &mut node2);
        assert_nodes_sync(&[&node1, &node2]);
        {
            let state1 = node1.self_node_state();
            state1.set("key1a", "3");
            state1.set("key1c", "4");
        }
        run_chitchat_handshake(&mut node1, &mut node2);
        assert_nodes_sync(&[&node1, &node2]);
    }

    #[tokio::test]
    async fn test_multiple_nodes() -> anyhow::Result<()> {
        let nodes = setup_nodes(20001..=20005).await;

        let node = nodes.get(1).unwrap();
        assert_eq!(node.node_id().public_port(), 20002);
        wait_for_chitchat_state(
            node.chitchat(),
            4,
            &[
                NodeId::for_test_localhost(20001),
                NodeId::for_test_localhost(20003),
                NodeId::for_test_localhost(20004),
                NodeId::for_test_localhost(20005),
            ],
        )
        .await;

        shutdown_nodes(nodes).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_node_goes_from_live_to_down_to_live() -> anyhow::Result<()> {
        let mut nodes = setup_nodes(30001..=30006).await;
        let node = &nodes[1];
        assert_eq!(node.node_id().gossip_public_address.port(), 30002);
        wait_for_chitchat_state(
            node.chitchat(),
            5,
            &[
                NodeId::for_test_localhost(30001),
                NodeId::for_test_localhost(30003),
                NodeId::for_test_localhost(30004),
                NodeId::for_test_localhost(30005),
                NodeId::for_test_localhost(30006),
            ],
        )
        .await;

        // Take down node at localhost:30003
        let node = nodes.remove(2);
        assert_eq!(node.node_id().public_port(), 30003);
        node.shutdown().await.unwrap();

        let node = nodes.get(1).unwrap();
        assert_eq!(node.node_id().public_port(), 30002);
        wait_for_chitchat_state(
            node.chitchat(),
            4,
            &[
                NodeId::for_test_localhost(30001),
                NodeId::for_test_localhost(30004),
                NodeId::for_test_localhost(30005),
                NodeId::for_test_localhost(30006),
            ],
        )
        .await;

        // Restart node at localhost:10003
        let node_3 = NodeId::for_test_localhost(30003);
        nodes.push(
            start_node(
                node_3,
                &[NodeId::for_test_localhost(30_001)
                    .gossip_public_address
                    .to_string()],
            )
            .await,
        );

        let node = nodes.get(1).unwrap();
        assert_eq!(node.node_id().public_port(), 30002);
        wait_for_chitchat_state(
            node.chitchat(),
            5,
            &[
                NodeId::for_test_localhost(30001),
                NodeId::for_test_localhost(30003),
                NodeId::for_test_localhost(30004),
                NodeId::for_test_localhost(30005),
                NodeId::for_test_localhost(30006),
            ],
        )
        .await;

        shutdown_nodes(nodes).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_dead_node_should_not_be_gossiped_when_node_joins() -> anyhow::Result<()> {
        let mut nodes = setup_nodes(40001..=40004).await;
        {
            let node2 = nodes.get(1).unwrap();
            assert_eq!(node2.node_id().public_port(), 40002);
            wait_for_chitchat_state(
                node2.chitchat(),
                3,
                &[
                    NodeId::for_test_localhost(40001),
                    NodeId::for_test_localhost(40003),
                    NodeId::for_test_localhost(40004),
                ],
            )
            .await;
        }

        // Take down node at localhost:40003
        let node3 = nodes.remove(2);
        let node3_id = node3.node_id().clone();
        assert_eq!(node3.node_id().public_port(), 40003);
        node3.shutdown().await.unwrap();

        {
            let node2 = nodes.get(1).unwrap();
            assert_eq!(node2.node_id().public_port(), 40002);
            wait_for_chitchat_state(
                node2.chitchat(),
                2,
                &[
                    NodeId::for_test_localhost(40001),
                    NodeId::for_test_localhost(40004),
                ],
            )
            .await;
        }

        // Restart node at localhost:40003 with new name
        let addr_40003 = node3_id.gossip_public_address;
        let new_node_chitchat = ChitchatServer::spawn(
            NodeId::new("new_node".to_string(), addr_40003),
            &[NodeId::for_test_localhost(40_001)
                .gossip_public_address
                .to_string()],
            addr_40003,
            "test-cluster".to_string(),
            Vec::<(&str, &str)>::new(),
            FailureDetectorConfig::default(),
        )
        .await;

        wait_for_chitchat_state(
            new_node_chitchat.chitchat(),
            3,
            &[
                NodeId::for_test_localhost(40_001),
                NodeId::for_test_localhost(40_002),
                NodeId::for_test_localhost(40_004),
            ],
        )
        .await;

        nodes.push(new_node_chitchat);
        shutdown_nodes(nodes).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_network_partition_nodes() -> anyhow::Result<()> {
        let port_range = 11_001u16..=11_006;
        let nodes = setup_nodes(port_range.clone()).await;

        // Check nodes know each other.
        for node in nodes.iter() {
            let expected_peers: Vec<NodeId> = port_range
                .clone()
                .filter(|peer_port| *peer_port != node.node_id().public_port())
                .map(NodeId::for_test_localhost)
                .collect::<Vec<_>>();
            wait_for_chitchat_state(node.chitchat(), 5, &expected_peers).await;
        }

        shutdown_nodes(nodes).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_dead_node_garbage_collection() -> anyhow::Result<()> {
        let mut nodes = setup_nodes(60001..=60006).await;

        let node = nodes.get(1).unwrap();
        assert_eq!(node.node_id().public_port(), 60002);
        wait_for_chitchat_state(
            node.chitchat(),
            5,
            &[
                NodeId::for_test_localhost(60_001),
                NodeId::for_test_localhost(60_003),
                NodeId::for_test_localhost(60_004),
                NodeId::for_test_localhost(60_005),
                NodeId::for_test_localhost(60_006),
            ],
        )
        .await;

        // Take down node at localhost:60003
        let node = nodes.remove(2);
        assert_eq!(node.node_id().public_port(), 60003);
        node.shutdown().await.unwrap();

        let node = nodes.get(1).unwrap();
        assert_eq!(node.node_id().public_port(), 60002);
        wait_for_chitchat_state(
            node.chitchat(),
            4,
            &[
                NodeId::for_test_localhost(60_001),
                NodeId::for_test_localhost(60_004),
                NodeId::for_test_localhost(60_005),
                NodeId::for_test_localhost(60_006),
            ],
        )
        .await;

        // Dead node should still be known to the cluster.
        let dead_node_id = NodeId::for_test_localhost(60003);
        for node in &nodes {
            assert!(node
                .chitchat()
                .lock()
                .await
                .node_state(&dead_node_id)
                .is_some());
        }

        // Wait a bit more than `dead_node_grace_period` since all nodes will not
        // notice cluster change at the same time.
        let wait_for = DEAD_NODE_GRACE_PERIOD.add(Duration::from_secs(20));
        time::sleep(wait_for).await;

        // Dead node should no longer be known to the cluster.
        for node in &nodes {
            assert!(node
                .chitchat()
                .lock()
                .await
                .node_state(&dead_node_id)
                .is_none());
        }

        shutdown_nodes(nodes).await?;
        Ok(())
    }
}
