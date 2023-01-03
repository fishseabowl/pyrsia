/*
   Copyright 2021 JFrog Ltd

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

use std::collections::{HashMap, HashSet};

use crate::{
    crypto::hash_algorithm::HashDigest,
    signature::{MultiSignature, Signature},
    structures::{block::Block, header::Address},
};

use super::dataio;
use aleph_bft::{NodeIndex, SpawnHandle, TaskHandle};
use futures::{
    channel::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    Future,
};
use log::{trace, warn};
use parity_scale_codec::{Decode, Encode};

pub type NetworkData = aleph_bft::NetworkData<HashDigest, Block, Signature, MultiSignature>;

pub struct Network {
    msg_to_manager_tx: mpsc::UnboundedSender<(NetworkData, Recipient)>,
    msg_from_manager_rx: mpsc::UnboundedReceiver<NetworkData>,
}

#[async_trait::async_trait]
impl aleph_bft::Network<NetworkData> for Network {
    fn send(&self, data: NetworkData, recipient: aleph_bft::Recipient) {
        trace!("Sending a message to: {:?}", recipient);
        if let Err(e) = self.msg_to_manager_tx.unbounded_send((data, recipient)) {
            warn!("Failed network send: {:?}", e);
        }
    }
    async fn next_event(&mut self) -> Option<NetworkData> {
        let msg = self.msg_from_manager_rx.next().await;
        msg.map(|m| {
            trace!(
                "New event received of network data {}",
                hex::encode(m.encode())
            );
            m
        })
    }
}

#[derive(Clone, Decode, Encode, Debug)]
enum BlockChainMessage {
    Consensus(NetworkData),
    Block(Block),
}

#[derive(Clone, Eq, PartialEq, Hash, Debug, Decode, Encode)]
pub enum Recipient {
    Everyone,
    Node(NodeIndex),
    AuthorizedNodeList(Vec<NodeIndex>),
}

pub struct NetworkManager {
    id: NodeIndex,
    address: Address,
    addresses: HashMap<NodeIndex, Address>,
    bootnodes: HashSet<NodeIndex>,
    n_nodes: usize,
    consensus_tx: UnboundedSender<NetworkData>,
    consensus_rx: UnboundedReceiver<(NetworkData, Recipient)>,
    block_tx: UnboundedSender<Block>,
    block_rx: UnboundedReceiver<Block>,
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default, Decode, Encode)]
pub struct Spawner;

impl SpawnHandle for Spawner {
    fn spawn(&self, _name: &str, task: impl Future<Output = ()> + Send + 'static) {
        tokio::spawn(task);
    }

    fn spawn_essential(
        &self,
        _: &str,
        task: impl Future<Output = ()> + Send + 'static,
    ) -> TaskHandle {
        let (res_tx, res_rx) = oneshot::channel();
        tokio::spawn(async move {
            task.await;
            res_tx.send(()).expect("We own the rx.");
        });
        Box::pin(async move { res_rx.await.map_err(|_| ()) })
    }
}

impl Spawner {
    pub fn new() -> Self {
        Spawner {}
    }
}
