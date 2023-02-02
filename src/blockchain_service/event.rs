/*
   Copyright 2022 JFrog Ltd

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

use crate::artifact_service::service::ArtifactService;
use crate::blockchain_service::service::BlockchainService;
use libp2p::PeerId;
use log::{debug, error, warn};
use pyrsia_blockchain_network::error::BlockchainError;
use pyrsia_blockchain_network::structures::block::Block;
use pyrsia_blockchain_network::structures::header::Ordinal;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
pub enum BlockchainEvent {
    // AddBlock is used to create a new block in the local blockchain
    AddBlock {
        payload: Vec<u8>,
        sender: oneshot::Sender<Result<Ordinal, BlockchainError>>,
    },
    // FetchBlocksLocal is used to fetch blocks from the local blockchain
    FetchBlocksLocal {
        start: Ordinal,
        end: Ordinal,
        sender: oneshot::Sender<Result<Vec<Block>, BlockchainError>>,
    },
    // QueryBlockOrdinalLocal is used to query last ordinal in the local blockchain
    QueryBlockOrdinalLocal {
        sender: oneshot::Sender<Result<Ordinal, BlockchainError>>,
    },
    // HandleBlockBroadcast is used to broadcast a block to all nodes
    HandleBlockBroadcast {
        block_ordinal: Ordinal,
        block: Box<Block>,
        sender: oneshot::Sender<Result<(), BlockchainError>>,
    },
    // HandlePullBlocks is used to pull blocks from the remote node
    HandlePullBlocks { 
        start: Ordinal,
        end: Ordinal,
        peer_id: PeerId,
        sender: oneshot::Sender<Result<Vec<Block>, BlockchainError>>,
    },
    // HandleQueryBlockOrdinal is used to query last blockchain ordinal in the remote node
    HandleQueryBlockOrdinal {
        peer_id: PeerId,
        sender: oneshot::Sender<Result<Ordinal, BlockchainError>>,
    },
}

#[derive(Clone)]
pub struct BlockchainEventClient {
    blockchain_event_sender: mpsc::Sender<BlockchainEvent>,
}

impl BlockchainEventClient {
    pub fn new(blockchain_event_sender: mpsc::Sender<BlockchainEvent>) -> Self {
        Self {
            blockchain_event_sender,
        }
    }

    /// Create a new block on the local node
    pub async fn add_block(&self, payload: Vec<u8>) -> Result<Ordinal, BlockchainError> {
        let (sender, receiver) = oneshot::channel();
        self.blockchain_event_sender
            .send(BlockchainEvent::AddBlock { payload, sender })
            .await
            .unwrap_or_else(|e| {
                error!("Error blockchain_event_sender. {:#?}", e);
            });
        receiver.await.map_err(BlockchainError::ChannelClosed)?
    }

    /// Fetch block data from the local node
    pub async fn fetch_blocks_local(
        &self,
        start: Ordinal,
        end: Ordinal, //include end ordinal block data
    ) -> Result<Vec<Block>, BlockchainError> {
        let (sender, receiver) = oneshot::channel();
        self.blockchain_event_sender
            .send(BlockchainEvent::FetchBlocksLocal { start, end, sender })
            .await
            .unwrap_or_else(|e| {
                error!("Error blockchain_event_sender. {:#?}", e);
            });
        receiver.await.map_err(BlockchainError::ChannelClosed)?
    }

    /// Query the last block ordinal from the remote node
    pub async fn query_block_ordinal_local(&self) -> Result<Ordinal, BlockchainError> {
        let (sender, receiver) = oneshot::channel();
        self.blockchain_event_sender
            .send(BlockchainEvent::QueryBlockOrdinalLocal {sender})
            .await
            .unwrap_or_else(|e| {
                error!("Error blockchain_event_sender. {:#?}", e);
            });
        receiver.await.map_err(BlockchainError::ChannelClosed)?
    }

    /// Broadcast a block to all nodes
    pub async fn handle_block_broadcast(
        &self,
        block_ordinal: Ordinal,
        block: Block,
    ) -> Result<(), BlockchainError> {
        let (sender, receiver) = oneshot::channel();
        self.blockchain_event_sender
            .send(BlockchainEvent::HandleBlockBroadcast {
                block_ordinal,
                block: Box::new(block),
                sender,
            })
            .await
            .unwrap_or_else(|e| {
                error!("Error blockchain_event_sender. {:#?}", e);
            });
        receiver.await.map_err(BlockchainError::ChannelClosed)?
    }

    /// Pull block data from the remote node
    pub async fn hanlde_pull_blocks(
        &self,
        start: Ordinal,
        end: Ordinal,
        peer_id: &PeerId,
    ) -> Result<Vec<Block>, BlockchainError> {
        let (sender, receiver) = oneshot::channel();
        self.blockchain_event_sender
            .send(BlockchainEvent::HandlePullBlocks {
                start,
                end,
                peer_id: *peer_id,
                sender,
            })
            .await
            .unwrap_or_else(|e| {
                error!("Error blockchain_event_sender. {:#?}", e);
            });
        receiver.await.map_err(BlockchainError::ChannelClosed)?
    }

    /// Query the last block ordinal from the remote node
    pub async fn handle_query_block_ordinal(&self, peer_id: PeerId) -> Result<Ordinal, BlockchainError> {
        let (sender, receiver) = oneshot::channel();
        self.blockchain_event_sender
            .send(BlockchainEvent::HandleQueryBlockOrdinal {peer_id, sender})
            .await
            .unwrap_or_else(|e| {
                error!("Error blockchain_event_sender. {:#?}", e);
            });
        receiver.await.map_err(BlockchainError::ChannelClosed)?
    }
}

pub struct BlockchainEventLoop {
    artifact_service: ArtifactService,
    blockchain_service: BlockchainService,
    blockchain_event_receiver: mpsc::Receiver<BlockchainEvent>,
}

impl BlockchainEventLoop {
    pub fn new(
        artifact_service: ArtifactService,
        blockchain_service: BlockchainService,
        blockchain_event_receiver: mpsc::Receiver<BlockchainEvent>,
    ) -> Self {
        Self {
            artifact_service,
            blockchain_service,
            blockchain_event_receiver,
        }
    }

    pub fn blockchain_service(&self) -> &BlockchainService {
        &self.blockchain_service
    }

    pub async fn run(mut self) {
        loop {
            match self.blockchain_event_receiver.recv().await {
                Some(blockchain_event) => {
                    self.handle_blockchain_event(blockchain_event).await;
                }
                None => {
                    warn!("Got empty build event");
                    return;
                }
            }
        }
    }

    async fn handle_blockchain_event(&mut self, blockchain_event: BlockchainEvent) {
        debug!("Handle BlockchainEvent: {:?}", blockchain_event);
        match blockchain_event {
            BlockchainEvent::AddBlock { payload, sender } => {
                let result = self.blockchain_service.add_payload(payload).await;
                sender.send(result).unwrap_or_else(|e| {
                    error!("add block error. {:#?}", e);
                });
            }
            BlockchainEvent::PullBlocksFromPeer { start, end, peer_id, sender } => {
                let result = self
                    .blockchain_service
                    .pull_blocks_from_peer(&peer_id, start, end )
                    .await;
                sender.send(result).unwrap_or_else(|e| {
                    error!("pull blocks from peer error. {:#?}", e);
                });
            }
            BlockchainEvent::FetchBlocksLocal { start, end, sender } => {
                debug!("Handling pull blocks from {:?} to {:?} ", start, end);

                let result = self.blockchain_service.fetch_blocks(start, end).await;
                sender.send(result).unwrap_or_else(|e| {
                    error!("pull blocks local error. {:#?}", e);
                });
            }
            BlockchainEvent::HandleBlockBroadcast {
                block_ordinal,
                block,
                sender,
            } => {
                debug!("Handling broadcast blocks");

                let payloads = block.fetch_payload();
                if let Err(e) = self
                    .blockchain_service
                    .add_block_local(block_ordinal, block)
                    .await
                {
                    sender.send(Err(e.into())).unwrap_or_else(|e| {
                        error!("block broadcast error. {:#?}", e);
                    });
                } else if let Err(e) = self.artifact_service.handle_block_added(payloads).await {
                    sender.send(Err(e)).unwrap_or_else(|e| {
                        error!("block broadcast error. {:#?}", e);
                    });
                } else {
                    sender.send(Ok(())).unwrap_or_else(|e| {
                        error!("block broadcast error. {:#?}", e);
                    });
                }
            }
            BlockchainEvent::HandlePullBlocks { start, end, sender } => {
                debug!("Handling pull blocks from {:?} to {:?} ", start, end);

                let result = self.blockchain_service.pull_blocks(start, end).await;
                sender
                    .send(result.map_err(|e| e.into()))
                    .unwrap_or_else(|e| {
                        error!("block broadcast error. {:#?}", e);
                    });
            }
            BlockchainEvent::HandleQueryBlockOrdinal { sender } => {
                debug!("Handling query block ordinal");

                match self.blockchain_service.query_last_block().await {
                    Some(latest_block) => {
                        let highest_ordinal = latest_block.header.ordinal;
                        sender.send(Ok(highest_ordinal)).unwrap_or_else(|e| {
                            error!("block broadcast error. {:#?}", e);
                        });
                    }
                    None => {
                        sender
                            .send(Err(BlockchainError::InvalidBlockchainLength(0).into()))
                            .unwrap_or_else(|e| {
                                error!("block broadcast error. {:#?}", e);
                            });
                    }
                }
            }
        }
    }
}
