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

use aleph_bft::NodeIndex;
use async_trait::async_trait;
use codec::{Decode, Encode};
use futures::{channel::mpsc::unbounded, future::pending};
use log::{error, info};

use crate::structures::block::Block;

type Receiver<T> = futures::channel::mpsc::UnboundedReceiver<T>;
type Sender<T> = futures::channel::mpsc::UnboundedSender<T>;

pub type Data = Block;

#[derive(Clone, Eq, PartialEq, Hash, Debug, Default, Decode, Encode)]
pub struct DataStore {
    id: NodeIndex,
    block: Block,
}

impl DataStore {
    pub fn new(id: NodeIndex, block: Block) -> Self {
        Self { id, block }
    }
}

#[async_trait]
impl aleph_bft::DataProvider<Data> for DataStore {
    async fn get_data(&mut self) -> Option<Data> {
        Some(self.block)
    }
}

#[derive(Clone)]
pub struct FinalizationHandler {
    tx: Sender<Data>,
}

impl aleph_bft::FinalizationHandler<Data> for FinalizationHandler {
    fn data_finalized(&mut self, d: Data) {
        if let Err(e) = self.tx.unbounded_send(d) {
            error!(target: "finalization-handler", "Error when sending data from FinalizationHandler {:?}.", e);
        }
    }
}

impl FinalizationHandler {
    pub fn new() -> (Self, Receiver<Data>) {
        let (tx, rx) = unbounded();
        (Self { tx }, rx)
    }
}
#[derive(Clone, Debug, Default)]
pub struct Saver {
    data: Arc<Mutex<Vec<u8>>>,
}

impl Saver {
    pub fn new(data: Arc<Mutex<Vec<u8>>>) -> Self {
        Self { data }
    }
}

impl Write for Saver {
    fn write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        self.data.lock().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

pub type Loader = Cursor<Vec<u8>>;
