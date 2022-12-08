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

use aleph_bft::{NodeCount, NodeIndex};
use async_trait::async_trait;
use libp2p::identity::ed25519::PublicKey;
use log::trace;

use super::authority_pen::AuthorityPen;
use super::authority_verifier::AuthorityVerifier;
use super::signature::Signature;

#[derive(Clone)]
pub struct Authority {
    count: NodeCount,
    authority_pen: AuthorityPen,
    authority_verifier: AuthorityVerifier,
}

impl Authority {
    pub fn new(authority_pen: AuthorityPen, authority_verifier: AuthorityVerifier) -> Self {
        let mut kb = Self {
            count: authority_verifier.node_count(),
            authority_pen,
            authority_verifier,
        };
        // Record the pen as a known authority -- always trust yourself
        kb.add_new_authority(kb.authority_pen.index(), kb.authority_pen.public());
        kb
    }

    pub fn add_new_authority(&mut self, node_index: NodeIndex, public_key: PublicKey) {
        self.authority_verifier
            .add_new_authority(node_index, public_key);

        self.count = self.authority_verifier.node_count();
    }
}

#[async_trait]
impl aleph_bft::Keychain for Authority {
    type Signature = Signature;

    fn node_count(&self) -> NodeCount {
        self.count
    }

    async fn sign(&self, msg: &[u8]) -> Self::Signature {
        trace!("🖋️ {:?} signing message", self.authority_pen.index());
        self.authority_pen.sign(msg)
    }

    fn verify(&self, msg: &[u8], sgn: &Self::Signature, index: NodeIndex) -> bool {
        trace!(
            "🔎 {:?} verifying message and signature from {:?}",
            self.authority_pen.index(),
            index
        );
        self.authority_verifier.verify(msg, sgn, index)
    }
}

impl aleph_bft::Index for Authority {
    fn index(&self) -> NodeIndex {
        self.authority_pen.index()
    }
}

#[cfg(test)]
#[cfg(not(tarpaulin_include))]
mod tests {
    use super::*;
    use aleph_bft::Keychain;
    use libp2p::core::identity::ed25519::Keypair;

    #[tokio::test]
    async fn test_key_box_self_signed() {
        let authority = Authority::new(
            AuthorityPen::new(0.into(), Keypair::generate()),
            AuthorityVerifier::new(),
        );
        let sign: Signature = authority.sign(b"hello world!").await;

        assert!(!authority.verify(b"hello world", &sign, 0.into()));
    }
}
