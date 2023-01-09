use std::thread::JoinHandle;

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

pub async fn new_member_handle() -> JoinHandle<()> {
    let member_handle = tokio::spawn(async move {
        let keychain = Keychain::new(args.n_members.into(), args.my_id.into());
        let config = aleph_bft::default_config(args.n_members.into(), args.my_id.into(), 0);
        let backup_loader = Loader::new(vec![]);
        let backup_saver = Saver::new(Arc::new(Mutex::new(vec![])));
        let local_io = aleph_bft::LocalIO::new(
            data_provider,
            finalization_handler,
            backup_saver,
            backup_loader,
        );
        run_session(
            config,
            local_io,
            network,
            keychain,
            Spawner {},
            member_terminator,
        )
        .await
    });

    member_handle
}
