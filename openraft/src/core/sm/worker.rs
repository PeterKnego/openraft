use display_more::DisplayOptionExt;
use futures_util::TryStreamExt;
use tracing::Instrument;

use crate::RaftLogReader;
use crate::RaftSnapshotBuilder;
use crate::RaftTypeConfig;
use crate::StorageError;
use crate::async_runtime::MpscReceiver;
use crate::async_runtime::OneshotSender;
use crate::core::ApplyResult;
use crate::core::notification::Notification;
use crate::core::sm::Command;
use crate::core::sm::CommandResult;
use crate::core::sm::Response;
use crate::core::sm::handle::Handle;
use crate::entry::RaftEntry;
use crate::errors::StorageIOResult;
use crate::raft::responder::core_responder::CoreResponder;
#[cfg(doc)]
use crate::storage::RaftLogStorage;
use crate::storage::RaftStateMachine;
use crate::storage::v2::entry_responder::EntryResponderBuilder;
use crate::type_config::TypeConfigExt;
use crate::type_config::alias::JoinHandleOf;
use crate::type_config::alias::LogIdOf;
use crate::type_config::alias::MpscReceiverOf;
use crate::type_config::alias::MpscSenderOf;
use crate::type_config::alias::OneshotSenderOf;
use crate::type_config::alias::SnapshotOf;
use crate::type_config::async_runtime::mpsc::MpscSender;

pub(crate) struct Worker<C, SM, LR>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
    LR: RaftLogReader<C>,
{
    /// The application state machine implementation.
    state_machine: SM,

    /// Read logs from the [`RaftLogStorage`] implementation to apply them to the state machine.
    log_reader: LR,

    /// Read command from RaftCore to execute.
    cmd_rx: MpscReceiverOf<C, Command<C, SM>>,

    /// Send back the result of the command to RaftCore.
    resp_tx: MpscSenderOf<C, Notification<C>>,
}

impl<C, SM, LR> Worker<C, SM, LR>
where
    C: RaftTypeConfig,
    SM: RaftStateMachine<C>,
    LR: RaftLogReader<C>,
{
    /// Spawn a new state machine worker, return a controlling handle.
    pub(crate) fn spawn(
        state_machine: SM,
        log_reader: LR,
        resp_tx: MpscSenderOf<C, Notification<C>>,
        state_machine_channel_size: usize,
        span: tracing::Span,
    ) -> Handle<C, SM> {
        let (cmd_tx, cmd_rx) = C::mpsc(state_machine_channel_size);

        let worker = Worker {
            state_machine,
            log_reader,
            cmd_rx,
            resp_tx,
        };

        let join_handle = worker.do_spawn(span);

        Handle { cmd_tx, join_handle }
    }

    fn do_spawn(mut self, span: tracing::Span) -> JoinHandleOf<C, ()> {
        let fu = async move {
            let res = self.worker_loop().await;

            if let Err(err) = res {
                tracing::error!("{} while execute state machine command", err,);

                self.resp_tx
                    .send(Notification::StateMachine {
                        command_result: CommandResult { result: Err(err) },
                    })
                    .await
                    .ok();
            }
        };
        C::spawn(fu.instrument(span))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn worker_loop(&mut self) -> Result<(), StorageError<C>> {
        loop {
            let cmd = self.cmd_rx.recv().await;
            let cmd = match cmd {
                None => {
                    tracing::info!("{}: rx closed, state machine worker quit", func_name!());
                    return Ok(());
                }
                Some(x) => x,
            };

            tracing::debug!("{}: received command: {:?}", func_name!(), cmd);

            match cmd {
                Command::BuildSnapshot => {
                    tracing::info!("{}: build snapshot", func_name!());

                    // It is a read operation and is spawned, and it responds in another task
                    self.build_snapshot(self.resp_tx.clone()).await;
                }
                Command::GetSnapshot { tx } => {
                    tracing::info!("{}: get snapshot", func_name!());

                    self.get_snapshot(tx).await?;
                    // GetSnapshot does not respond to RaftCore
                }
                Command::InstallFullSnapshot {
                    log_io_id: io_id,
                    snapshot,
                } => {
                    tracing::info!("{}: install complete snapshot", func_name!());

                    let meta = snapshot.meta.clone();
                    self.state_machine
                        .install_snapshot(&meta, snapshot.snapshot)
                        .await
                        .sto_write_snapshot(Some(meta.signature()))?;

                    tracing::info!("Done install complete snapshot, meta: {}", meta);

                    let res = CommandResult::new(Ok(Response::InstallSnapshot((io_id, Some(meta)))));
                    self.resp_tx.send(Notification::sm(res)).await.ok();
                }
                Command::BeginReceivingSnapshot { tx } => {
                    tracing::info!("{}: BeginReceivingSnapshot", func_name!());

                    let snapshot_data = self.state_machine.begin_receiving_snapshot().await.sto_write_snapshot(None)?;

                    tx.send(snapshot_data).ok();
                    // No response to RaftCore
                }
                Command::Apply {
                    first,
                    last,
                    client_resp_channels,
                } => {
                    let resp = self.apply(first, last, client_resp_channels).await?;
                    let res = CommandResult::new(Ok(Response::Apply(resp)));
                    self.resp_tx.send(Notification::sm(res)).await.ok();
                }
                Command::ExternalFunc { func } => {
                    tracing::debug!("{}: run user defined ExternalFunc", func_name!());
                    func(&mut self.state_machine).await;
                }
            };
        }
    }
    #[tracing::instrument(level = "debug", skip_all)]
    async fn apply(
        &mut self,
        first: LogIdOf<C>,
        last: LogIdOf<C>,
        client_resp_channels: Vec<(u64, CoreResponder<C>)>,
    ) -> Result<ApplyResult<C>, StorageError<C>> {
        let since = first.index();
        let end = last.index() + 1;

        #[cfg(debug_assertions)]
        let (got_last_index, last_apply) = {
            let l = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
            (l.clone(), l)
        };

        let strm = self.log_reader.entries_stream(since..end).await;

        // Convert Vec to an iterator for efficient matching
        let mut responder_iter = client_resp_channels.into_iter().peekable();

        // Prepare entries with responders upfront.
        let strm = strm.map_ok(move |entry| {
            let log_index = entry.index();

            // Check if the next responder matches this log index
            let responder = if responder_iter.peek().map(|(idx, _)| *idx) == Some(log_index) {
                responder_iter.next().map(|(_, r)| r)
            } else {
                None
            };

            let item = EntryResponderBuilder { entry, responder };

            #[cfg(debug_assertions)]
            last_apply.store(log_index, std::sync::atomic::Ordering::Relaxed);

            tracing::debug!("Applying entry to state machine: {}", item);

            let (ent, responder) = item.into_parts();

            (ent, responder)
        });

        self.state_machine.apply(Box::pin(strm)).await.sto_apply(last.clone())?;

        #[cfg(debug_assertions)]
        {
            assert_eq!(end - 1, got_last_index.load(std::sync::atomic::Ordering::Relaxed));
        }

        let resp = ApplyResult {
            since,
            end,
            last_applied: last,
        };

        Ok(resp)
    }

    /// Build a snapshot by requesting a builder from the state machine.
    ///
    /// This method calls
    /// [`try_create_snapshot_builder(false)`](`RaftStateMachine::try_create_snapshot_builder`)
    /// to allow the state machine to defer snapshot creation based on operational conditions.
    /// If deferred (`None` returned), a `BuildSnapshotDone(None)` response is sent to RaftCore.
    ///
    /// Building snapshot is a read-only operation that runs in a spawned task. This parallelization
    /// depends on the [`RaftSnapshotBuilder`] implementation: The builder must:
    /// - hold a consistent view of the state machine that won't be affected by further writes such
    ///   as applying a log entry,
    /// - or it must be able to acquire a lock that prevents any write operations.
    #[tracing::instrument(level = "info", skip_all)]
    async fn build_snapshot(&mut self, resp_tx: MpscSenderOf<C, Notification<C>>) {
        // TODO: need to be abortable?
        // use futures_util::future::abortable;
        // let (fu, abort_handle) = abortable(async move { builder.build_snapshot().await });

        let builder = self.state_machine.try_create_snapshot_builder(false).await;

        let Some(mut builder) = builder else {
            tracing::info!("{}: snapshot building is refused by state machine", func_name!());
            let res = CommandResult::new(Ok(Response::BuildSnapshotDone(None)));
            resp_tx.send(Notification::sm(res)).await.ok();
            return;
        };

        let _handle = C::spawn(async move {
            let res = builder.build_snapshot().await.sto_write_snapshot(None);
            let res = res.map(|snap| Response::BuildSnapshotDone(Some(snap.meta)));
            let cmd_res = CommandResult::new(res);
            resp_tx.send(Notification::sm(cmd_res)).await.ok();
        });
        tracing::info!("{}: returning; spawned building snapshot task", func_name!());
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn get_snapshot(&mut self, tx: OneshotSenderOf<C, Option<SnapshotOf<C>>>) -> Result<(), StorageError<C>> {
        tracing::info!("{}", func_name!());

        let snapshot = self.state_machine.get_current_snapshot().await.sto_read_snapshot(None)?;

        tracing::info!(
            "sending back snapshot: meta: {}",
            snapshot.as_ref().map(|s| &s.meta).display()
        );
        tx.send(snapshot).ok();
        Ok(())
    }
}

#[cfg(test)]
mod apply_drain_assert_tests {
    //! Reproduces the apply-worker drain debug-assert at the top of this file
    //! (`assert_eq!(end - 1, got_last_index)` in [`Worker::apply`]).
    //!
    //! The assert requires that the entry stream handed to
    //! [`RaftStateMachine::apply`](RaftStateMachine::apply) yields an entry whose
    //! index equals `last.index()`. However, the documented contract of
    //! [`RaftLogReader::try_get_log_entries`](RaftLogReader::try_get_log_entries)
    //! (which the default [`RaftLogReader::entries_stream`] is built on) states:
    //!
    //! > If the log doesn't contain all the requested entries, return the existing entries. The
    //! > absence of an entry is tolerated only at the beginning or end of the range.
    //!
    //! So a *contract-compliant* log reader is allowed to return a short read that omits the entry
    //! at the end of the range. When it does, `apply` fully drains the (short) stream and returns
    //! `Ok`, yet `got_last_index < end - 1` and the debug-assert panics — even though neither the
    //! state machine nor the log reader violated its contract.

    use std::io;
    use std::ops::Bound;
    use std::ops::RangeBounds;

    use futures_util::Stream;
    use futures_util::StreamExt;

    use super::Worker;
    use crate::AsyncRuntime;
    use crate::OptionalSend;
    use crate::RaftTypeConfig;
    use crate::engine::testing::UTConfig;
    use crate::engine::testing::log_id;
    use crate::entry::RaftEntry;
    use crate::storage::EntryResponder;
    use crate::storage::RaftLogReader;
    use crate::storage::RaftSnapshotBuilder;
    use crate::storage::RaftStateMachine;
    use crate::type_config::TypeConfigExt;
    use crate::type_config::alias::EntryOf;
    use crate::type_config::alias::LogIdOf;
    use crate::type_config::alias::SnapshotMetaOf;
    use crate::type_config::alias::SnapshotOf;
    use crate::type_config::alias::StoredMembershipOf;

    type C = UTConfig;

    /// Extract a `[start, end)` index range from any `RangeBounds<u64>`.
    fn range_to_start_end<RB>(range: &RB) -> (u64, u64)
    where RB: RangeBounds<u64> {
        let start = match range.start_bound() {
            Bound::Included(&s) => s,
            Bound::Excluded(&s) => s + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&e) => e + 1,
            Bound::Excluded(&e) => e,
            Bound::Unbounded => panic!("unbounded range end is not used by apply()"),
        };
        (start, end)
    }

    /// A log reader that produces blank entries for the requested range.
    ///
    /// When `drop_last_entry` is set it omits the final entry of the range, which is an explicitly
    /// permitted short read per the `try_get_log_entries` contract ("absence ... tolerated ... at
    /// the end of the range").
    struct ShortReadLogReader {
        drop_last_entry: bool,
    }

    impl RaftLogReader<C> for ShortReadLogReader {
        async fn try_get_log_entries<RB>(&mut self, range: RB) -> Result<Vec<EntryOf<C>>, io::Error>
        where RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend {
            let (start, mut end) = range_to_start_end(&range);

            // Legal short read: drop the entry at the end of the range.
            if self.drop_last_entry && end > start {
                end -= 1;
            }

            let entries = (start..end).map(|idx| EntryOf::<C>::new_blank(log_id(1, 1, idx))).collect();
            Ok(entries)
        }

        async fn read_vote(&mut self) -> Result<Option<crate::type_config::alias::VoteOf<C>>, io::Error> {
            // The default `entries_stream` does not consult the vote.
            Ok(None)
        }
    }

    /// A snapshot builder that is never invoked in these tests.
    struct UnusedSnapshotBuilder;

    impl RaftSnapshotBuilder<C> for UnusedSnapshotBuilder {
        async fn build_snapshot(&mut self) -> Result<SnapshotOf<C>, io::Error> {
            unreachable!("snapshot building is not exercised by these tests")
        }
    }

    /// A state machine whose `apply` fully drains the entry stream and returns `Ok`.
    ///
    /// This is the well-behaved contract: it pulls every item from the stream before returning,
    /// so any drain-mismatch is attributable to the stream being short, not to the state machine.
    struct DrainingStateMachine;

    impl RaftStateMachine<C> for DrainingStateMachine {
        type SnapshotBuilder = UnusedSnapshotBuilder;

        async fn applied_state(&mut self) -> Result<(Option<LogIdOf<C>>, StoredMembershipOf<C>), io::Error> {
            Ok((None, StoredMembershipOf::<C>::default()))
        }

        async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
        where Strm: Stream<Item = Result<EntryResponder<C>, io::Error>> + Unpin + OptionalSend {
            // Fully drain the stream; every early exit would be an `Err` via `?`.
            while let Some(item) = entries.next().await {
                let (_entry, _responder) = item?;
            }
            Ok(())
        }

        async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
            UnusedSnapshotBuilder
        }

        async fn begin_receiving_snapshot(&mut self) -> Result<<C as RaftTypeConfig>::SnapshotData, io::Error> {
            unreachable!("snapshot receiving is not exercised by these tests")
        }

        async fn install_snapshot(
            &mut self,
            _meta: &SnapshotMetaOf<C>,
            _snapshot: <C as RaftTypeConfig>::SnapshotData,
        ) -> Result<(), io::Error> {
            unreachable!("snapshot install is not exercised by these tests")
        }

        async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf<C>>, io::Error> {
            Ok(None)
        }
    }

    fn build_worker(drop_last_entry: bool) -> Worker<C, DrainingStateMachine, ShortReadLogReader> {
        let (_cmd_tx, cmd_rx) = C::mpsc(1);
        let (resp_tx, _resp_rx) = C::mpsc(1);
        Worker {
            state_machine: DrainingStateMachine,
            log_reader: ShortReadLogReader { drop_last_entry },
            cmd_rx,
            resp_tx,
        }
    }

    /// Control: a full read of the range applies cleanly and the assert holds.
    #[test]
    fn full_read_applies_cleanly() {
        let first = log_id(1, 1, 1);
        let last = log_id(1, 1, 2);

        let mut worker = build_worker(false);

        let res = <C as RaftTypeConfig>::AsyncRuntime::run(async move { worker.apply(first, last, vec![]).await });

        assert!(
            res.is_ok(),
            "full read should apply the whole [1,2] range without panicking"
        );
    }

    /// Reproduce the bug: a legal short read (omitting the entry at the end of the range) drains
    /// fully yet trips `assert_eq!(end - 1, got_last_index)` at the top of this file.
    ///
    /// The assert only exists under `debug_assertions`, so the panic only occurs there.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "left == right")]
    fn short_read_at_range_end_trips_apply_drain_assert() {
        let first = log_id(1, 1, 1);
        let last = log_id(1, 1, 2);

        let mut worker = build_worker(true);

        // The entry stream yields only index 1 (index 2 legally omitted at the end of the range).
        // `apply` drains it and returns `Ok`, but `got_last_index == 1 != end - 1 == 2`.
        let _ = <C as RaftTypeConfig>::AsyncRuntime::run(async move { worker.apply(first, last, vec![]).await });
    }
}
