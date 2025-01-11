// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::*;
use kvproto::raft_serverpb::RaftLocalState;
use raft::eraftpb::Entry;

pub trait RaftEngineReadOnly: Sync + Send + 'static {
    fn get_raft_state(&self, raft_group_id: u64) -> Result<Option<RaftLocalState>>;

    fn get_entry(&self, raft_group_id: u64, index: u64) -> Result<Option<Entry>>;

    fn get_entry_location(&self, key: &[u8]) -> Option<u64>;

    /// Return count of fetched entries.
    fn fetch_entries_to(
        &self,
        raft_group_id: u64,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        to: &mut Vec<Entry>,
    ) -> Result<usize>;

    /// Get all available entries in the region.
    fn get_all_entries_to(&self, region_id: u64, buf: &mut Vec<Entry>) -> Result<()>;
}

pub struct RaftLogGCTask {
    pub raft_group_id: u64,
    pub from: u64,
    pub to: u64,
}

pub trait RaftEngine: RaftEngineReadOnly + Clone + Sync + Send + 'static {
    type LogBatch: RaftLogBatch;

    fn log_batch(&self, capacity: usize) -> Self::LogBatch;

    /// Synchronize the Raft engine.
    fn sync(&self) -> Result<()>;

    /// Get all the keys from a LogBatch (used by WOTR)
    fn get_keys<'a>(&self, batch: &'a Self::LogBatch) -> Option<Vec<&'a [u8]>>;

    /// Consume the write batch by moving the content into the engine itself
    /// and return written bytes.
    fn consume(&self,
               batch: &mut Self::LogBatch,
               sync: bool
    ) -> Result<(usize, Vec<usize>)>;

    fn consume_to_lsm(&self,
                      batch: &Self::LogBatch,
                      sync: bool
    ) -> Result<usize>;

    /// Like `consume` but shrink `batch` if need.
    fn consume_and_shrink(
        &self,
        batch: &mut Self::LogBatch,
        sync: bool,
        max_capacity: usize,
        shrink_to: usize,
    ) -> Result<(usize, Vec<usize>)>;

    /// Shrink an empty batch if it is bigger than max_capacity. Use after
    /// "consume"
    fn shrink(
        &self,
        batch: &mut Self::LogBatch,
        data_size: usize,
        max_capacity: usize,
        shrink_to: usize,
    ) -> Result<()>;

    fn clean(
        &self,
        raft_group_id: u64,
        first_index: u64,
        state: &RaftLocalState,
        batch: &mut Self::LogBatch,
    ) -> Result<()>;

    /// Append some log entries and return written bytes.
    ///
    /// Note: `RaftLocalState` won't be updated in this call.
    fn append(&self, raft_group_id: u64, entries: Vec<Entry>)
              -> Result<(usize, Vec<usize>)>;

    fn put_raft_state(&self, raft_group_id: u64, state: &RaftLocalState) -> Result<()>;

    /// Like `cut_logs` but the range could be very large. Return the deleted count.
    /// Generally, `from` can be passed in `0`.
    fn gc<E: KvEngine>(&self, raft_group_id: u64, from: u64, to: u64, kv_engine: &E) -> Result<usize>;

    fn batch_gc<E: KvEngine>(&self, tasks: Vec<RaftLogGCTask>, kv_engine: &E) -> Result<usize> {
        let mut total = 0;
        for task in tasks {
            total += self.gc(task.raft_group_id, task.from, task.to, kv_engine)?;
        }
        Ok(total)
    }

    /// Purge expired logs files and return a set of Raft group ids
    /// which needs to be compacted ASAP.
    fn purge_expired_files(&self) -> Result<Vec<u64>>;

    /// The `RaftEngine` has a builtin entry cache or not.
    fn has_builtin_entry_cache(&self) -> bool {
        false
    }

    /// GC the builtin entry cache.
    fn gc_entry_cache(&self, _raft_group_id: u64, _to: u64) {}

    fn flush_metrics(&self, _instance: &str) {}
    fn flush_stats(&self) -> Option<CacheStats> {
        None
    }
    fn reset_statistics(&self) {}

    fn stop(&self) {}

    fn dump_stats(&self) -> Result<String>;

    fn get_engine_size(&self) -> Result<u64>;
}

pub trait RaftLogBatch: Send {
    /// Note: `RaftLocalState` won't be updated in this call.
    fn append(&mut self, raft_group_id: u64, entries: Vec<Entry>) -> Result<()>;

    /// Remove Raft logs in [`from`, `to`) which will be overwritten later.
    fn cut_logs(&mut self, raft_group_id: u64, from: u64, to: u64);

    fn put_raft_state(&mut self, raft_group_id: u64, state: &RaftLocalState) -> Result<()>;

    /// The data size of this RaftLogBatch.
    fn persist_size(&self) -> usize;

    /// Whether it is empty or not.
    fn is_empty(&self) -> bool;

    /// Merge another RaftLogBatch to itself.
    fn merge(&mut self, _: Self);
}

#[derive(Clone, Copy, Default)]
pub struct CacheStats {
    pub hit: usize,
    pub miss: usize,
    pub cache_size: usize,
}
