// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use crate::{util, RocksEngine, RocksWriteBatch};

use engine_traits::{
    Error, Iterable, KvEngine, MiscExt, Mutable, Peekable, RaftEngine, RaftEngineReadOnly,
    RaftLogBatch, RaftLogGCTask, Result, SyncMutable, WriteBatch, WriteBatchExt, WriteOptions,
    CF_DEFAULT,
};
use crate::db_vector::RocksDBVector;
use kvproto::raft_serverpb::RaftLocalState;
use protobuf::Message;
use raft::eraftpb::Entry;
use tikv_util::{box_err, box_try};
use std::ops::Deref;

const RAFT_LOG_MULTI_GET_CNT: u64 = 8;

impl RaftEngineReadOnly for RocksEngine {
//    type DBVector =  RocksDBVector;
    fn get_raft_state(&self, raft_group_id: u64) -> Result<Option<RaftLocalState>> {
        let key = keys::raft_state_key(raft_group_id);
        self.get_msg_cf_valuelog(CF_DEFAULT, &key)
    }

    fn get_entry(&self, raft_group_id: u64, index: u64) -> Result<Option<Entry>> {
        let key = keys::raft_log_key(raft_group_id, index);
        self.get_msg_cf_valuelog(CF_DEFAULT, &key)
    }

    // presume we already know the key
    fn get_entry_location(&self, key: &[u8]) -> Option<u64> {
        match self.get_value(&key) {
            Ok(result) => {
                if let Some(logoffset) = result {
		    let value = unsafe {
		        let vptr = logoffset.as_ptr() as *const u64;
		        *vptr
		    };
//		    let value = u64::from_le_bytes(varr);
//                    let data = std::str::from_utf8(&logoffset).ok()?;
//                    let value: u64 = data.parse().ok()?;
                    println!("got value {}", value);
                    Some(value)
                } else {
                    return None;
                }
            },
            Err(_) => { return None; }
        }
    }

    fn fetch_entries_to(
        &self,
        region_id: u64,
        low: u64,
        high: u64,
        max_size: Option<usize>,
        buf: &mut Vec<Entry>,
    ) -> Result<usize> {
        let (max_size, mut total_size, mut count) = (max_size.unwrap_or(usize::MAX), 0, 0);

        if high - low <= RAFT_LOG_MULTI_GET_CNT {
            // If election happens in inactive regions, they will just try to fetch one empty log.
            for i in low..high {
                if total_size > 0 && total_size >= max_size {
                    println!("case0");
                    break;
                }
                let key = keys::raft_log_key(region_id, i);
                match self.get_valuelog(&key) {
                    Ok(None) => {
                        println!("err1");
                        return Err(Error::EntriesCompacted)
                    }
                    Ok(Some(v)) => {
                        println!("case1");
                        let mut entry = Entry::default();
                        entry.merge_from_bytes(&v)?;
                        assert_eq!(entry.get_index(), i);
                        buf.push(entry);
                        total_size += v.len();
                        count += 1;
                    }
                    Err(e) => {
                        println!("err2");
                        return Err(box_err!(e))
                    }
                }
            }
            return Ok(count);
        }
        println!("out of the loop");

        let (mut check_compacted, mut next_index) = (true, low);
        let start_key = keys::raft_log_key(region_id, low);
        let end_key = keys::raft_log_key(region_id, high);
        self.scan(
            &start_key,
            &end_key,
            true, // fill_cache
	    false, // use_wotr disabled for raft keys
            |key, _value| {
                let realvalue = self.get_valuelog(key).unwrap().unwrap();
                let mut entry = Entry::default();
                entry.merge_from_bytes(&realvalue)?;

                if check_compacted {
                    if entry.get_index() != low {
                        // May meet gap or has been compacted.
                        return Ok(false);
                    }
                    check_compacted = false;
                } else {
                    assert_eq!(entry.get_index(), next_index);
                }
                next_index += 1;

                buf.push(entry);
                total_size += realvalue.len();
                count += 1;
                Ok(total_size < max_size)
            },
        )?;

        // If we get the correct number of entries, returns.
        // Or the total size almost exceeds max_size, returns.
        if count == (high - low) as usize || total_size >= max_size {
            return Ok(count);
        }

        // Here means we don't fetch enough entries.
        Err(Error::EntriesUnavailable)
    }

    fn get_all_entries_to(&self, region_id: u64, buf: &mut Vec<Entry>) -> Result<()> {
        let start_key = keys::raft_log_key(region_id, 0);
        let end_key = keys::raft_log_key(region_id, u64::MAX);
        self.scan(
            &start_key,
            &end_key,
            false, // fill_cache
	    false, // use_wotr (shawgerj: we could do a better job here if the wotr iterator could either use GetExternal or GetP. But right now we do the extra reads with GetExternal in tikv 
            |key, _value| {
                let realvalue = self.get_valuelog(key).unwrap().unwrap();
                let mut entry = Entry::default();
                entry.merge_from_bytes(&realvalue)?;
                buf.push(entry);
                Ok(true)
            },
        )?;
        Ok(())
    }
}
impl RocksEngine {
    fn gc_eligible(key: &[u8], end: u64) -> bool {
	let (region_id, index) = keys::decode_raft_log_key(key);
	index < end
    }
    
    // shawgerj: rather than seek and scan in Raft-LSM, we need to seek and scan the log. 
    fn gc_impl(
        &self,
        raft_group_id: u64,
        mut from: u64,
        to: u64, // equals persisted I_app due to prior sync
        raft_wb: &mut RocksWriteBatch,
    ) -> Result<usize> {
	let logtail = self.get_cf(CF_DEFAULT, "wotr_tail").unwrap();
	let vtail = kv.get_cf(CF_DEFAULT, "vtail").unwrap();
	let new_vtail = vtail;
	
	let iter = self.get_wotr().wotr_iter_init().unwrap();
	let _ = iter.seek(logtail);

	let prefix = keys::raft_log_prefix(raft_group_id);

	while (iter.valid().unwrap() > 0 && iter.position() < vtail {
	    let key = unsafe {
		slice::from_raw_parts(iter.key().unwrap(),
				      iter.key_size().unwrap() as usize)
	    };

	    let value = unsafe {
		slice::from_raw_parts(iter.value().unwrap(),
				      iter.value_size().unwrap() as usize)
	    };

	    // not a raft entry: write the k-v again at log head
	    if !keys::is_raft_key(&key) {
		raft_wb.put(key, value);
	    }
	    
	    // if raft key can't be gc-ed we break out of here
	    if (!gc_eligible(&key, &to)) {
		break;
	    }
	    
	    // any valid data in the entry?
	    let mut entry = Entry::default();
	    entry.merge_from_bytes(&value).unwrap();

	    let index = entry.get_index();
	    let term = entry.get_term();
	    let data = entry.get_data();
	    // do I need datasize? see apply.rs:1104

	    if !data.is_empty() {
		let mut cmd = RaftCmdRequest::default();
		cmd.merge_from_bytes(entry.data()).unwrap_or_else(|e| {
		    panic!("log data unexpected at pos {}: {:?}",
			   iter.position(), e);
		});
		if cmd.has_admin_request() {
		    raft_wb.delete(&key)?;
		    // we don't need to re-write this to the log
		} else {
		    let requests = req.get_requests();
		    // we only need to do something if there is a Put
		    // if there is at least one Put, we need to re-write
		    // this entry and update key(s) in kv-lsm
		    for req in requests {
			let cmd_type = req.get_cmd_type();
			match cmd_type {
			    CmdType::Put => {
				validated = 1;
				raft_wb.delete(&key)?;
				// we are going to have to do a manual write to wotr rather than re-insert the key through raft-lsm because of concurrency issues. Basically, it is very hard to know we get the right offset back from rocksdb. Wotr::WotrWrite() does have a lock.
				kv_wb.put(...)?;
			    },
			    _ => continue,
			}?;
		    }
		}
	    }

	    let _ = iter.next();
	    new_vtail = iter.position();
	}

	self.consume(...); // write the new set of entries to loghead
	self.put_cf(CF_DEFAULT, new_vtail);

	Ok(total as usize)
	
//         if from == 0 {
//             let start_key = keys::raft_log_key(raft_group_id, 0);
//             let prefix = keys::raft_log_prefix(raft_group_id);
//             match self.seek(&start_key)? {
//                 Some((k, _)) if k.starts_with(&prefix) => from = box_try!(keys::raft_log_index(&k)),
//                 // No need to gc.
//                 _ => return Ok(0),
//             }
//         }
//         if from >= to {
//             return Ok(0);
//         }

//         for idx in from..to {
//             let key = keys::raft_log_key(raft_group_id, idx);
//             raft_wb.delete(&key)?;

// 	    // avoid this!
// //	    let entry: Option<Entry> = self.get_msg_cf_valuelog(CF_DEFAULT, &key);
// //	    if Some(entry) {
// 		// iterate through the entry, finding all Put keys
// 		// query kv-lsm with each Put key
// 		// - if key exists with same offset in kv-lsm:
// 		//   - raft_wb.put_external(&key)
// 		//   - kv_wb.put(new offset of updated key)
// 		//   - fallocate depends on value of persisted v_tail (a new value which must be stored in kv-lsm)
// 		// - if not:
// 		//   - raft_wb.delete(&key)
// 	    }
	    
//             if raft_wb.count() >= Self::WRITE_BATCH_MAX_KEYS * 2 {
//                 raft_wb.write()?;
//                 raft_wb.clear();
//             }
//         }
//         Ok((to - from) as usize)
    }
}

// FIXME: RaftEngine should probably be implemented generically
// for all KvEngines, but is currently implemented separately for
// every engine.
impl RaftEngine for RocksEngine {
    type LogBatch = RocksWriteBatch;

    fn log_batch(&self, capacity: usize) -> Self::LogBatch {
        RocksWriteBatch::with_capacity(self.as_inner().clone(), capacity)
    }

    fn sync(&self) -> Result<()> {
        self.sync_wal()
    }

    fn get_keys<'a>(&self, batch: &'a Self::LogBatch) -> Option<Vec<&'a [u8]>> {
        // // iterate through the batch, and return a vector of all the keys
        // let batch_iter = batch.as_inner().iter();
        // let mut keys = Vec::new();

        // for i in batch_iter {
        //     let (value_type, _column_family, key, _val) = i;
           
        //     if value_type == DBValueType::TypeValue {
        //         keys.push(key);
        //     }
        // }
        // Some(keys)
        batch.as_inner().keys_to_write()
    }

    fn consume(&self,
               batch: &mut Self::LogBatch,
               sync_log: bool
    ) -> Result<(usize, Vec<usize>)> {
        let bytes = batch.data_size();
        let mut opts = WriteOptions::default();
        opts.set_sync(sync_log);
        opts.set_disable_wal(true);
        let offsets = batch.write_valuelog(&opts)?;
        batch.clear();
        Ok((bytes, offsets))
    }

    fn consume_to_lsm(&self,
               batch: &Self::LogBatch,
               sync_log: bool
    ) -> Result<usize> {
        let bytes = batch.data_size();
        let mut opts = WriteOptions::default();
        opts.set_sync(sync_log);
        batch.write_opt(&opts)?;
        Ok(bytes)
    }


    fn consume_and_shrink(
        &self,
        batch: &mut Self::LogBatch,
        sync_log: bool,
        max_capacity: usize,
        shrink_to: usize,
    ) -> Result<(usize, Vec<usize>)> {
        let (data_size, offsets) = self.consume(batch, sync_log)?;
        if data_size > max_capacity {
            *batch = self.write_batch_with_cap(shrink_to);
        }
        Ok((data_size, offsets))
    }

    fn shrink(
        &self,
        batch: &mut Self::LogBatch,
        data_size: usize,
        max_capacity: usize,
        shrink_to: usize,
    ) -> Result<()> {
        batch.clear();
        if data_size > max_capacity {
            *batch = self.write_batch_with_cap(shrink_to);
        }
        Ok(())
    }

    fn clean(
        &self,
        raft_group_id: u64,
        mut first_index: u64,
        state: &RaftLocalState,
        batch: &mut Self::LogBatch,
    ) -> Result<()> {
        batch.delete(&keys::raft_state_key(raft_group_id))?;
        if first_index == 0 {
            let seek_key = keys::raft_log_key(raft_group_id, 0);
            let prefix = keys::raft_log_prefix(raft_group_id);
            fail::fail_point!("engine_rocks_raft_engine_clean_seek", |_| Ok(()));
            if let Some((key, _)) = self.seek(&seek_key)? {
                if !key.starts_with(&prefix) {
                    // No raft logs for the raft group.
                    return Ok(());
                }
                first_index = match keys::raft_log_index(&key) {
                    Ok(index) => index,
                    Err(_) => return Ok(()),
                };
            } else {
                return Ok(());
            }
        }
        if first_index <= state.last_index {
            for index in first_index..=state.last_index {
                let key = keys::raft_log_key(raft_group_id, index);
                batch.delete(&key)?;
            }
        }
        Ok(())
    }

    // this could be really problematic with WOTR but I haven't actually
    // found an instance of it being called. Ignore for now
    fn append(&self, raft_group_id: u64, entries: Vec<Entry>
    ) -> Result<(usize, Vec<usize>)> {
        let mut wb = RocksWriteBatch::new(self.as_inner().clone());
        let buf = Vec::with_capacity(1024);
        wb.append_impl(raft_group_id, &entries, buf)?;
        self.consume(&mut wb, false)
    }

    fn put_raft_state(&self, raft_group_id: u64, state: &RaftLocalState) -> Result<()> {
        self.put_msg_valuelog(&keys::raft_state_key(raft_group_id), state);
        Ok(())
    }

    fn batch_gc(&self, groups: Vec<RaftLogGCTask>) -> Result<usize> {
        let mut total = 0;
        let mut raft_wb = self.write_batch_with_cap(4 * 1024);
        for task in groups {
            total += self.gc_impl(task.raft_group_id, task.from, task.to, &mut raft_wb)?;
        }
        // TODO: disable WAL here.
        if !WriteBatch::is_empty(&raft_wb) {
            let mut opts = WriteOptions::default();
            opts.set_disable_wal(true);
            opts.set_sync(false);
            raft_wb.write_opt(&opts)?;
        }
        Ok(total)
    }

    fn gc(&self, raft_group_id: u64, from: u64, to: u64) -> Result<usize> {
        let mut raft_wb = self.write_batch_with_cap(1024);
        let total = self.gc_impl(raft_group_id, from, to, &mut raft_wb)?;
        // TODO: disable WAL here.
        if !WriteBatch::is_empty(&raft_wb) {
            let mut opts = WriteOptions::default();
            opts.set_disable_wal(true);
            opts.set_sync(false);
            raft_wb.write_opt(&opts)?;
        }
        Ok(total)
    }

    fn purge_expired_files(&self) -> Result<Vec<u64>> {
        Ok(vec![])
    }

    fn has_builtin_entry_cache(&self) -> bool {
        false
    }

    fn flush_metrics(&self, instance: &str) {
        KvEngine::flush_metrics(self, instance)
    }

    fn reset_statistics(&self) {
        KvEngine::reset_statistics(self)
    }

    fn dump_stats(&self) -> Result<String> {
        MiscExt::dump_stats(self)
    }

    fn get_engine_size(&self) -> Result<u64> {
        let handle = util::get_cf_handle(self.as_inner(), CF_DEFAULT)?;
        let used_size = util::get_engine_cf_used_size(self.as_inner(), handle);

        Ok(used_size)
    }
}

impl RaftLogBatch for RocksWriteBatch {
    fn append(&mut self, raft_group_id: u64, entries: Vec<Entry>) -> Result<()> {
        if let Some(max_size) = entries.iter().map(|e| e.compute_size()).max() {
            let ser_buf = Vec::with_capacity(max_size as usize);
            return self.append_impl(raft_group_id, &entries, ser_buf);
        }
        Ok(())
    }

    fn cut_logs(&mut self, raft_group_id: u64, from: u64, to: u64) {
        for index in from..to {
            let key = keys::raft_log_key(raft_group_id, index);
            self.delete(&key).unwrap();
        }
    }

    fn put_raft_state(&mut self, raft_group_id: u64, state: &RaftLocalState) -> Result<()> {
        self.put_msg(&keys::raft_state_key(raft_group_id), state);
        Ok(())
    }

    fn persist_size(&self) -> usize {
        self.data_size()
    }

    fn is_empty(&self) -> bool {
        WriteBatch::is_empty(self)
    }

    fn merge(&mut self, src: Self) {
        WriteBatch::<RocksEngine>::merge(self, src);
    }
}

impl RocksWriteBatch {
    fn append_impl(
        &mut self,
        raft_group_id: u64,
        entries: &[Entry],
        mut ser_buf: Vec<u8>,
    ) -> Result<()> {
        for entry in entries {
            let key = keys::raft_log_key(raft_group_id, entry.get_index());
            ser_buf.clear();
            entry.write_to_vec(&mut ser_buf).unwrap();
            self.put(&key, &ser_buf)?;
        }
        Ok(())
    }
}
