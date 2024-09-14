// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::engine::RocksEngine;
use crate::options::RocksWriteOptions;
use crate::util::get_cf_handle;
use engine_traits::{self, Error, Mutable, Result, WriteBatchExt, WriteOptions};
use rocksdb::{Writable, WriteBatch as RawWriteBatch, DB};

const WRITE_BATCH_MAX_BATCH: usize = 16;
const WRITE_BATCH_LIMIT: usize = 16;

impl WriteBatchExt for RocksEngine {
    type WriteBatch = RocksWriteBatch;
    type WriteBatchVec = RocksWriteBatchVec;

    const WRITE_BATCH_MAX_KEYS: usize = 256;

    fn support_write_batch_vec(&self) -> bool {
        let options = self.as_inner().get_db_options();
        options.is_enable_multi_batch_write()
    }

    fn write_batch(&self) -> Self::WriteBatch {
        Self::WriteBatch::new(Arc::clone(self.as_inner()))
    }

    fn write_batch_with_cap(&self, cap: usize) -> Self::WriteBatch {
        Self::WriteBatch::with_capacity(Arc::clone(self.as_inner()), cap)
    }
}

pub struct RocksWriteBatch {
    db: Arc<DB>,
    wb: RawWriteBatch,
}

impl RocksWriteBatch {
    pub fn new(db: Arc<DB>) -> RocksWriteBatch {
        RocksWriteBatch {
            db,
            wb: RawWriteBatch::default(),
        }
    }

    pub fn as_inner(&self) -> &RawWriteBatch {
        &self.wb
    }

    pub fn with_capacity(db: Arc<DB>, cap: usize) -> RocksWriteBatch {
        let wb = if cap == 0 {
            RawWriteBatch::default()
        } else {
            RawWriteBatch::with_capacity(cap)
        };
        RocksWriteBatch { db, wb }
    }

    pub fn from_raw(db: Arc<DB>, wb: RawWriteBatch) -> RocksWriteBatch {
        RocksWriteBatch { db, wb }
    }

    pub fn get_db(&self) -> &DB {
        self.db.as_ref()
    }

    pub fn merge(&mut self, src: &Self) {
        self.wb.append(src.wb.data());
    }
}

impl engine_traits::WriteBatch<RocksEngine> for RocksWriteBatch {
    fn with_capacity(e: &RocksEngine, cap: usize) -> RocksWriteBatch {
        e.write_batch_with_cap(cap)
    }

    fn write_opt(&self, opts: &WriteOptions) -> Result<()> {
//        self.write_valuelog(opts)
         let opt: RocksWriteOptions = opts.into();
         self.get_db()
             .write_opt(self.as_inner(), &opt.into_raw())
             .map_err(Error::Engine)
    }

    fn write_valuelog(&self, opts: &WriteOptions) -> Result<Vec<usize>> {
        let opt: RocksWriteOptions = opts.into();
        self.get_db()
            .write_wotr(self.as_inner(), &opt.into_raw())
            .map_err(Error::Engine)
    }

    fn data_size(&self) -> usize {
        self.wb.data_size()
    }

    fn count(&self) -> usize {
        self.wb.count()
    }

    fn is_empty(&self) -> bool {
        self.wb.is_empty()
    }

    fn should_write_to_engine(&self) -> bool {
        self.wb.count() > RocksEngine::WRITE_BATCH_MAX_KEYS
    }

    fn clear(&mut self) {
        self.wb.clear();
    }

    fn set_save_point(&mut self) {
        self.wb.set_save_point();
    }

    fn pop_save_point(&mut self) -> Result<()> {
        self.wb.pop_save_point().map_err(Error::Engine)
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        self.wb.rollback_to_save_point().map_err(Error::Engine)
    }

    fn merge(&mut self, src: Self) {
        self.wb.append(src.wb.data());
    }
}

impl Mutable for RocksWriteBatch {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.wb.put(key, value).map_err(Error::Engine)
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        self.wb.put_cf(handle, key, value).map_err(Error::Engine)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.wb.delete(key).map_err(Error::Engine)
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        self.wb.delete_cf(handle, key).map_err(Error::Engine)
    }

    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.wb
            .delete_range(begin_key, end_key)
            .map_err(Error::Engine)
    }

    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        self.wb
            .delete_range_cf(handle, begin_key, end_key)
            .map_err(Error::Engine)
    }
}

/// `RocksWriteBatchVec` is for method `multibatch_write` of RocksDB, which splits a large WriteBatch
/// into many smaller ones and then any thread could help to deal with these small WriteBatch when it
/// is calling `AwaitState` and wait to become leader of WriteGroup. `multi_batch_write` will perform
/// much better than traditional `pipelined_write` when TiKV writes very large data into RocksDB. We
/// will remove this feature when `unordered_write` of RocksDB becomes more stable and becomes compatible
/// with Titan.
pub struct RocksWriteBatchVec {
    db: Arc<DB>,
    wbs: Vec<RawWriteBatch>,
    save_points: Vec<usize>,
    index: usize,
    cur_batch_size: usize,
    batch_size_limit: usize,
}

impl RocksWriteBatchVec {
    pub fn new(db: Arc<DB>, batch_size_limit: usize, cap: usize) -> RocksWriteBatchVec {
        let wb = RawWriteBatch::with_capacity(cap);
        RocksWriteBatchVec {
            db,
            wbs: vec![wb],
            save_points: vec![],
            index: 0,
            cur_batch_size: 0,
            batch_size_limit,
        }
    }

    pub fn as_inner(&self) -> &[RawWriteBatch] {
        &self.wbs[0..=self.index]
    }

    pub fn as_raw(&self) -> &RawWriteBatch {
        &self.wbs[0]
    }

    pub fn get_db(&self) -> &DB {
        self.db.as_ref()
    }

    pub fn get_index(&self) -> usize {
        self.index
    }
    
    /// `check_switch_batch` will split a large WriteBatch into many smaller ones. This is to avoid
    /// a large WriteBatch blocking write_thread too long.
    fn check_switch_batch(&mut self) {
        if self.batch_size_limit > 0 && self.cur_batch_size >= self.batch_size_limit {
            self.index += 1;
            self.cur_batch_size = 0;
            if self.index >= self.wbs.len() {
                self.wbs.push(RawWriteBatch::default());
            }
        }
        self.cur_batch_size += 1;
    }
}

impl engine_traits::WriteBatch<RocksEngine> for RocksWriteBatchVec {
    fn with_capacity(e: &RocksEngine, cap: usize) -> RocksWriteBatchVec {
        RocksWriteBatchVec::new(e.as_inner().clone(), WRITE_BATCH_LIMIT, cap)
    }

    fn write_opt(&self, opts: &WriteOptions) -> Result<()> {
//        self.write_valuelog(opts)
         let opt: RocksWriteOptions = opts.into();
         if self.index > 0 {
             self.get_db()
                 .multi_batch_write(self.as_inner(), &opt.into_raw())
                 .map_err(Error::Engine)
         } else {
             self.get_db()
                 .write_opt(&self.wbs[0], &opt.into_raw())
                 .map_err(Error::Engine)
         }
    }

    fn write_valuelog(&self, opts: &WriteOptions) -> Result<Vec<usize>> {
        let opt: RocksWriteOptions = opts.into();
        if self.index > 0 {
            self.get_db()
                .multib_write_wotr(self.as_inner(), &opt.into_raw())
                .map_err(Error::Engine)
        } else {
            self.get_db()
                .write_wotr(&self.wbs[0], &opt.into_raw())
                .map_err(Error::Engine)
        }
    }

    fn data_size(&self) -> usize {
        self.wbs.iter().fold(0, |a, b| a + b.data_size())
    }

    fn count(&self) -> usize {
        self.cur_batch_size + self.index * self.batch_size_limit
    }

    fn is_empty(&self) -> bool {
        self.wbs[0].is_empty()
    }

    fn should_write_to_engine(&self) -> bool {
        self.index >= WRITE_BATCH_MAX_BATCH
    }

    fn clear(&mut self) {
        for i in 0..=self.index {
            self.wbs[i].clear();
        }
        self.save_points.clear();
        self.index = 0;
        self.cur_batch_size = 0;
    }

    fn set_save_point(&mut self) {
        self.wbs[self.index].set_save_point();
        self.save_points.push(self.index);
    }

    fn pop_save_point(&mut self) -> Result<()> {
        if let Some(x) = self.save_points.pop() {
            return self.wbs[x].pop_save_point().map_err(Error::Engine);
        }
        Err(Error::Engine("no save point".into()))
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        if let Some(x) = self.save_points.pop() {
            for i in x + 1..=self.index {
                self.wbs[i].clear();
            }
            self.index = x;
            return self.wbs[x].rollback_to_save_point().map_err(Error::Engine);
        }
        Err(Error::Engine("no save point".into()))
    }

    fn merge(&mut self, _: Self) {
        panic!("merge is not implemented for RocksWriteBatchVec");
    }
}

impl Mutable for RocksWriteBatchVec {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_switch_batch();
        self.wbs[self.index].put(key, value).map_err(Error::Engine)
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        self.wbs[self.index]
            .put_cf(handle, key, value)
            .map_err(Error::Engine)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        self.wbs[self.index].delete(key).map_err(Error::Engine)
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        self.wbs[self.index]
            .delete_cf(handle, key)
            .map_err(Error::Engine)
    }

    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        self.wbs[self.index]
            .delete_range(begin_key, end_key)
            .map_err(Error::Engine)
    }

    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        self.check_switch_batch();
        let handle = get_cf_handle(self.db.as_ref(), cf)?;
        self.wbs[self.index]
            .delete_range_cf(handle, begin_key, end_key)
            .map_err(Error::Engine)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use super::super::util::new_engine_opt;
    use super::super::RocksDBOptions;
    use super::*;
    use crate::{RocksWOTR};
    use engine_traits::{WOTR, WOTRExt, WriteBatch, Peekable, KvEngine};
    use rocksdb::DBOptions as RawDBOptions;
    use tempfile::Builder;

    #[test]
    fn test_should_write_to_engine() {
        let path = Builder::new()
            .prefix("test-should-write-to-engine")
            .tempdir()
            .unwrap();
        let opt = RawDBOptions::default();
        opt.enable_multi_batch_write(true);
        opt.enable_unordered_write(false);
        opt.enable_pipelined_write(true);
        let engine = new_engine_opt(
            path.path().join("db").to_str().unwrap(),
            RocksDBOptions::from_raw(opt),
            vec![],
        )
        .unwrap();
        assert!(engine.support_write_batch_vec());
        let mut wb = engine.write_batch();
        for _i in 0..RocksEngine::WRITE_BATCH_MAX_KEYS {
            wb.put(b"aaa", b"bbb").unwrap();
        }
        assert!(!wb.should_write_to_engine());
        wb.put(b"aaa", b"bbb").unwrap();
        assert!(wb.should_write_to_engine());
        let mut wb = RocksWriteBatchVec::with_capacity(&engine, 1024);
        for _i in 0..WRITE_BATCH_MAX_BATCH * WRITE_BATCH_LIMIT {
            wb.put(b"aaa", b"bbb").unwrap();
        }
        assert!(!wb.should_write_to_engine());
        wb.put(b"aaa", b"bbb").unwrap();
        assert!(wb.should_write_to_engine());
        wb.clear();
        assert!(!wb.should_write_to_engine());
    }

    #[test]
    fn test_wotr_write() {
        let path = Builder::new()
            .prefix("test-wotr-write")
            .tempdir().
            unwrap();
                    
        let w = Arc::new(RocksWOTR::new(path.path().join("wotrlog.txt").to_str().unwrap()));
        let opt = RawDBOptions::default();
        let mut engine = new_engine_opt(
            path.path().join("db").to_str().unwrap(),
            RocksDBOptions::from_raw(opt),
            vec![],
        ).unwrap();

        assert!(engine.register_valuelog(w.clone(), false).is_ok());

        let mut wb = engine.write_batch();
        wb.put(b"k1", b"v1111");
        wb.put(b"k2", b"v2222");
        // assert wb length is 2?
        let offsets = wb.write_valuelog(&WriteOptions::new()).unwrap();
        assert!(offsets.len() == 2);
        assert!(offsets[1] != 0);

        let r = engine.get_valuelog(b"k1");
        assert!(r.unwrap().unwrap() == b"v1111");
        let r2 = engine.get_valuelog(b"k2");
        assert!(r2.unwrap().unwrap() == b"v2222");
    }
    
    #[test]
    fn test_wotr_read_snapshot() {
        let path = Builder::new()
            .prefix("test-wotr-read-snapshot")
            .tempdir().
            unwrap();
                    
        let w = Arc::new(RocksWOTR::new(path.path().join("wotrlog2.txt").to_str().unwrap()));
        let opt = RawDBOptions::default();
        let mut engine = new_engine_opt(
            path.path().join("db").to_str().unwrap(),
            RocksDBOptions::from_raw(opt),
            vec![],
        ).unwrap();

        assert!(engine.register_valuelog(w.clone(), false).is_ok());

        let mut wb = engine.write_batch();
        wb.put(b"k1", b"v1111");
        wb.put(b"k2", b"v2222");
        // assert wb length is 2?
        let offsets = wb.write_valuelog(&WriteOptions::new()).unwrap();
        assert!(offsets.len() == 2);
        assert!(offsets[1] != 0);

        let snapshot = engine.snapshot();

        let r = snapshot.get_valuelog(b"k1");
        assert!(r.unwrap().unwrap() == b"v1111");
        let r2 = snapshot.get_valuelog(b"k2");
        assert!(r2.unwrap().unwrap() == b"v2222");
    }

    #[test]
    fn test_wotr_write_delete() {
        let path = Builder::new()
            .prefix("test-wotr-write")
            .tempdir().
            unwrap();
                    
        let w = Arc::new(RocksWOTR::new(path.path().join("wotrlog.txt").to_str().unwrap()));
        let opt = RawDBOptions::default();
        let mut engine = new_engine_opt(
            path.path().join("db").to_str().unwrap(),
            RocksDBOptions::from_raw(opt),
            vec![],
        ).unwrap();

        assert!(engine.register_valuelog(w.clone(), false).is_ok());

        let mut wb = engine.write_batch();
        wb.put(b"k1", b"v1111");
        wb.put(b"k2", b"v2222");
        // assert wb length is 2?
        let offsets = wb.write_valuelog(&WriteOptions::new()).unwrap();
        assert!(offsets.len() == 2);
        assert!(offsets[1] != 0);

        let r = engine.get_valuelog(b"k1");
        assert!(r.unwrap().unwrap() == b"v1111");
        let r2 = engine.get_valuelog(b"k2");
        assert!(r2.unwrap().unwrap() == b"v2222");

        let mut wb2 = engine.write_batch();
        wb2.delete(b"k1");
        let offsets = wb2.write_valuelog(&WriteOptions::new()).unwrap();
        assert_eq!(offsets.len(), 0);

        assert!(engine.get_valuelog(b"k1").unwrap().is_none());
    }

    #[test]
    fn test_wotr_multi_batch() {
        let path = Builder::new()
            .prefix("test-wotr-multi-batch")
            .tempdir()
            .unwrap();
    
        let w = Arc::new(RocksWOTR::new(path.path().join("wotrlog.txt").to_str().unwrap()));
        let opt = RawDBOptions::default();
        opt.enable_multi_batch_write(true);
        opt.enable_unordered_write(false);
        opt.enable_pipelined_write(true);
    
        let mut engine = new_engine_opt(
            path.path().join("db").to_str().unwrap(),
            RocksDBOptions::from_raw(opt),
            vec![],
        ).unwrap();

        assert!(engine.register_valuelog(w.clone(), false).is_ok());
        assert!(engine.support_write_batch_vec());
        
        let mut wb = RocksWriteBatchVec::with_capacity(&engine, 1024);
        let numrecords = WRITE_BATCH_MAX_BATCH * WRITE_BATCH_LIMIT;
        for _i in 0..numrecords {
            wb.put(b"aaa", b"bbb").unwrap();
        }
        assert!(wb.get_index() > 1);
        let offsets = wb.write_valuelog(&WriteOptions::new()).unwrap();
        assert!(offsets.len() == numrecords);

        let r = engine.get_valuelog(b"aaa");
        assert!(r.unwrap().unwrap() == b"bbb");
    }

    fn engine_maker(path: &tempfile::TempDir, w: Arc<RocksWOTR>) -> (RocksEngine, RocksEngine) {
        let path = Builder::new()
            .prefix("test-wotr-write")
            .tempdir().
            unwrap();

        let opt1 = RawDBOptions::default();
        let opt2 = RawDBOptions::default();
        let mut engine1 = new_engine_opt(
            path.path().join("db").to_str().unwrap(),
            RocksDBOptions::from_raw(opt1),
            vec![],
        ).unwrap();
        let mut engine2 = new_engine_opt(
            path.path().join("db2").to_str().unwrap(),
            RocksDBOptions::from_raw(opt2),
            vec![],
        ).unwrap();

        assert!(engine1.register_valuelog(w.clone(), false).is_ok());
        assert!(engine2.register_valuelog(w.clone(), false).is_ok());

        (engine1, engine2)
    }

    #[test]
    fn test_wotr_two_dbs() {
        let path = Builder::new()
            .prefix("test-wotr-write")
            .tempdir().
            unwrap();
        
        let w = Arc::new(RocksWOTR::new(path.path().join("wotrlog.txt").to_str().unwrap()));

        let (engine1, engine2) = engine_maker(&path, w.clone());

        let mut wb1 = engine1.write_batch();
        wb1.put(b"wb1k1", b"wb1v1111").unwrap();
        wb1.put(b"wb1k2", b"wb1v2222").unwrap();
        let mut wb2 = engine2.write_batch();
        wb2.put(b"wb2k1", b"wb2v1111").unwrap();
        wb2.put(b"wb2k2", b"wb2v2222").unwrap();

        let offsets = wb1.write_valuelog(&WriteOptions::new()).unwrap();
        let offsets = wb2.write_valuelog(&WriteOptions::new()).unwrap();
        assert!(offsets.len() == 2);
        assert!(offsets[1] != 0);

        let r = engine1.get_valuelog(b"wb1k1");
        assert!(r.unwrap().unwrap() == b"wb1v1111");
        let r2 = engine2.get_valuelog(b"wb2k1");
        assert!(r2.unwrap().unwrap() == b"wb2v1111");
    }
    
    #[test]
    fn test_wotr_two_dbs_apply() {
        let path = Builder::new()
            .prefix("test-wotr-write")
            .tempdir().
            unwrap();
        
        let w = Arc::new(RocksWOTR::new(path.path().join("wotrlog.txt").to_str().unwrap()));

        let (engine1, engine2) = engine_maker(&path, w.clone());

        let mut wb1 = engine1.write_batch();
        wb1.put(b"wb1k1", b"wb1v1111").unwrap();
        wb1.put(b"wb1k2", b"wb1v2222").unwrap();

        let offsets = wb1.write_valuelog(&WriteOptions::new()).unwrap();
        assert!(offsets.len() == 2);
        let mut wb2 = engine2.write_batch();
        wb2.put(b"wb1k1", &format!("{}", offsets[0]).into_bytes()).unwrap();
        wb2.put(b"wb1k2", &format!("{}", offsets[1]).into_bytes()).unwrap();

        wb2.write_opt(&WriteOptions::new()).unwrap();

        let r = engine1.get_valuelog(b"wb1k1");
        assert!(r.unwrap().unwrap() == b"wb1v1111");
        let r2 = engine2.get_valuelog(b"wb1k2");
        assert!(r2.unwrap().unwrap() == b"wb1v2222");
    }
    

}
