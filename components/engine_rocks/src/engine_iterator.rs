// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use engine_traits::{self, Error, Result, Iterator};
use rocksdb::{DBIterator, SeekKey as RawSeekKey, DB};
use rocksdb::rocksdb_options::ReadOptions;
use crate::util::get_cf_handle;

// FIXME: Would prefer using &DB instead of Arc<DB>.  As elsewhere in
// this crate, it would require generic associated types.
pub struct RocksEngineIterator {
    iter: DBIterator<Arc<DB>>,
    db: Arc<DB>,
    cf: Option<String>,
    value: Vec<u8>,
}

impl RocksEngineIterator {
    pub fn from_raw(iter: DBIterator<Arc<DB>>, db: Arc<DB>, cf: Option<String>) -> RocksEngineIterator {
       RocksEngineIterator {
           iter,
           db,
           cf,
           value: vec![],
       }
    }

    pub fn sequence(&self) -> Option<u64> {
        self.iter.sequence()
    }
}

impl engine_traits::Iterator for RocksEngineIterator {
    fn seek(&mut self, key: engine_traits::SeekKey<'_>) -> Result<bool> {
        let k: RocksSeekKey<'_> = key.into();
        match self.iter.seek(k.into_raw()) {
            Err(e) => return Err(Error::Engine(e)),
            Ok(true) => {
                if let Some(h) = &self.cf {
                    let handle = get_cf_handle(&self.db, h.as_str()).unwrap();
                    match self.db.get_external_cf(handle, self.iter.key(), &ReadOptions::default()) {
                        Ok(Some(v)) => {
                            self.value = v.to_vec();
                            Ok(true)
                        },
                        _ => Err(Error::Engine("get external iterator error".to_string()))
                    }
                }
                else {
                    match self.db.get_external(self.iter.key(), &ReadOptions::default()) {
                        Ok(Some(v)) => {
                            self.value = v.to_vec();
                            dbg!(&self.value);
                            Ok(true)
                        },
                        _ => Err(Error::Engine("get external iterator error".to_string()))
                    }
                }
            }
            Ok(false) => Ok(false)
        }
    }

    fn seek_for_prev(&mut self, key: engine_traits::SeekKey<'_>) -> Result<bool> {
        let k: RocksSeekKey<'_> = key.into();
        self.iter.seek_for_prev(k.into_raw()).map_err(Error::Engine)
    }

    fn prev(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(Error::Engine("Iterator invalid".to_string()));
        }
        self.iter.prev().map_err(Error::Engine)
    }

    fn next(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(Error::Engine("Iterator invalid".to_string()));
        }
        //let res = self.iter.next().map_err(Error::Engine);

        match self.iter.next() {
            Err(e) => return Err(Error::Engine(e)),
            Ok(true) => {
                if let Some(h) = &self.cf {
                    let handle = get_cf_handle(&self.db, h.as_str()).unwrap();
                    match self.db.get_external_cf(handle, self.iter.key(), &ReadOptions::default()) {
                        Ok(Some(v)) => {
                            self.value = v.to_vec();
                            Ok(true)
                        },
                        _ => Err(Error::Engine("get external iterator error".to_string()))
                    }
                }
                else {
                    match self.db.get_external(self.iter.key(), &ReadOptions::default()) {
                        Ok(Some(v)) => {
                            self.value = v.to_vec();
                            dbg!(&self.value);
                            Ok(true)
                        },
                        _ => Err(Error::Engine("get external iterator error".to_string()))
                    }
                }
            }
            Ok(false) => Ok(false)
        }
    }

    fn key(&self) -> &[u8] {
        #[cfg(not(feature = "nortcheck"))]
        assert!(self.valid().unwrap());
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        #[cfg(not(feature = "nortcheck"))]
        assert!(self.valid().unwrap());
        self.value.as_slice()
    }

    fn valid(&self) -> Result<bool> {
        self.iter.valid().map_err(Error::Engine)
    }
}

pub struct RocksSeekKey<'a>(RawSeekKey<'a>);

impl<'a> RocksSeekKey<'a> {
    pub fn into_raw(self) -> RawSeekKey<'a> {
        self.0
    }
}

impl<'a> From<engine_traits::SeekKey<'a>> for RocksSeekKey<'a> {
    fn from(key: engine_traits::SeekKey<'a>) -> Self {
        let k = match key {
            engine_traits::SeekKey::Start => RawSeekKey::Start,
            engine_traits::SeekKey::End => RawSeekKey::End,
            engine_traits::SeekKey::Key(k) => RawSeekKey::Key(k),
        };
        RocksSeekKey(k)
    }
}
