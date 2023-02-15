use std::sync::Arc;
use crate::engine::RocksEngine;
//use crate::RocksWriteBatch;
//use crate::options::RocksReadOptions;
use crate::db_vector::RocksDBVector;
use engine_traits::{self, Error, Result, WOTRExt};
use rocksdb::{WOTR as RawWOTR};

impl WOTRExt for RocksEngine {
//    type WriteBatch = RocksWriteBatch;
    type DBVector = RocksDBVector;
    type WOTR = RocksWOTR;
    
    fn register_valuelog(&mut self, logobj: Arc<Self::WOTR>) -> Result<()> {
        self.set_wotr(logobj.clone());
        let w = logobj.as_inner();
        self.as_inner().set_wotr(w).map_err(Error::Engine)
    }

    fn have_wotr(&self) {
        self.have_wotr();
    }
}

#[derive(Debug)]
pub struct RocksWOTR {
    w: RawWOTR,
}

impl engine_traits::WOTR for RocksWOTR {
    fn new(logpath: &str) -> RocksWOTR {
        RocksWOTR {
            w: RawWOTR::wotr_init(logpath).unwrap(),
        }
    }
}

impl RocksWOTR {
    pub fn as_inner(&self) -> &RawWOTR {
        &self.w
    }
}    

#[cfg(test)]
mod test {
    use super::super::util::new_engine_opt;
    use super::super::RocksDBOptions;
    use super::*;
    use engine_traits::WOTR;
    use rocksdb::DBOptions as RawDBOptions;
    use tempfile::Builder;

    #[test]
    fn test_wotr_register() {
        let path = Builder::new()
            .prefix("test-wotr-register")
            .tempdir().
            unwrap();
                    
        let w = Arc::new(RocksWOTR::new(path.path().join("wotrlog.txt").to_str().unwrap()));
        let opt = RawDBOptions::default();
        let mut engine = new_engine_opt(
            path.path().join("db").to_str().unwrap(),
            RocksDBOptions::from_raw(opt),
            vec![],
        ).unwrap();

        assert!(engine.register_valuelog(w.clone()).is_ok());
    }
}

