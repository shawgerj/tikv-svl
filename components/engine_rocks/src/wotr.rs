use crate::engine::RocksEngine;
use engine_traits::{self, Result, WOTRExt};
use rocksdb::{WOTR as RawWOTR, DB};

impl WOTRExt for RocksEngine {
    type WOTR = RocksWOTR;
    
    fn register_valuelog(&self, logobj: &Self::WOTR) -> Result<()> {
        let w = logobj.as_inner();
        self.as_inner().set_wotr(w);
        Ok(())
    }
}

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

