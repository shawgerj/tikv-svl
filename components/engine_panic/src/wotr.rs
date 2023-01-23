use crate::engine::PanicEngine;
use crate::db_vector::PanicDBVector;
use engine_traits::{Result, WOTRExt, WOTR, ReadOptions};
//use rocksdb::{WOTR as RawWOTR, DB};

impl WOTRExt for PanicEngine {
    type DBVector = PanicDBVector;
    type WOTR = PanicWOTR;

    fn register_valuelog(&self, logobj: &Self::WOTR) -> Result<()> {
        panic!()
    }

    fn get_valuelog(&self,
                    _: &ReadOptions,
                    _: &[u8]
    ) -> Result<Option<PanicDBVector>> {
        panic!()
    }
}

pub struct PanicWOTR;

impl WOTR for PanicWOTR {
    fn new(logpath: &str) -> PanicWOTR {
        panic!()
    }
}
