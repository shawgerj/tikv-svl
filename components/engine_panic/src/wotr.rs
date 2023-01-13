use crate::engine::PanicEngine;
use engine_traits::{Result, WOTRExt, WOTR};
//use rocksdb::{WOTR as RawWOTR, DB};

impl WOTRExt for PanicEngine {
    type WOTR = PanicWOTR;

    fn register_valuelog(&self, logobj: &Self::WOTR) -> Result<()> {
        panic!()
    }
}

pub struct PanicWOTR;

impl WOTR for PanicWOTR {
    fn new(logpath: &str) -> PanicWOTR {
        panic!()
    }
}
