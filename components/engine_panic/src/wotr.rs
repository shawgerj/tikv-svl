use std::rc::Rc;
use crate::engine::PanicEngine;
use crate::db_vector::PanicDBVector;
use engine_traits::{Result, WOTRExt, WOTR, ReadOptions};
//use rocksdb::{WOTR as RawWOTR, DB};

impl WOTRExt for PanicEngine {
    type DBVector = PanicDBVector;
    type WOTR = PanicWOTR;

    fn register_valuelog(&self, logobj: Rc<Self::WOTR>) -> Result<()> {
        panic!()
    }
}

pub struct PanicWOTR;

impl WOTR for PanicWOTR {
    fn new(logpath: &str) -> PanicWOTR {
        panic!()
    }
}
