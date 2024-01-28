use std::fmt::Debug;
use std::sync::Arc;
use crate::engine::PanicEngine;
use crate::db_vector::PanicDBVector;
use engine_traits::{Result, WOTRExt, WOTR, ReadOptions};
//use rocksdb::{WOTR as RawWOTR, DB};

impl WOTRExt for PanicEngine {
    type DBVector = PanicDBVector;
    type WOTR = PanicWOTR;

    fn register_valuelog(&mut self, logobj: Arc<Self::WOTR>, recover: bool) -> Result<()> {
        panic!()
    }

    fn have_wotr(&self) {
        panic!()
    }
}

#[derive(Debug)]
pub struct PanicWOTR;

impl WOTR for PanicWOTR {
    fn new(logpath: &str) -> PanicWOTR {
        panic!()
    }
}
