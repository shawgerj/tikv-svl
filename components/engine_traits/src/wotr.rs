use std::fmt::Debug;
use std::sync::Arc;
use crate::*;
//use crate::options::{ReadOptions};

// shawgerj added
// engines which can accept WOTR external shared log
// should get-external and such be in here?
pub trait WOTRExt: Sized {
    type WOTR: WOTR;
//    type WriteBatch: WriteBatch<E>;
    type DBVector: DBVector;
    
    fn register_valuelog(&mut self, logobj: Arc<Self::WOTR>, recover: bool) -> Result<()>;
    fn have_wotr(&self); 
}

// wotr interface (for now, just create)
pub trait WOTR: Debug + Sized + Sync + Send + 'static {
    fn new(logpath: &str) -> Self;
}
