use crate::*;
use crate::options::{ReadOptions};

// shawgerj added
// engines which can accept WOTR external shared log
// should get-external and such be in here?
pub trait WOTRExt: Sized {
    type WOTR: WOTR;
//    type WriteBatch: WriteBatch<E>;
    type DBVector: DBVector;
    
    fn register_valuelog(&self, logobj: &Self::WOTR) -> Result<()>;

    fn get_valuelog(&self,
                readopts: &ReadOptions,
                key: &[u8],
    ) -> Result<Option<Self::DBVector>>;
       
}

// wotr interface (for now, just create)
pub trait WOTR: Sized {
    fn new(logpath: &str) -> Self;
}
