use crate::*;

// shawgerj added
// engines which can accept WOTR external shared log
// should get-external and such be in here?
pub trait WOTRExt: Sized {
    type WOTR: WOTR;
    fn register_valuelog(&self, logobj: &Self::WOTR) -> Result<()>;
}

// wotr interface (for now, just create)
pub trait WOTR: Sized {
    fn new(logpath: &str) -> Self;
}
