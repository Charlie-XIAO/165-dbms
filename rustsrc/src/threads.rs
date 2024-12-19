use std::sync::atomic::{AtomicBool, Ordering};

static MULTI_THREADED: AtomicBool = AtomicBool::new(true);

pub fn is_multi_threaded() -> bool {
    MULTI_THREADED.load(Ordering::Relaxed)
}

pub fn set_multi_threaded(multi_threaded: bool) {
    MULTI_THREADED.store(multi_threaded, Ordering::Relaxed);
}
