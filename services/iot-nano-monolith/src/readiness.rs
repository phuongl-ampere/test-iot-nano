use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[derive(Clone, Debug, Default)]
pub struct Readiness {
    ready: Arc<AtomicBool>,
}

impl Readiness {
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub(crate) fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    pub(crate) fn mark_not_ready(&self) {
        self.ready.store(false, Ordering::Release);
    }
}
