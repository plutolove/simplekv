use crate::Result;
mod shared_queue;

pub trait ThreadPool {
    fn new(n: i32) -> Result<Self>
        where
            Self: Sized;
    fn spawn<F>(&self, job: F)
        where
            F: FnOnce() + Send + 'static;
}

pub use shared_queue::SharedQueueThreadPool;