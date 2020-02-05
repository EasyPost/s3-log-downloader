use std::sync::Arc;

use futures::{Async, Future, Poll};
use tokio_sync::semaphore::{AcquireError, Permit, Semaphore};

pub(crate) struct SemaphoreWaiter {
    permit: Option<Permit>,
    semaphore: Arc<Semaphore>,
}

impl SemaphoreWaiter {
    pub fn new(sem: Arc<Semaphore>) -> Self {
        SemaphoreWaiter {
            permit: Some(Permit::new()),
            semaphore: sem,
        }
    }
}

impl Future for SemaphoreWaiter {
    type Item = Permit;
    type Error = AcquireError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Some(ref mut p) = self.permit {
            match p.poll_acquire(&self.semaphore) {
                Ok(Async::Ready(_)) => {
                    let p = self.permit.take();
                    Ok(Async::Ready(p.unwrap()))
                }
                Ok(Async::NotReady) => Ok(Async::NotReady),
                Err(e) => Err(e),
            }
        } else {
            Ok(Async::NotReady)
        }
    }
}
