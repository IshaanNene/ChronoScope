//! A minimal async channel.
//!
//! The driver needs to wait on three sources at once — inbound messages, the
//! tick timer, and locally generated work — and the trait surface deliberately
//! offers no `select!`. Funnelling everything into one queue is simpler than a
//! combinator, and it has a property that matters more: the driver processes
//! events in a single, observable order, so *when* it batches is explicit
//! rather than emergent. That batching is group commit.
//!
//! Wakers are collected and fired after the lock is released. Waking re-enters
//! the executor, and holding a lock across that is how simulators deadlock in a
//! way that looks like a bug in the system under test.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct Inner<T> {
    queue: VecDeque<T>,
    wakers: Vec<Waker>,
    closed: bool,
}

/// A multi-producer, single-consumer queue. Cloning gives another handle to
/// the same queue.
pub struct Chan<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T> Clone for Chan<T> {
    fn clone(&self) -> Self {
        Chan { inner: Arc::clone(&self.inner) }
    }
}

impl<T> Default for Chan<T> {
    fn default() -> Self {
        Chan::new()
    }
}

impl<T> std::fmt::Debug for Chan<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().unwrap();
        f.debug_struct("Chan")
            .field("queued", &inner.queue.len())
            .field("closed", &inner.closed)
            .finish()
    }
}

impl<T> Chan<T> {
    pub fn new() -> Chan<T> {
        Chan {
            inner: Arc::new(Mutex::new(Inner {
                queue: VecDeque::new(),
                wakers: Vec::new(),
                closed: false,
            })),
        }
    }

    pub fn send(&self, value: T) {
        let wakers = {
            let mut inner = self.inner.lock().unwrap();
            if inner.closed {
                return;
            }
            inner.queue.push_back(value);
            std::mem::take(&mut inner.wakers)
        };
        for w in wakers {
            w.wake();
        }
    }

    /// Take an item if one is waiting. Used to drain a burst without yielding,
    /// which is what turns a burst of proposals into one fsync.
    pub fn try_recv(&self) -> Option<T> {
        self.inner.lock().unwrap().queue.pop_front()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Wait for the next item. Resolves to `None` once the channel is closed
    /// and drained.
    pub fn recv(&self) -> Recv<'_, T> {
        Recv { chan: self }
    }

    pub fn close(&self) {
        let wakers = {
            let mut inner = self.inner.lock().unwrap();
            inner.closed = true;
            std::mem::take(&mut inner.wakers)
        };
        for w in wakers {
            w.wake();
        }
    }
}

pub struct Recv<'a, T> {
    chan: &'a Chan<T>,
}

impl<T> Future for Recv<'_, T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut inner = self.chan.inner.lock().unwrap();
        if let Some(v) = inner.queue.pop_front() {
            return Poll::Ready(Some(v));
        }
        if inner.closed {
            return Poll::Ready(None);
        }
        inner.wakers.push(cx.waker().clone());
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A one-shot executor: polls a future to completion, with a waker that
    /// just records that it fired. Enough to test the channel without pulling
    /// in the simulator.
    fn poll_once<F: Future>(fut: &mut Pin<Box<F>>, woken: &Arc<AtomicUsize>) -> Poll<F::Output> {
        struct W(Arc<AtomicUsize>);
        impl std::task::Wake for W {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let waker = Waker::from(Arc::new(W(Arc::clone(woken))));
        let mut cx = Context::from_waker(&waker);
        fut.as_mut().poll(&mut cx)
    }

    #[test]
    fn a_queued_item_is_returned_immediately() {
        let c: Chan<u32> = Chan::new();
        c.send(7);
        let woken = Arc::new(AtomicUsize::new(0));
        let mut f = Box::pin(c.recv());
        assert_eq!(poll_once(&mut f, &woken), Poll::Ready(Some(7)));
        assert_eq!(woken.load(Ordering::SeqCst), 0, "no wake needed when an item is ready");
    }

    #[test]
    fn an_empty_channel_parks_and_a_send_wakes_it() {
        let c: Chan<u32> = Chan::new();
        let woken = Arc::new(AtomicUsize::new(0));
        let mut f = Box::pin(c.recv());
        assert_eq!(poll_once(&mut f, &woken), Poll::Pending);
        assert_eq!(woken.load(Ordering::SeqCst), 0);
        c.send(1);
        assert_eq!(woken.load(Ordering::SeqCst), 1, "the send must wake the parked receiver");
        assert_eq!(poll_once(&mut f, &woken), Poll::Ready(Some(1)));
    }

    #[test]
    fn closing_wakes_and_then_yields_none() {
        let c: Chan<u32> = Chan::new();
        let woken = Arc::new(AtomicUsize::new(0));
        let mut f = Box::pin(c.recv());
        assert_eq!(poll_once(&mut f, &woken), Poll::Pending);
        c.close();
        assert_eq!(woken.load(Ordering::SeqCst), 1);
        assert_eq!(poll_once(&mut f, &woken), Poll::Ready(None));
    }

    #[test]
    fn a_closed_channel_still_drains_what_was_queued() {
        let c: Chan<u32> = Chan::new();
        c.send(1);
        c.send(2);
        c.close();
        let woken = Arc::new(AtomicUsize::new(0));
        for want in [Some(1), Some(2), None] {
            let mut f = Box::pin(c.recv());
            assert_eq!(poll_once(&mut f, &woken), Poll::Ready(want));
        }
    }

    #[test]
    fn ordering_is_fifo_across_producers() {
        let c: Chan<u32> = Chan::new();
        let a = c.clone();
        let b = c.clone();
        a.send(1);
        b.send(2);
        a.send(3);
        assert_eq!(c.try_recv(), Some(1));
        assert_eq!(c.try_recv(), Some(2));
        assert_eq!(c.try_recv(), Some(3));
        assert_eq!(c.try_recv(), None);
    }

    #[test]
    fn try_recv_drains_a_burst_without_awaiting() {
        // This is the group-commit primitive: take everything queued right now,
        // handle it as one batch, fsync once.
        let c: Chan<u32> = Chan::new();
        for i in 0..50 {
            c.send(i);
        }
        let mut drained = Vec::new();
        while let Some(v) = c.try_recv() {
            drained.push(v);
        }
        assert_eq!(drained, (0..50).collect::<Vec<_>>());
    }
}
