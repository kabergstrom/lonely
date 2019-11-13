#![feature(test)]

extern crate test;

use lonely::{load_balance, Exec, ExecGroup, LocalSpawn};
use tokio::sync::oneshot;

use mimalloc::MiMalloc;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{mpsc, Arc};
use std::task::{Context, Poll};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

struct Backoff(usize);

impl Future for Backoff {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 == 0 {
            Poll::Ready(())
        } else {
            self.0 -= 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

const NUM_THREADS: usize = 8;

fn make_exec_group(num_threads: usize) -> ExecGroup<load_balance::RoundRobin> {
    let hash_builder = lonely::DefaultBuildHasher::default();
    let mut executor_group = ExecGroup::new(load_balance::RoundRobin::new(), hash_builder);
    for _ in 0..num_threads {
        let exec = Exec::new();
        let send_handle = exec.send_handle();
        let exec_send = exec.sendable();
        std::thread::spawn(move || {
            let exec = exec_send.into_exec();
            let mut reactor = tokio::net::driver::Reactor::new().unwrap();
            let handle = reactor.handle();
            let _guard = tokio::net::driver::set_default(&handle);
            loop {
                exec.poll();
                reactor
                    .turn(Some(std::time::Duration::from_millis(0)))
                    .expect("reactor error");
            }
            drop(_guard);
        });
        let buf = lonely::Ring::new(4);
        let future_buf = buf.clone();
        send_handle.send(async move {
            future_buf.push(1);
        });
        while let None = buf.pop() {}
        executor_group.add_executor(send_handle);
    }
    executor_group
}

#[bench]
fn spawn_many(b: &mut test::Bencher) {
    const NUM_SPAWN: usize = 10_000;

    let threadpool = make_exec_group(NUM_THREADS);

    let (tx, rx) = mpsc::sync_channel(1000);
    let rem = Arc::new(AtomicUsize::new(0));

    b.iter(|| {
        rem.store(NUM_SPAWN, Relaxed);
        let tx = tx.clone();

        let rem = rem.clone();
        for _ in 0..NUM_SPAWN {
            let tx = tx.clone();
            let rem = rem.clone();

            threadpool.spawn(async move {
                if 1 == rem.fetch_sub(1, Relaxed) {
                    tx.send(()).unwrap();
                }
            });
        }

        let _ = rx.recv().unwrap();
    });
}

#[bench]
fn yield_many(b: &mut test::Bencher) {
    const NUM_YIELD: usize = 1_000;
    const TASKS_PER_CPU: usize = 50;

    let threadpool = make_exec_group(NUM_THREADS);

    let tasks = TASKS_PER_CPU * NUM_THREADS;
    let (tx, rx) = mpsc::sync_channel(tasks);

    b.iter(move || {
        let tx = tx.clone();
        for _ in 0..tasks {
            let tx = tx.clone();

            threadpool.spawn(async move {
                let backoff = Backoff(NUM_YIELD);
                backoff.await;
                tx.send(()).unwrap();
            });
        }

        for _ in 0..tasks {
            let _ = rx.recv().unwrap();
        }
    });
}

// The stabilized Waker API effectively penalizes designs like `lonely` from the async ecosystem,
// since the removal of LocalWaker: https://boats.gitlab.io/blog/post/wakers-ii/
// For lonely I've explicitly chosen to NOT implement the Waker's `Send/Sync` properties,
// which makes it unsound. Very unfortunate, but the alternative would be too costly for my goals.
// #[bench]
// fn ping_pong(b: &mut test::Bencher) {
//     const NUM_PINGS: usize = 1_000;

//     let threadpool = make_exec_group(NUM_THREADS);

//     let (done_tx, done_rx) = mpsc::sync_channel(1000);
//     let rem = Arc::new(AtomicUsize::new(0));

//     b.iter(|| {
//         let done_tx = done_tx.clone();
//         let rem = rem.clone();
//         rem.store(NUM_PINGS, Relaxed);

//         threadpool.spawn_with_local_spawner(move |spawner| {
//             for _ in 0..NUM_PINGS {
//                 let rem = rem.clone();
//                 let done_tx = done_tx.clone();

//                 let spawner2 = spawner.clone();

//                 spawner.spawn(async move {
//                     let (tx1, rx1) = oneshot::channel();
//                     let (tx2, rx2) = oneshot::channel();

//                     spawner2.spawn(async move {
//                         rx1.await.unwrap();
//                         tx2.send(()).unwrap();
//                     });

//                     tx1.send(()).unwrap();
//                     rx2.await.unwrap();

//                     if 1 == rem.fetch_sub(1, Relaxed) {
//                         done_tx.send(()).unwrap();
//                     }
//                 });
//             }
//         });

//         done_rx.recv().unwrap();
//     });
// }

#[bench]
fn chained_spawn(b: &mut test::Bencher) {
    const ITER: usize = 1_000;

    let threadpool = make_exec_group(NUM_THREADS);

    fn iter(spawner: LocalSpawn, done_tx: mpsc::SyncSender<()>, n: usize) {
        if n == 0 {
            done_tx.send(()).unwrap();
        } else {
            let s2 = spawner.clone();
            spawner.spawn(async move {
                iter(s2, done_tx, n - 1);
            });
        }
    }

    let (done_tx, done_rx) = mpsc::sync_channel(1000);

    b.iter(move || {
        let done_tx = done_tx.clone();
        threadpool.spawn_with_local_spawner(move |spawner| {
            iter(spawner, done_tx, ITER);
        });

        done_rx.recv().unwrap();
    });
}
