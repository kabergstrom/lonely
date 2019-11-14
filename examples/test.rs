use lonely::{load_balance, Exec, ExecGroup};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::net::driver;
struct Counter {
    counter: usize,
    num: usize,
}
impl std::future::Future for Counter {
    type Output = ();
    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context,
    ) -> std::task::Poll<Self::Output> {
        self.counter += 1;
        if self.counter >= self.num {
            std::task::Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }
}

fn main() {
    let hash_builder =
        std::hash::BuildHasherDefault::<std::collections::hash_map::DefaultHasher>::default();
    let mut executor_group = ExecGroup::new(load_balance::RoundRobin::new(), hash_builder);
    for _ in 0..5 {
        let exec = Exec::new();
        executor_group.add_executor(exec.send_handle());
        let exec_send = exec.sendable();
        std::thread::spawn(move || {
            let exec = exec_send.into_exec();
            let mut reactor = driver::Reactor::new().unwrap();
            let handle = reactor.handle();
            let guard = driver::set_default(&handle);
            loop {
                exec.poll();
                reactor
                    .turn(Some(std::time::Duration::from_millis(0)))
                    .unwrap();
            }
            drop(guard);
        });
    }

    let yield_value = Arc::new(AtomicUsize::new(0));
    let num_tasks = 100;
    let num_yields: usize = 10;
    let num_spawners = 1000;

    let num_iterations = num_tasks * num_spawners;

    for _ in 0..num_spawners {
        let yield_value_closure = yield_value.clone();
        executor_group.spawn_with_local_spawner(move |ls| {
            for _ in 0..num_tasks {
                let inner_value = yield_value_closure.clone();
                let future = Counter {
                    counter: 0,
                    num: num_yields,
                };
                use futures_util::FutureExt;
                ls.spawn(future.then(move |_| {
                    async move {
                        inner_value.fetch_add(1, Ordering::Relaxed);
                        ()
                    }
                }));
            }
        });
    }
    while yield_value.load(Ordering::Relaxed) != num_iterations {}

    let value = Arc::new(AtomicUsize::new(0));
    let num_tasks = 5;
    for _ in 0..num_tasks {
        let closure_value = value.clone();
        executor_group.spawn(async move {
            closure_value.fetch_add(1, Ordering::Relaxed);
        });
    }
    while value.load(Ordering::Relaxed) != num_tasks {}
    executor_group.spawn_with_local_spawner(move |ls| {
        ls.spawn(async move {
            let mut listener = tokio::net::tcp::TcpListener::bind("127.0.0.1:9000")
                .await
                .unwrap();

            match listener.accept().await {
                Ok((socket, addr)) => println!("new client: {:?}", addr),
                Err(e) => println!("couldn't get client: {:?}", e),
            }
        });
    });
    executor_group.spawn_with_local_spawner(move |ls| {
        ls.spawn(async move {
            use tokio::io::AsyncWriteExt;
            // Connect to a peer
            let mut stream = tokio::net::TcpStream::connect("127.0.0.1:9000")
                .await
                .unwrap();

            // Write some data.
            stream.write_all(b"hello world!").await.unwrap();
        });
    });
    std::thread::sleep_ms(5000);
}
