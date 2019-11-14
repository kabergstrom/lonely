use crate::HeapRing;
pub use async_task::JoinHandle;
use core::{
    cell::UnsafeCell,
    future::*,
    hash::{BuildHasher, Hash},
    mem::MaybeUninit,
    pin::Pin,
    task::*,
};
#[cfg(feature = "std")]
use std::{collections::VecDeque, rc::Rc};

type TaskTag = MaybeUninit<Rc<ExecInner>>;
type Task = async_task::Task<TaskTag>;
#[repr(transparent)]
struct SendTask(Task);
unsafe impl Send for SendTask {}
type SendQueue = HeapRing<SendTask>;
fn spawn_task<R: 'static, T: Future<Output = R> + 'static>(
    f: T,
    executor: TaskTag,
) -> JoinHandle<R, TaskTag> {
    let (task, handle) = create_task(f, executor);
    task.schedule();
    handle
}
fn create_task<R: 'static, T: Future<Output = R> + 'static>(
    f: T,
    tag: TaskTag,
) -> (Task, JoinHandle<R, TaskTag>) {
    unsafe {
        async_task::spawn(
            f,
            |task| {
                let exec = &*(&*(task.tag() as *const TaskTag)).as_ptr();
                exec.poll_queue_mut().push_back(task);
            },
            tag,
        )
    }
}
pub struct ExecInner {
    poll_queue: UnsafeCell<VecDeque<Task>>,
    recv_buffer: UnsafeCell<Vec<SendTask>>,
    send_queue: UnsafeCell<Option<SendQueue>>,
}
impl ExecInner {
    unsafe fn poll_queue_mut<'a>(&'a self) -> &'a mut VecDeque<Task> {
        let queue = &mut *self.poll_queue.get();
        std::mem::transmute::<&mut VecDeque<Task>, &'a mut VecDeque<Task>>(queue)
    }
    unsafe fn poll_queue(&self) -> &VecDeque<Task> {
        &*self.poll_queue.get()
    }
    fn receiver(&self) -> Option<&SendQueue> {
        let send_queue = unsafe { &*self.send_queue.get() };
        send_queue.as_ref()
    }
}
pub struct ExecSend {
    inner: Rc<ExecInner>,
}
// We only construct ExecSend if the Rc has no other references, so it should be safe
unsafe impl Send for ExecSend {}
impl ExecSend {
    pub fn into_exec(self) -> Exec {
        Exec { inner: self.inner }
    }
}
pub struct Exec {
    inner: Rc<ExecInner>,
}
impl Exec {
    pub fn new() -> Self {
        Self {
            inner: Rc::new(ExecInner {
                poll_queue: UnsafeCell::new(VecDeque::with_capacity(10_000)),
                recv_buffer: UnsafeCell::new(Vec::with_capacity(64)),
                send_queue: UnsafeCell::new(None),
            }),
        }
    }
    pub fn sendable(self) -> ExecSend {
        if Rc::strong_count(&self.inner) != 1 || Rc::weak_count(&self.inner) != 0 {
            panic!("executor has more than 1 reference");
        }
        ExecSend { inner: self.inner }
    }
    pub fn send_handle(&self) -> SendHandle {
        unsafe {
            let send_queue = &mut *self.inner.send_queue.get();
            let sender = send_queue.get_or_insert_with(|| SendQueue::new(1_024));
            SendHandle {
                sender: sender.clone(),
            }
        }
    }
    pub fn spawn<R: 'static, T: Future<Output = R> + 'static>(
        &self,
        f: T,
    ) -> JoinHandle<R, TaskTag> {
        spawn_task(f, TaskTag::new(self.inner.clone()))
    }

    pub fn poll(&self) -> bool {
        unsafe {
            let receiver = self.inner.receiver();
            if let Some(receiver) = receiver {
                let poll_queue = self.inner.poll_queue_mut();
                let recv_buf = &mut *self.inner.recv_buffer.get();
                loop {
                    let futures_received =
                        receiver.pop_up_to(recv_buf.capacity(), recv_buf.as_mut_ptr());
                    recv_buf.set_len(futures_received);
                    if futures_received == 0 {
                        break;
                    }
                    for sent_task in recv_buf.drain(0..) {
                        let task = sent_task.0;
                        // Set the executor tag of the received task
                        let mut_tag_ptr = task.tag() as *const TaskTag as *mut TaskTag;
                        std::ptr::write(mut_tag_ptr, TaskTag::new(self.inner.clone()));
                        poll_queue.push_back(task);
                    }
                }
            }
            let poll_queue = self.inner.poll_queue();
            let len = poll_queue.len();
            if len == 0 {
                return false;
            }

            while !self.inner.poll_queue().is_empty() {
                let to_poll = poll_queue.len();
                for _ in 0..to_poll {
                    let task = {
                        let poll_queue = self.inner.poll_queue_mut();
                        poll_queue.pop_front().unwrap()
                    };
                    task.run();
                }
            }
            !poll_queue.is_empty()
        }
    }
}

#[derive(Clone)]
pub struct SendHandle {
    sender: SendQueue,
}
unsafe impl Send for SendHandle {}
impl SendHandle {
    pub fn send<T: Future<Output = ()> + Send + 'static>(&self, f: T) -> bool {
        unsafe {
            let (task, _) = create_task(f, std::mem::zeroed());
            self.sender.push(SendTask(task)).is_none()
        }
    }
    /// Returns f back to the caller if the send was not successful
    #[inline(always)]
    fn send_internal(&self, f: SendTask) -> Option<SendTask> {
        self.sender.push(f)
    }
}

#[cfg(feature = "tls_exec")]
pub use thread_local::{poll, spawn};
#[cfg(feature = "tls_exec")]
mod thread_local {
    use super::*;
    std::thread_local! {
        static EXECUTOR: Exec = Exec::new();
    }

    /// Spawn using the thread-local executor
    pub fn spawn<T: Future<Output = ()> + 'static>(f: T) {
        EXECUTOR.with(|exec| exec.spawn(f))
    }

    pub fn poll() -> bool {
        EXECUTOR.with(|exec| exec.poll())
    }
}

pub mod load_balance {
    use super::*;
    pub struct RoundRobin {
        num_workers: usize,
        current_id: UnsafeCell<usize>,
    }
    impl Clone for RoundRobin {
        fn clone(&self) -> Self {
            Self {
                num_workers: self.num_workers,
                current_id: UnsafeCell::new(0),
            }
        }
    }
    impl RoundRobin {
        pub fn new() -> Self {
            Self {
                num_workers: 0,
                current_id: UnsafeCell::new(0),
            }
        }
    }
    impl LoadBalanceStrategy for RoundRobin {
        fn set_num_workers(&mut self, num_workers: usize) {
            self.num_workers = num_workers;
            unsafe {
                *self.current_id.get() = 0;
            }
        }
        #[inline(always)]
        fn worker_id(&self) -> usize {
            unsafe {
                let id = *self.current_id.get();
                *self.current_id.get() = (id + 1) % self.num_workers;
                id
            }
        }
        #[inline(always)]
        fn worker_id_hash(&self, _hash: u64) -> usize {
            self.worker_id()
        }
    }
}

pub trait LoadBalanceStrategy {
    fn set_num_workers(&mut self, num_workers: usize);
    fn worker_id(&self) -> usize;
    fn worker_id_hash(&self, hash: u64) -> usize;
}

#[derive(Clone)]
pub struct LocalSpawn {
    executor: Rc<ExecInner>,
}
impl LocalSpawn {
    pub fn spawn<R: 'static, T: Future<Output = R> + 'static>(
        &self,
        f: T,
    ) -> JoinHandle<R, TaskTag> {
        spawn_task(f, TaskTag::new(self.executor.clone()))
    }
}

struct GlobalSpawn<F: FnOnce(LocalSpawn)> {
    f: Option<F>,
}
impl<F: FnOnce(LocalSpawn)> Unpin for GlobalSpawn<F> {}
impl<F: FnOnce(LocalSpawn)> Future for GlobalSpawn<F> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> std::task::Poll<Self::Output> {
        let executor = unsafe { async_task::Task::<Rc<ExecInner>>::tag_from_context(cx) };
        if let Some(f) = self.f.take() {
            (f)(LocalSpawn {
                executor: executor.clone(),
            });
        }
        std::task::Poll::Ready(())
    }
}

#[cfg(feature = "lb_ahash")]
pub type DefaultBuildHasher = ahash::ABuildHasher;
#[cfg(not(feature = "lb_ahash"))]
pub type DefaultBuildHasher =
    std::hash::BuildHasherDefault<std::collections::hash_map::DefaultHasher>;
pub struct ExecGroup<L: LoadBalanceStrategy, H: BuildHasher = DefaultBuildHasher> {
    send_handles: Vec<SendHandle>,
    load_balance: L,
    hasher: H,
}

impl<L: LoadBalanceStrategy, H: BuildHasher> ExecGroup<L, H> {
    pub fn new(load_balance: L, hasher: H) -> Self {
        Self {
            send_handles: Vec::new(),
            load_balance,
            hasher,
        }
    }
    #[inline(always)]
    pub fn spawn_with_local_spawner<F: FnOnce(LocalSpawn) + Send + 'static>(&self, f: F) {
        let (task, handle) = unsafe { create_task(GlobalSpawn { f: Some(f) }, std::mem::zeroed()) };
        drop(handle);
        let mut task = SendTask(task);
        loop {
            let worker_id = self.load_balance.worker_id();
            match self.send_handles[worker_id].send_internal(task) {
                Some(recycled) => task = recycled,
                None => break,
            }
        }
    }
    #[inline(always)]
    pub fn spawn<T: Future<Output = ()> + Send + 'static>(&self, f: T) {
        let (task, handle) = unsafe { create_task(f, std::mem::zeroed()) };
        drop(handle);
        let mut task = SendTask(task);
        loop {
            let worker_id = self.load_balance.worker_id();
            match self.send_handles[worker_id].send_internal(task) {
                Some(recycled) => task = recycled,
                None => break,
            }
        }
    }
    #[inline(always)]
    pub fn spawn_with_hash<T: Future<Output = ()> + Send + 'static, HV: Hash>(
        &self,
        f: T,
        hash: HV,
    ) {
        use std::hash::Hasher;
        let mut hasher = self.hasher.build_hasher();
        hash.hash(&mut hasher);
        let hash_u64 = hasher.finish();
        let worker_id = self.load_balance.worker_id_hash(hash_u64);
        self.send_handles[worker_id].send(f);
    }
    pub fn add_executor(&mut self, send_handle: SendHandle) {
        self.send_handles.push(send_handle);
        self.load_balance.set_num_workers(self.send_handles.len());
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
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

    #[test]
    fn test_poll_iter() {
        let exec = Exec::new();
        exec.spawn(Counter {
            counter: 0,
            num: 1000,
        });
        while exec.poll() {}
    }

    #[test]
    fn test_send_group() {
        let hash_builder =
            std::hash::BuildHasherDefault::<std::collections::hash_map::DefaultHasher>::default();
        let mut executor_group = ExecGroup::new(load_balance::RoundRobin::new(), hash_builder);
        for _ in 0..5 {
            let exec = Exec::new();
            executor_group.add_executor(exec.send_handle());
            let exec_send = exec.sendable();
            std::thread::spawn(move || {
                let exec = exec_send.into_exec();
                loop {
                    exec.poll();
                    std::thread::sleep(std::time::Duration::from_millis(0));
                }
            });
        }

        let mut value = Arc::new(AtomicBool::new(false));
        let closure_value = value.clone();
        executor_group.spawn(async move {
            closure_value.store(true, Ordering::Relaxed);
        });
        while !value.load(Ordering::Relaxed) {}
    }
}
