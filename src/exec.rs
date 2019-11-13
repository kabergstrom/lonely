use crate::Ring;
use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    future::*,
    hash::{BuildHasher, Hash},
    pin::Pin,
    ptr::NonNull,
    rc::Rc,
    task::*,
};

#[repr(u8)]
#[derive(PartialEq)]
enum TaskState {
    None,
    Waking,
    Done,
}

struct FutureObj<T: ?Sized> {
    vtable_ptr: usize,
    executor: *const ExecInner,
    task_state: UnsafeCell<TaskState>,
    value: T,
}

type SendQueue = Ring<NonNull<()>>;

/// Decompose a fat pointer into its constituent [pointer, extdata] pair
unsafe fn decomp_fat<T: ?Sized>(ptr: *const T) -> [usize; 2] {
    let ptr_ref: *const *const T = &ptr;
    let decomp_ref = ptr_ref as *const [usize; 2];
    *decomp_ref
}

/// Recompose a fat pointer from its constituent [pointer, extdata] pair
unsafe fn recomp_fat<T: ?Sized>(components: [usize; 2]) -> *const T {
    let component_ref: *const [usize; 2] = &components;
    let ptr_ref = component_ref as *const *const T;
    *ptr_ref
}

#[inline(always)]
unsafe fn create_future_obj<T: Future<Output = ()> + 'static>(
    executor: *const ExecInner,
    f: T,
) -> NonNull<()> {
    let data = FutureObj {
        vtable_ptr: 0,
        executor,
        task_state: UnsafeCell::new(TaskState::Waking),
        value: f,
    };
    let b = Rc::new(data);
    let ptr = Rc::into_raw(b) as *const FutureObj<dyn Future<Output = ()>>
        as *mut FutureObj<dyn Future<Output = ()>>;
    let decomposed = decomp_fat(ptr);
    (*ptr).vtable_ptr = decomposed[1];
    NonNull::new_unchecked(decomposed[0] as *mut ())
}

struct ExecInner {
    poll_queue: UnsafeCell<VecDeque<NonNull<()>>>,
    recv_buffer: UnsafeCell<Vec<NonNull<()>>>,
    send_queue: UnsafeCell<Option<SendQueue>>,
}
impl ExecInner {
    unsafe fn poll_queue_mut<'a>(&'a self) -> &'a mut VecDeque<NonNull<()>> {
        let queue = &mut *self.poll_queue.get();
        std::mem::transmute::<&mut VecDeque<NonNull<()>>, &'a mut VecDeque<NonNull<()>>>(queue)
    }
    unsafe fn poll_queue(&self) -> &VecDeque<NonNull<()>> {
        &*self.poll_queue.get()
    }
    unsafe fn receiver(&self) -> Option<&SendQueue> {
        let send_queue = &*self.send_queue.get();
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

    pub fn spawn<T: Future<Output = ()> + 'static>(&self, f: T) {
        unsafe {
            let future_obj = create_future_obj(self.inner.as_ref(), f);
            self.inner.poll_queue_mut().push_back(future_obj);
        }
    }

    pub fn poll(&self) -> bool {
        unsafe {
            let mut waker = Waker::from_raw(RawWaker::new(std::ptr::null(), &WAKER_VTABLE));
            let waker_ptr = &mut (*(&mut waker as *mut Waker as *mut RawWakerHack)).data;
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
                    for i in 0..futures_received {
                        let ptr = *recv_buf.get_unchecked(i);
                        let rc = ptr_to_rc(ptr);
                        let value = &mut *(&rc.executor as *const *const ExecInner
                            as *mut *const ExecInner);
                        *value = self.inner.as_ref() as *const ExecInner;
                        std::mem::forget(rc);
                        poll_queue.push_back(ptr);
                    }
                }
            }
            let poll_queue = self.inner.poll_queue();
            let len = poll_queue.len();
            if len == 0 {
                std::mem::forget(waker);
                return false;
            }

            while !self.inner.poll_queue().is_empty() {
                let to_poll = poll_queue.len();
                for i in 0..to_poll {
                    let f = {
                        let poll_queue = self.inner.poll_queue();
                        *poll_queue.get(i).unwrap()
                    };
                    let b = ptr_to_rc(f);
                    if *b.task_state.get() == TaskState::Done {
                        continue;
                    }
                    *b.task_state.get() = TaskState::None;
                    let value = &mut *(&b.value as *const dyn Future<Output = ()>
                        as *mut dyn Future<Output = ()>);

                    *waker_ptr = f.as_ptr();
                    let mut ctx = Context::from_waker(&waker);
                    match Pin::new_unchecked(value).poll(&mut ctx) {
                        Poll::Ready(_) => {
                            *b.task_state.get() = TaskState::Done;
                        }
                        Poll::Pending => {} // future is dropped if it's the last ref, otherwise handled by Waker clone
                    }
                }
                let poll_queue = self.inner.poll_queue_mut();
                poll_queue.drain(0..to_poll);
            }
            std::mem::forget(waker);
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
            self.sender
                .push(create_future_obj(std::ptr::null(), f))
                .is_none()
        }
    }
    /// Returns f back to the caller if the send was not successful
    #[inline(always)]
    fn send_internal(&self, f: NonNull<()>) -> Option<NonNull<()>> {
        self.sender.push(f)
    }
}

const WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);

unsafe fn clone(f: *const ()) -> RawWaker {
    let rc = ptr_to_rc(NonNull::new_unchecked(f as *mut ()));
    let clone = rc.clone();
    std::mem::forget(rc);
    RawWaker::new(rc_to_ptr(clone).as_ptr(), &WAKER_VTABLE)
}
unsafe fn wake(f: *const ()) {
    let ptr = NonNull::new_unchecked(f as *mut ());
    let rc = ptr_to_rc(ptr);
    if *rc.task_state.get() == TaskState::None {
        *rc.task_state.get() = TaskState::Waking;
        (&*rc.executor).poll_queue_mut().push_back(ptr);
        // ownership of the RC transfer to the poll_queue
        std::mem::forget(rc);
    }
}

pub unsafe fn wake_by_ref(f: *const ()) {
    let ptr = NonNull::new_unchecked(f as *mut ());
    let rc = ptr_to_rc(ptr);
    if *rc.task_state.get() == TaskState::None {
        *rc.task_state.get() = TaskState::Waking;
        (&*rc.executor)
            .poll_queue_mut()
            .push_back(rc_to_ptr(rc.clone()));
    }
    std::mem::forget(rc);
}
unsafe fn drop(f: *const ()) {
    ptr_to_rc(NonNull::new_unchecked(f as *mut ()));
    // RC will drop when out of scope
}

unsafe fn rc_to_ptr(rc: Rc<FutureObj<dyn Future<Output = ()>>>) -> NonNull<()> {
    NonNull::new_unchecked(decomp_fat(Rc::into_raw(rc))[0] as *mut ())
}
unsafe fn ptr_to_rc(ptr: NonNull<()>) -> Rc<FutureObj<dyn Future<Output = ()>>> {
    let fat_ptr = recomp_fat([ptr.as_ptr() as usize, *ptr.cast::<usize>().as_ptr()]);
    Rc::from_raw(fat_ptr)
}

// To avoid calling RawWaker::new (doesn't inline), we just write directly to `data`
pub struct RawWakerHack {
    data: *const (),
    _vtable: &'static RawWakerVTable,
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
    executor: *const ExecInner,
}
impl LocalSpawn {
    pub fn spawn<T: Future<Output = ()> + 'static>(&self, f: T) {
        unsafe {
            let poll_queue = (&*self.executor).poll_queue_mut();
            poll_queue.push_back(create_future_obj(self.executor, f));
        }
    }
}

struct GlobalSpawn<F: FnOnce(LocalSpawn)> {
    f: Option<F>,
}
impl<F: FnOnce(LocalSpawn)> Unpin for GlobalSpawn<F> {}
impl<F: FnOnce(LocalSpawn)> Future for GlobalSpawn<F> {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> std::task::Poll<Self::Output> {
        let waker = cx.waker();
        let executor = unsafe {
            let hack = std::mem::transmute::<&Waker, &RawWaker>(waker);
            let hack = &*(hack as *const RawWaker as *const RawWakerHack);
            let self_future_obj = ptr_to_rc(NonNull::new_unchecked(hack.data as *mut ()));
            let executor = self_future_obj.executor;
            std::mem::forget(self_future_obj);
            executor
        };
        if let Some(f) = self.f.take() {
            (f)(LocalSpawn { executor });
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
        let mut future_obj =
            unsafe { create_future_obj(std::ptr::null(), GlobalSpawn { f: Some(f) }) };
        loop {
            let worker_id = self.load_balance.worker_id();
            match self.send_handles[worker_id].send_internal(future_obj) {
                Some(recycled) => future_obj = recycled,
                None => break,
            }
        }
    }
    #[inline(always)]
    pub fn spawn<T: Future<Output = ()> + Send + 'static>(&self, f: T) {
        let mut future_obj = unsafe { create_future_obj(std::ptr::null(), f) };
        loop {
            let worker_id = self.load_balance.worker_id();
            match self.send_handles[worker_id].send_internal(future_obj) {
                Some(recycled) => future_obj = recycled,
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
                    std::thread::sleep_ms(0);
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
