use crossbeam_utils::CachePadded;
use std::alloc::Layout;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct RingInner<T> {
    cons_head: CachePadded<AtomicUsize>,
    cons_tail: CachePadded<AtomicUsize>,
    prod_head: CachePadded<AtomicUsize>,
    prod_tail: CachePadded<AtomicUsize>,
    buffer: *mut T,
    stride: usize,
    mask: usize,
}
unsafe impl<T> Send for RingInner<T> {}
unsafe impl<T> Sync for RingInner<T> {}

#[derive(Clone)]
pub struct Ring<T> {
    ring: Arc<RingInner<T>>,
}

impl<T> Ring<T> {
    #[cfg(test)]
    fn new_custom_start(capacity: usize, start: usize) -> Self {
        Self {
            ring: Arc::new(RingInner::new_custom_start(capacity, start)),
        }
    }
    pub fn new(capacity: usize) -> Self {
        Self {
            ring: Arc::new(RingInner::new(capacity)),
        }
    }
    pub fn push(&self, value: T) -> bool {
        self.ring.push(value)
    }
    pub fn pop(&self) -> Option<T> {
        self.ring.pop()
    }
}

pub fn buffer_size<T>(n: usize) -> Result<(Layout, usize), std::fmt::Error> {
    let layout = std::alloc::Layout::new::<T>();
    let padding = (layout.align() - (layout.size() % layout.align())) % layout.align();
    let padded_size = layout.size().checked_add(padding).ok_or(std::fmt::Error)?;
    let alloc_size = padded_size.checked_mul(n).ok_or(std::fmt::Error)?;

    unsafe {
        // self.align is already known to be valid and alloc_size has been
        // padded already.
        Ok((
            Layout::from_size_align_unchecked(alloc_size, layout.align()),
            padded_size,
        ))
    }
}

impl<T> RingInner<T> {
    #[cfg(test)]
    fn new_custom_start(capacity: usize, start: usize) -> Self {
        let ring = Self::new(capacity);
        ring.cons_head.store(start, Ordering::Relaxed);
        ring.cons_tail.store(start, Ordering::Relaxed);
        ring.prod_head.store(start, Ordering::Relaxed);
        ring.prod_tail.store(start, Ordering::Relaxed);
        ring
    }

    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.checked_next_power_of_two().expect("size overflow");
        if capacity > std::usize::MAX / 2 - 1 {
            // cons and prod must be
            panic!("buffer too big");
        }
        if capacity == 1 {
            panic!("size must be >= 2");
        }
        let mask = capacity - 1;
        let (layout, padded_size) = buffer_size::<T>(capacity).expect("buffer too big");
        let buffer = unsafe { std::alloc::alloc(layout) as *mut T };
        Self {
            cons_head: CachePadded::new(AtomicUsize::new(0)),
            cons_tail: CachePadded::new(AtomicUsize::new(0)),
            prod_head: CachePadded::new(AtomicUsize::new(0)),
            prod_tail: CachePadded::new(AtomicUsize::new(0)),
            buffer: buffer,
            stride: padded_size,
            mask,
        }
    }

    pub fn push(&self, value: T) -> bool {
        let mut p_head = self.prod_head.load(Ordering::Relaxed);
        let c_tail = self.cons_tail.load(Ordering::Relaxed);
        let prod_next = loop {
            let num_free_entries = self.mask.wrapping_add(c_tail).wrapping_sub(p_head);
            if num_free_entries == 0 {
                return false;
            }
            let prod_next = p_head.wrapping_add(1);
            match self.prod_head.compare_exchange_weak(
                p_head,
                prod_next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // update successful
                    break prod_next;
                }
                Err(x) => {
                    p_head = x;
                }
            }
        };
        let write_idx = p_head & self.mask;
        unsafe {
            *((self.buffer as *mut u8).offset((write_idx * self.stride) as isize) as *mut T) =
                value;
        }
        loop {
            match self.prod_tail.compare_exchange_weak(
                p_head,
                prod_next,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // update successful
                    break;
                }
                Err(_) => {
                    continue;
                }
            }
        }
        true
    }

    pub fn pop(&self) -> Option<T> {
        
    }

    pub fn pop_up_to(&self, max: usize, &mut Vec<T>) -> usize {

        let mut c_head = self.cons_head.load(Ordering::Relaxed);

        let cons_next = loop {
            let p_tail = self.prod_tail.load(Ordering::Relaxed);
            if c_head == p_tail {
                // no elements available
                return None;
            }
            let cons_next = c_head.wrapping_add(1);
            match self.cons_head.compare_exchange_weak(
                c_head,
                cons_next,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // update successful
                    break cons_next;
                }
                Err(x) => {
                    c_head = x;
                }
            }
        };
        let read_idx = c_head & self.mask;
        let value = unsafe {
            let buf_ptr =
                ((self.buffer as *mut u8).offset((read_idx * self.stride) as isize) as *mut T);
            std::ptr::read(buf_ptr)
        };
        loop {
            match self.cons_tail.compare_exchange_weak(
                c_head,
                cons_next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // update successful
                    break;
                }
                Err(_) => {
                    continue;
                }
            }
        }

        Some(value)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_simple() {
        let buffer = Ring::new(128);
        assert_eq!(buffer.push(5u32), true);
        assert_eq!(buffer.pop(), Some(5u32));
        assert_eq!(buffer.pop(), None);
    }

    #[test]
    fn test_empty_fail() {
        let buffer = Ring::<u32>::new(128);
        assert_eq!(buffer.pop(), None);
    }

    #[test]
    fn test_size_three() {
        let buffer = Ring::new(3);
        assert_eq!(buffer.push(5u32), true);
        assert_eq!(buffer.push(3u32), true);
        assert_eq!(buffer.push(2u32), true);
        assert_eq!(buffer.pop(), Some(5u32));
        assert_eq!(buffer.pop(), Some(3u32));
        assert_eq!(buffer.pop(), Some(2u32));
        assert_eq!(buffer.pop(), None);
    }

    #[test]
    fn test_size_overflow() {
        // Create a new ring with 4 slots, starting at std::usize::MAX - 4.
        let buffer = Ring::new_custom_start(4, std::usize::MAX - 4);
        // Push/pop elements until we overflow and make sure things still work
        assert_eq!(buffer.push(5u32), true);
        assert_eq!(buffer.push(3u32), true);
        assert_eq!(buffer.push(2u32), true);
        assert_eq!(buffer.pop(), Some(5u32));
        assert_eq!(buffer.pop(), Some(3u32));
        assert_eq!(buffer.pop(), Some(2u32));
        assert_eq!(buffer.push(5u32), true);
        assert_eq!(buffer.push(3u32), true);
        assert_eq!(buffer.push(2u32), true);
        assert_eq!(buffer.pop(), Some(5u32));
        assert_eq!(buffer.pop(), Some(3u32));
        assert_eq!(buffer.pop(), Some(2u32));
        assert_eq!(buffer.pop(), None);
    }

    #[test]
    fn test_spsc() {
        let NUM_ELEMENTS = 1_000_000_000;
        let buffer = Ring::<u32>::new(1_000_000);
        let consumer_buf = buffer.clone();
        let consumer = std::thread::spawn(move || {
            let mut num_received = 0;
            while num_received != NUM_ELEMENTS {
                if let Some(_) = consumer_buf.pop() {
                    num_received += 1;
                }
            }
            num_received
        });
        let producer_buf = buffer.clone();
        let producer = std::thread::spawn(move || {
            let mut num_sent = 0;
            for i in 0..NUM_ELEMENTS {
                loop {
                    if producer_buf.push(i) {
                        break;
                    }
                }
                num_sent += 1;
            }
            num_sent
        });
        let producer_result = producer.join();
        assert!(producer_result.is_ok());
        assert_eq!(producer_result.unwrap(), NUM_ELEMENTS);
        let consumer_result = consumer.join();
        assert!(consumer_result.is_ok());
        assert_eq!(consumer_result.unwrap(), NUM_ELEMENTS);
    }
}
