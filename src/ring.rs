use crate::cache_padded::CachePadded;
use core::alloc::Layout;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct Ring<T> {
    cons_head: CachePadded<AtomicUsize>,
    cons_tail: CachePadded<AtomicUsize>,
    prod_head: CachePadded<AtomicUsize>,
    prod_tail: CachePadded<AtomicUsize>,
    buffer: *mut T,
    mask: usize,
}
impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        let p_tail = self.prod_head.load(Ordering::Acquire);
        let mut c_head = self.cons_tail.load(Ordering::Acquire);
        while c_head != p_tail {
            let idx = c_head & self.mask;
            unsafe {
                std::ptr::drop_in_place(self.buffer.offset(idx as isize));
            }
            c_head = c_head.wrapping_add(1);
        }
        unsafe {
            std::alloc::dealloc(
                self.buffer as *mut u8,
                buffer_size::<T>(self.mask + 1).unwrap().0,
            );
        }
    }
}
unsafe impl<T> Send for Ring<T> {}
unsafe impl<T> Sync for Ring<T> {}

#[cfg(feature = "std")]
pub struct HeapRing<T> {
    ring: Arc<Ring<T>>,
}
impl<T> Clone for HeapRing<T> {
    fn clone(&self) -> Self {
        Self {
            ring: Arc::clone(&self.ring),
        }
    }
}

#[cfg(feature = "std")]
impl<T> HeapRing<T> {
    #[cfg(test)]
    fn new_custom_start(capacity: usize, start: usize) -> Self {
        Self {
            ring: Arc::new(Ring::new_custom_start(capacity, start)),
        }
    }
    pub fn new(capacity: usize) -> Self {
        Self {
            ring: Arc::new(Ring::new(capacity)),
        }
    }
    pub fn push(&self, value: T) -> Option<T> {
        self.ring.push(value)
    }
    /// Popping a single object at a time is not good for performance.
    /// Performance is the whole point of this crate, so let's hide it from the docs.
    #[doc(hidden)]
    pub fn pop_single(&self) -> Option<T> {
        self.ring.pop_single()
    }
    pub unsafe fn pop_up_to(&self, max: usize, out_buf: *mut T) -> usize {
        self.ring.pop_internal(max, 1, out_buf)
    }
    pub unsafe fn pop_exact(&self, max: usize, out_buf: *mut T) -> usize {
        self.ring.pop_internal(max, max, out_buf)
    }
}

fn padding_needed_for(len: usize, align: usize) -> usize {
    let len_rounded_up = len.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1);
    len_rounded_up.wrapping_sub(len)
}

// this is copied from the Layout nightly features
fn buffer_size<T>(n: usize) -> Result<(Layout, usize), std::fmt::Error> {
    let layout = std::alloc::Layout::new::<T>();
    let padded_size = layout
        .size()
        .checked_add(padding_needed_for(layout.size(), layout.align()))
        .ok_or(std::fmt::Error)?;
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

impl<T> Ring<T> {
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
        if capacity > core::usize::MAX / 2 - 1 {
            // cons and prod must be
            panic!("buffer too big");
        }
        if capacity == 1 {
            panic!("size must be >= 2");
        }
        let mask = capacity - 1;
        let (layout, _) = buffer_size::<T>(capacity).expect("buffer too big");
        let buffer = unsafe { std::alloc::alloc(layout) as *mut T };
        Self {
            cons_head: CachePadded::new(AtomicUsize::new(0)),
            cons_tail: CachePadded::new(AtomicUsize::new(0)),
            prod_head: CachePadded::new(AtomicUsize::new(0)),
            prod_tail: CachePadded::new(AtomicUsize::new(0)),
            buffer: buffer,
            mask,
        }
    }

    /// Puts provided value in the ring buffer.
    /// If the buffer has insufficient space, Some(value) is returned.
    pub fn push(&self, value: T) -> Option<T> {
        let mut p_head = self.prod_head.load(Ordering::Relaxed);
        let c_tail = self.cons_tail.load(Ordering::Relaxed);
        let prod_next = loop {
            let num_free_entries = self.mask.wrapping_add(c_tail).wrapping_sub(p_head);
            if num_free_entries == 0 {
                return Some(value);
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
            core::ptr::write(self.buffer.offset(write_idx as isize), value);
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
        None
    }

    pub fn pop_single(&self) -> Option<T> {
        unsafe {
            let mut out_value = core::mem::MaybeUninit::uninit();
            let num_read = self.pop_internal(1, 1, out_value.as_mut_ptr());
            if num_read == 1 {
                Some(out_value.assume_init())
            } else {
                None
            }
        }
    }
    pub unsafe fn pop_up_to(&self, max: usize, out_buf: *mut T) -> usize {
        self.pop_internal(max, 1, out_buf)
    }
    pub unsafe fn pop_exact(&self, max: usize, out_buf: *mut T) -> usize {
        self.pop_internal(max, max, out_buf)
    }

    unsafe fn pop_internal(&self, max: usize, fail_limit: usize, out_buf: *mut T) -> usize {
        let mut c_head = self.cons_head.load(Ordering::Relaxed);

        let (cons_next, to_read) = loop {
            let p_tail = self.prod_tail.load(Ordering::Relaxed);
            let entries_available = p_tail.wrapping_sub(c_head);
            if entries_available < fail_limit {
                // no elements available
                return 0;
            }
            let to_read = core::cmp::min(max, entries_available);
            let cons_next = c_head.wrapping_add(to_read);
            match self.cons_head.compare_exchange_weak(
                c_head,
                cons_next,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // update successful
                    break (cons_next, to_read);
                }
                Err(x) => {
                    c_head = x;
                }
            }
        };
        let read_idx = c_head & self.mask;
        let end_read_idx = cons_next & self.mask;
        let second_seg_size = if end_read_idx < read_idx {
            end_read_idx
        } else {
            0
        };
        let first_seg_size = to_read.saturating_sub(second_seg_size);
        // dbg!(c_head);
        // dbg!(cons_next);
        // dbg!(read_idx);
        // dbg!(to_read);
        // dbg!(second_seg_size);
        // dbg!(first_seg_size);
        let first_seg_ptr = self.buffer.offset(read_idx as isize);
        core::ptr::copy_nonoverlapping(first_seg_ptr, out_buf, first_seg_size);
        core::ptr::copy_nonoverlapping(
            self.buffer,
            out_buf.offset(first_seg_size as isize),
            second_seg_size,
        );
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
        to_read
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_simple() {
        let buffer = Ring::new(128);
        assert_eq!(buffer.push(5u32), None);
        assert_eq!(buffer.pop_single(), Some(5u32));
        assert_eq!(buffer.pop_single(), None);
    }

    #[test]
    fn test_empty_fail() {
        let buffer = Ring::<u32>::new(128);
        assert_eq!(buffer.pop_single(), None);
    }

    #[test]
    fn test_size_three() {
        let buffer = Ring::new(3);
        assert_eq!(buffer.push(5u32), None);
        assert_eq!(buffer.push(3u32), None);
        assert_eq!(buffer.push(2u32), None);
        assert_eq!(buffer.pop_single(), Some(5u32));
        assert_eq!(buffer.pop_single(), Some(3u32));
        assert_eq!(buffer.pop_single(), Some(2u32));
        assert_eq!(buffer.pop_single(), None);
    }

    #[test]
    fn test_size_overflow() {
        // Create a new ring with 4 slots, starting at std::usize::MAX - 4.
        let buffer = Ring::new_custom_start(4, core::usize::MAX - 4);
        // Push/pop elements until we overflow and make sure things still work
        assert_eq!(buffer.push(5u32), None);
        assert_eq!(buffer.push(3u32), None);
        assert_eq!(buffer.push(2u32), None);
        assert_eq!(buffer.pop_single(), Some(5u32));
        assert_eq!(buffer.pop_single(), Some(3u32));
        assert_eq!(buffer.pop_single(), Some(2u32));
        assert_eq!(buffer.push(5u32), None);
        assert_eq!(buffer.push(3u32), None);
        assert_eq!(buffer.push(2u32), None);
        assert_eq!(buffer.pop_single(), Some(5u32));
        assert_eq!(buffer.pop_single(), Some(3u32));
        assert_eq!(buffer.pop_single(), Some(2u32));
        assert_eq!(buffer.pop_single(), None);
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_spsc() {
        let buffer = HeapRing::<u32>::new(4);
        let consumer_buf = buffer.clone();
        let consumer = std::thread::spawn(move || loop {
            if let Some(value) = consumer_buf.pop_single() {
                return value;
            }
        });
        let producer_buf = buffer.clone();
        let producer = std::thread::spawn(move || producer_buf.push(5u32));
        let producer_result = producer.join();
        assert!(producer_result.is_ok());
        let consumer_result = consumer.join();
        assert!(consumer_result.is_ok());
        assert_eq!(consumer_result.unwrap(), 5u32);
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_pop_up_to() {
        // Create a new ring with 4 slots, starting at std::usize::MAX - 4.
        let buffer = Ring::new_custom_start(4, std::usize::MAX - 4);
        let mut values = Vec::with_capacity(4);
        assert_eq!(buffer.push(5u32), None);
        assert_eq!(buffer.push(3u32), None);
        assert_eq!(buffer.push(2u32), None);
        unsafe {
            let values_read = buffer.pop_up_to(3, values.as_mut_ptr());
            values.set_len(values_read);
        }
        assert_eq!(&values, &[5, 3, 2]);
    }

    #[cfg(feature = "std")]
    struct DropTest(u32, HeapRing<u32>);
    #[cfg(feature = "std")]
    impl Drop for DropTest {
        fn drop(&mut self) {
            self.1.push(self.0);
        }
    }

    #[cfg(feature = "std")]
    #[test]
    fn test_drop() {
        // Create a new ring with 4 slots, starting at std::usize::MAX - 4.
        let buffer = HeapRing::new_custom_start(4, std::usize::MAX - 4);
        let recv_buffer = HeapRing::new(4);
        buffer.push(DropTest(5u32, recv_buffer.clone()));
        buffer.push(DropTest(3u32, recv_buffer.clone()));
        buffer.push(DropTest(2u32, recv_buffer.clone()));
        drop(buffer);
        assert_eq!(recv_buffer.pop_single(), Some(5u32));
        assert_eq!(recv_buffer.pop_single(), Some(3u32));
        assert_eq!(recv_buffer.pop_single(), Some(2u32));
        assert_eq!(recv_buffer.pop_single(), None);
    }
}
