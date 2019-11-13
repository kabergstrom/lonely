#![feature(test)]

extern crate test;
use lonely::Ring;
const ELEMENTS: usize = 100_000;
const NUM_PRODUCERS: usize = 10;
const ELEMENTS_PER_PRODUCER: usize = ELEMENTS / NUM_PRODUCERS;

#[bench]
fn ring_spsc(b: &mut test::Bencher) {
    let buffer = Ring::<u32>::new(1_000);
    let producer_buf = producer_thread(buffer.clone());

    b.iter(|| {
        producer_buf.push(ELEMENTS as u32);
        let mut num_received = 0;
        while num_received != ELEMENTS {
            if let Some(_) = buffer.pop() {
                num_received += 1;
            }
        }
        assert_eq!(num_received, ELEMENTS);
    });
    producer_buf.push(0);
}

fn producer_thread(output_buf: Ring<u32>) -> Ring<u32> {
    let producer_buf = Ring::new(4);
    let to_return = producer_buf.clone();
    std::thread::spawn(move || loop {
        let to_produce = loop {
            if let Some(to_produce) = producer_buf.pop() {
                if to_produce == 0 {
                    return;
                } else {
                    break to_produce;
                }
            }
        };
        for i in 0..to_produce {
            loop {
                if output_buf.push(i).is_none() {
                    break;
                }
            }
        }
    });
    to_return.push(1);
    // wait for thread to start
    while let None = to_return.pop() {}
    to_return
}

#[bench]
fn ring_mpsc(b: &mut test::Bencher) {
    let buffer = Ring::<u32>::new(10_000);
    let mut producer_buffers = Vec::new();
    for _ in 0..NUM_PRODUCERS {
        producer_buffers.push(producer_thread(buffer.clone()));
    }

    b.iter(|| {
        for producer in producer_buffers.iter() {
            producer.push(ELEMENTS_PER_PRODUCER as u32);
        }
        let mut num_received = 0;
        while num_received != ELEMENTS {
            if let Some(_) = buffer.pop() {
                num_received += 1;
            }
        }
        assert_eq!(num_received, ELEMENTS);
    });
    for producer in producer_buffers.iter() {
        producer.push(0);
    }
}

#[bench]
fn ring_mpsc_batch_pop(b: &mut test::Bencher) {
    ring_mpsc_batch_pop_bench(b, 10, NUM_PRODUCERS, ELEMENTS);
}
#[bench]
fn ring_mpsc_batch_pop_1(b: &mut test::Bencher) {
    ring_mpsc_batch_pop_bench(b, 100, 1, ELEMENTS);
}
#[bench]
fn ring_mpsc_batch_pop_2(b: &mut test::Bencher) {
    ring_mpsc_batch_pop_bench(b, 100, 2, ELEMENTS * 2);
}
#[bench]
fn ring_mpsc_batch_pop_4(b: &mut test::Bencher) {
    ring_mpsc_batch_pop_bench(b, 100, 4, ELEMENTS * 4);
}
#[bench]
fn ring_mpsc_batch_pop_8(b: &mut test::Bencher) {
    ring_mpsc_batch_pop_bench(b, 100, 8, ELEMENTS * 8);
}
fn ring_mpsc_batch_pop_bench(
    b: &mut test::Bencher,
    batch_multiple: usize,
    num_producers: usize,
    num_elements: usize,
) {
    let elements_per_producer = num_elements / num_producers;
    let buffer = Ring::<u32>::new(10_000);
    let mut producer_buffers = Vec::new();
    for _ in 0..num_producers {
        producer_buffers.push(producer_thread(buffer.clone()));
    }

    b.iter(|| {
        for producer in producer_buffers.iter() {
            producer.push(elements_per_producer as u32);
        }
        let mut num_received = 0;
        let batch_size = NUM_PRODUCERS * batch_multiple;
        let mut value_vec = Vec::with_capacity(batch_size);
        while num_received != elements_per_producer * num_producers {
            num_received += unsafe { buffer.pop_exact(batch_size, value_vec.as_mut_ptr()) };
        }
        assert_eq!(num_received, elements_per_producer * num_producers);
    });
    for producer in producer_buffers.iter() {
        producer.push(0);
    }
}
