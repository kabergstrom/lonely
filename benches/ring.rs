#![feature(test)]

extern crate test;
use lonely::Ring;

#[bench]
fn spsc(b: &mut test::Bencher) {
    let NUM_ELEMENTS = 1_000_000;
    let buffer = Ring::<u32>::new(1_000);

    b.iter(|| {
        let consumer_buf = buffer.clone();
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
        let mut num_received = 0;
        while num_received != NUM_ELEMENTS {
            if let Some(_) = consumer_buf.pop() {
                num_received += 1;
            }
        }
        assert_eq!(num_received, NUM_ELEMENTS);
    });
}

#[bench]
fn mpsc(b: &mut test::Bencher) {
    let ELEMENTS_PER_PRODUCER = 1_000_000;
    let NUM_PRODUCERS = 2;
    let buffer = Ring::<u32>::new(1_000);

    b.iter(|| {
        let consumer_buf = buffer.clone();
        for _ in 0..NUM_PRODUCERS {
            let producer_buf = buffer.clone();
            std::thread::spawn(move || {
                let mut num_sent = 0;
                for i in 0..ELEMENTS_PER_PRODUCER  {
                    loop {
                        if producer_buf.push(i) {
                            break;
                        }
                    }
                    num_sent += 1;
                }
                num_sent
            });
        }
        let mut num_received = 0;
        while num_received != ELEMENTS_PER_PRODUCER * NUM_PRODUCERS {
            if let Some(_) = consumer_buf.pop() {
                num_received += 1;
            }
        }
        assert_eq!(num_received, ELEMENTS_PER_PRODUCER * NUM_PRODUCERS);
    });
}