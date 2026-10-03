//! A token bucket over payload bytes, with the clock injected.
//!
//! Replication competes with guests for the local disks and with everything else for the link,
//! so it is limited at the sender (D-31). The clock is a trait so the limit can be tested
//! without sleeping: a test advances a fake one and asserts how long sending *would* take.

use std::time::Duration;

pub trait Clock {
    /// Time since an arbitrary fixed point.
    fn now(&self) -> Duration;
    fn sleep(&self, d: Duration);
}

pub struct SystemClock {
    start: std::time::Instant,
}

impl SystemClock {
    pub fn new() -> SystemClock {
        SystemClock { start: std::time::Instant::now() }
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Duration {
        self.start.elapsed()
    }
    fn sleep(&self, d: Duration) {
        std::thread::sleep(d);
    }
}

pub struct TokenBucket {
    /// Bytes per second; zero means unlimited.
    rate: u64,
    burst: f64,
    tokens: f64,
    last: Option<Duration>,
}

impl TokenBucket {
    pub fn new(rate_bytes_per_sec: u64, burst_bytes: u64) -> TokenBucket {
        TokenBucket { rate: rate_bytes_per_sec, burst: burst_bytes as f64,
                      tokens: burst_bytes as f64, last: None }
    }

    pub fn unlimited() -> TokenBucket {
        TokenBucket::new(0, 0)
    }

    /// Account for `n` bytes about to be sent, sleeping if the bucket is in debt.
    ///
    /// A request larger than the burst is allowed and paid for afterwards, as a sleep, so a
    /// chunk bigger than the bucket cannot deadlock it.
    pub fn take(&mut self, clock: &dyn Clock, n: u64) {
        if self.rate == 0 {
            return;
        }
        let now = clock.now();
        if let Some(last) = self.last {
            let elapsed = now.saturating_sub(last).as_secs_f64();
            self.tokens = (self.tokens + elapsed * self.rate as f64).min(self.burst);
        }
        self.last = Some(now);
        self.tokens -= n as f64;
        if self.tokens < 0.0 {
            let wait = Duration::from_secs_f64(-self.tokens / self.rate as f64);
            clock.sleep(wait);
            self.tokens = 0.0;
            self.last = Some(clock.now());
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::cell::Cell;

    /// A clock that only moves when something sleeps or a test moves it.
    pub struct FakeClock {
        pub now: Cell<Duration>,
        pub slept: Cell<Duration>,
    }

    impl FakeClock {
        pub fn new() -> FakeClock {
            FakeClock { now: Cell::new(Duration::ZERO), slept: Cell::new(Duration::ZERO) }
        }
    }

    impl Clock for FakeClock {
        fn now(&self) -> Duration {
            self.now.get()
        }
        fn sleep(&self, d: Duration) {
            self.now.set(self.now.get() + d);
            self.slept.set(self.slept.get() + d);
        }
    }

    #[test]
    fn sending_at_a_limit_takes_at_least_as_long_as_the_limit_says() {
        let clock = FakeClock::new();
        let mut b = TokenBucket::new(1000, 100);
        for _ in 0..10 {
            b.take(&clock, 1000);
        }
        // 10_000 bytes at 1000 B/s, less the burst allowance of 100 at the start.
        assert!(clock.slept.get() >= Duration::from_secs_f64(9.9), "{:?}", clock.slept.get());
        assert!(clock.slept.get() <= Duration::from_secs_f64(10.0), "{:?}", clock.slept.get());
    }

    #[test]
    fn an_idle_sender_accumulates_no_more_than_the_burst() {
        let clock = FakeClock::new();
        let mut b = TokenBucket::new(1000, 100);
        b.take(&clock, 100);
        clock.now.set(clock.now.get() + Duration::from_secs(3600));
        b.take(&clock, 100);
        assert_eq!(clock.slept.get(), Duration::ZERO);
        b.take(&clock, 100);
        // The hour of idleness bought one burst and no more.
        assert!(clock.slept.get() >= Duration::from_millis(99));
    }

    #[test]
    fn zero_means_unlimited_and_never_sleeps() {
        let clock = FakeClock::new();
        let mut b = TokenBucket::unlimited();
        for _ in 0..1000 {
            b.take(&clock, 1 << 30);
        }
        assert_eq!(clock.slept.get(), Duration::ZERO);
    }

    #[test]
    fn a_request_larger_than_the_burst_is_paid_for_not_refused() {
        let clock = FakeClock::new();
        let mut b = TokenBucket::new(1000, 10);
        b.take(&clock, 5000);
        assert!(clock.slept.get() >= Duration::from_secs_f64(4.9));
    }
}
