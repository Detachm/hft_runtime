#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeableInterval {
    pub start_ts_ns: i64,
    pub end_ts_ns: i64,
}

impl TradeableInterval {
    pub fn contains(self, ts_ns: i64) -> bool {
        self.start_ts_ns <= ts_ns && ts_ns <= self.end_ts_ns
    }
}
