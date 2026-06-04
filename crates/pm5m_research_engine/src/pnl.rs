#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RealizedPnl {
    pub cash_delta: f64,
    pub settlement_delta: f64,
    pub fees: f64,
}

impl RealizedPnl {
    pub fn total(self) -> f64 {
        self.cash_delta + self.settlement_delta - self.fees
    }
}
