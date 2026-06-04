#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Edge {
    pub fair_value: f64,
    pub executable_price: f64,
}

impl Edge {
    pub fn buy_edge(self) -> f64 {
        self.fair_value - self.executable_price
    }

    pub fn sell_edge(self) -> f64 {
        self.executable_price - self.fair_value
    }
}
