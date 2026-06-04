#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TriggerThreshold {
    pub min_edge: f64,
}

impl TriggerThreshold {
    pub fn fires(self, edge: f64) -> bool {
        edge >= self.min_edge
    }
}
