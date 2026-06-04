use crate::tradeable_interval::TradeableInterval;
use pm5m_data_etl::StandardEventIndexRow;

#[derive(Debug, Clone)]
pub struct ReplayCursor<'a> {
    events: &'a [StandardEventIndexRow],
    interval: TradeableInterval,
}

impl<'a> ReplayCursor<'a> {
    pub fn new(events: &'a [StandardEventIndexRow], interval: TradeableInterval) -> Self {
        Self { events, interval }
    }

    pub fn iter(&self) -> impl Iterator<Item = &'a StandardEventIndexRow> + '_ {
        self.events
            .iter()
            .filter(|event| self.interval.contains(event.event_ts_ns))
    }
}
