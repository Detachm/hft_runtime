mod discovery;
mod evidence;
mod http;
mod reference_ws;
mod state;
pub mod types;
mod util;
mod ws;

pub use http::{BlockingHttpFetcher, HttpFetchConfig, HttpFetchOutcome, HttpFetcher};
pub use reference_ws::{
    read_reference_ws_state, reference_output_path, run_reference_ws_forever,
    write_reference_ws_state, ReferenceWsFlushPolicy, ReferenceWsRecorderOptions,
    DEFAULT_BINANCE_REFERENCE_WS_URL, DEFAULT_OKX_REFERENCE_WS_URL,
    DEFAULT_REFERENCE_WS_CHANNEL_CAPACITY, DEFAULT_REFERENCE_WS_FLUSH_BYTES,
    DEFAULT_REFERENCE_WS_FLUSH_INTERVAL_MS, DEFAULT_REFERENCE_WS_FLUSH_ROWS,
};
pub use state::validate_config;
pub use types::*;
pub use ws::{
    dynamic_subscription_json, market_subscription_json, read_ws_state, run_ws_forever,
    should_flush_ws_segment, write_ws_state, ws_output_path, ws_rows_from_text_frame,
    WsFlushPolicy, WsRawFormat, WsRecorderOptions, WsRowContext, DEFAULT_WS_CHANNEL_CAPACITY,
    DEFAULT_WS_CONNECT_TIMEOUT_MS, DEFAULT_WS_FLUSH_BYTES, DEFAULT_WS_FLUSH_INTERVAL_MS,
    DEFAULT_WS_FLUSH_ROWS, DEFAULT_WS_PING_INTERVAL_MS, DEFAULT_WS_REDISCOVERY_INTERVAL_MS,
    POLYMARKET_CLOB_WS_MARKET_ENDPOINT,
};
