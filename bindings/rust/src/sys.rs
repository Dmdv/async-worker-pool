//! Low-level unsafe C FFI definitions for libawp.

use std::os::raw::{c_char, c_int, c_void};

pub const AWP_FEED_MAX: usize = 64;
pub const AWP_SYMBOL_MAX: usize = 64;
pub const AWP_PAYLOAD_MAX: usize = 4096;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AwpRingMode {
    Spsc = 0,
    Mpsc = 1,
    Spmc = 2,
    Mpmc = 3,
}

#[repr(C)]
pub struct AwpFrame {
    pub feed: [c_char; AWP_FEED_MAX + 1],
    pub symbol: [c_char; AWP_SYMBOL_MAX + 1],
    pub payload: [u8; AWP_PAYLOAD_MAX],
    pub payload_len: usize,
    pub seq: u64,
    pub submit_ns: u64,
    pub shard: u32,
    pub flags: u32,
}

pub type AwpProcessFn = unsafe extern "C" fn(frame: *const AwpFrame, user: *mut c_void) -> c_int;
pub type AwpOnErrorFn = unsafe extern "C" fn(frame: *const AwpFrame, err: c_int, user: *mut c_void);

#[repr(C)]
pub struct AwpConfig {
    pub n_workers: u32,
    pub queue_capacity: u32,
    pub frame_pool_size: u32,
    pub ring_mode: AwpRingMode,
    pub enable_supervisor: c_int,
    pub max_restarts: u32,
    pub restart_window_ms: u32,
    pub worker_deadlock_ms: u32,
    pub shutdown_deadline_ms: u32,
    pub process: Option<AwpProcessFn>,
    pub on_error: Option<AwpOnErrorFn>,
    pub user: *mut c_void,
    pub broadcast_feeds: *mut *const c_char,
    pub n_broadcast_feeds: u32,
}

#[repr(C)]
pub struct AwpClaim {
    pub frame: *mut AwpFrame,
    pub shard: u32,
    pub pos: usize,
    pub reserved: *mut c_void,
}

#[repr(C)]
pub struct AwpPool {
    _opaque: [u8; 0],
}

extern "C" {
    pub fn awp_config_init(cfg: *mut AwpConfig);
    pub fn awp_pool_create(cfg: *const AwpConfig, out: *mut *mut AwpPool) -> c_int;
    pub fn awp_submit(
        pool: *mut AwpPool,
        feed: *const c_char,
        symbol: *const c_char,
        payload: *const c_void,
        payload_len: usize,
        flags: u32,
    ) -> c_int;
    pub fn awp_submit_keyed(
        pool: *mut AwpPool,
        hash_key: u64,
        feed: *const c_char,
        symbol: *const c_char,
        payload: *const c_void,
        payload_len: usize,
        flags: u32,
    ) -> c_int;
    pub fn awp_claim_frame(
        pool: *mut AwpPool,
        shard: u32,
        out_claim: *mut AwpClaim,
    ) -> c_int;
    pub fn awp_commit_frame(
        pool: *mut AwpPool,
        claim: *const AwpClaim,
    ) -> c_int;
    pub fn awp_shard_of(
        pool: *const AwpPool,
        feed: *const c_char,
        symbol: *const c_char,
        flags: u32,
    ) -> u32;
    pub fn awp_pool_drops(pool: *const AwpPool) -> u64;
    pub fn awp_pool_shutdown(pool: *mut AwpPool) -> c_int;
    pub fn awp_pool_destroy(pool: *mut AwpPool);
}
