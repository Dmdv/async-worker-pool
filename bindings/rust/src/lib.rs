//! # awp-rs: Ultra-Low-Latency Industrial Async Worker Pool (Rust Bindings)
//!
//! Provides safe Rust ergonomics over the high-performance C `libawp` engine with:
//! - Lock-free multi-shard message dispatch
//! - Zero-Copy Claim & Commit API
//! - Thread-safe RAII lifecycle management

pub mod sys;

use std::ffi::CString;
use std::os::raw::{c_int, c_void};
use std::ptr;

pub use sys::AwpRingMode;

/// Frame received by a worker thread in process callback.
pub struct FrameView<'a> {
    raw: &'a sys::AwpFrame,
}

impl<'a> FrameView<'a> {
    pub fn payload(&self) -> &[u8] {
        &self.raw.payload[..self.raw.payload_len]
    }

    pub fn seq(&self) -> u64 {
        self.raw.seq
    }

    pub fn shard(&self) -> u32 {
        self.raw.shard
    }

    pub fn submit_ns(&self) -> u64 {
        self.raw.submit_ns
    }
}

/// Token representing a claimed slot for Zero-Copy in-place writing.
pub struct ClaimGuard<'a> {
    pool: &'a AsyncWorkerPool,
    claim: sys::AwpClaim,
    committed: bool,
}

impl<'a> ClaimGuard<'a> {
    /// Direct mutable access to payload buffer for zero-copy writes.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        unsafe {
            let f = &mut *self.claim.frame;
            &mut f.payload[..]
        }
    }

    /// Set payload length before committing.
    pub fn set_payload_len(&mut self, len: usize) {
        unsafe {
            let f = &mut *self.claim.frame;
            f.payload_len = len.min(sys::AWP_PAYLOAD_MAX);
        }
    }

    /// Commit the frame to the worker queue.
    pub fn commit(mut self) -> Result<(), i32> {
        let rc = unsafe { sys::awp_commit_frame(self.pool.handle, &self.claim) };
        if rc == 0 {
            self.committed = true;
            Ok(())
        } else {
            Err(rc)
        }
    }
}

impl<'a> Drop for ClaimGuard<'a> {
    fn drop(&mut self) {
        if !self.committed {
            // Auto-commit or discard
            unsafe {
                let _ = sys::awp_commit_frame(self.pool.handle, &self.claim);
            }
        }
    }
}

type CallbackBox = Box<dyn Fn(FrameView) -> i32 + Send + Sync + 'static>;

/// Safe RAII wrapper for the Async Worker Pool.
pub struct AsyncWorkerPool {
    handle: *mut sys::AwpPool,
    _context: Box<CallbackBox>,
}

unsafe impl Send for AsyncWorkerPool {}
unsafe impl Sync for AsyncWorkerPool {}

unsafe extern "C" fn rust_process_trampoline(
    frame: *const sys::AwpFrame,
    user: *mut c_void,
) -> c_int {
    let cb_ptr = user as *const CallbackBox;
    let cb = &*cb_ptr;
    let view = FrameView { raw: &*frame };
    cb(view) as c_int
}

impl AsyncWorkerPool {
    /// Create a new worker pool with specified configuration and callback.
    pub fn new<F>(
        workers: u32,
        queue_capacity: u32,
        mode: AwpRingMode,
        callback: F,
    ) -> Result<Self, i32>
    where
        F: Fn(FrameView) -> i32 + Send + Sync + 'static,
    {
        let cb_box: Box<CallbackBox> = Box::new(Box::new(callback));
        let user_ptr = (&*cb_box as *const CallbackBox) as *mut c_void;

        let mut cfg: sys::AwpConfig = unsafe {
            let mut c = std::mem::zeroed();
            sys::awp_config_init(&mut c);
            c
        };

        cfg.n_workers = workers;
        cfg.queue_capacity = queue_capacity;
        cfg.frame_pool_size = queue_capacity * workers * 2;
        cfg.ring_mode = mode;
        cfg.enable_supervisor = 0;
        cfg.process = Some(rust_process_trampoline);
        cfg.user = user_ptr;

        let mut handle: *mut sys::AwpPool = ptr::null_mut();
        let rc = unsafe { sys::awp_pool_create(&cfg, &mut handle) };
        if rc != 0 || handle.is_null() {
            return Err(rc);
        }

        Ok(Self {
            handle,
            _context: cb_box,
        })
    }

    /// Submit a message by copying payload (Standard API).
    pub fn submit(
        &self,
        feed: &str,
        symbol: &str,
        payload: &[u8],
        flags: u32,
    ) -> Result<(), i32> {
        let c_feed = CString::new(feed).unwrap_or_default();
        let c_symbol = CString::new(symbol).unwrap_or_default();

        let rc = unsafe {
            sys::awp_submit(
                self.handle,
                c_feed.as_ptr(),
                c_symbol.as_ptr(),
                payload.as_ptr() as *const c_void,
                payload.len(),
                flags,
            )
        };

        if rc == 0 {
            Ok(())
        } else {
            Err(rc)
        }
    }

    /// Claim an enqueue slot for Zero-Copy in-place writing.
    pub fn claim(&self, shard: u32) -> Result<ClaimGuard<'_>, i32> {
        let mut claim: sys::AwpClaim = unsafe { std::mem::zeroed() };
        let rc = unsafe { sys::awp_claim_frame(self.handle, shard, &mut claim) };
        if rc == 0 {
            Ok(ClaimGuard {
                pool: self,
                claim,
                committed: false,
            })
        } else {
            Err(rc)
        }
    }

    /// Total dropped frames.
    pub fn drops(&self) -> u64 {
        unsafe { sys::awp_pool_drops(self.handle) }
    }
}

impl Drop for AsyncWorkerPool {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe {
                sys::awp_pool_shutdown(self.handle);
                sys::awp_pool_destroy(self.handle);
            }
            self.handle = ptr::null_mut();
        }
    }
}
