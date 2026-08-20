//! # awp-rs: Ultra-Low-Latency Industrial Async Worker Pool (Rust Bindings)
//!
//! Provides idiomatic, zero-allocation Rust ergonomics over the high-performance C `libawp` engine with:
//! - Lock-free multi-shard message dispatch
//! - Zero-Copy Claim & Commit API for in-place serialization
//! - Stack-buffered zero-allocation string submissions
//! - Safe RAII lifecycle management and typed error handling
//! - Full `PoolBuilder` configuration

pub mod error;
pub mod sys;

pub use error::AwpError;
pub use sys::{AwpRingMode, AWP_FRAME_BROADCAST, AWP_PAYLOAD_MAX};

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;

#[inline]
fn copy_str_to_fixed_cstr(s: &str, buf: &mut [c_char]) -> Result<(), AwpError> {
    let bytes = s.as_bytes();
    if bytes.len() >= buf.len() {
        return Err(AwpError::TooBig);
    }
    unsafe {
        let u8_buf = &mut *(buf as *mut [c_char] as *mut [u8]);
        u8_buf[..bytes.len()].copy_from_slice(bytes);
        u8_buf[bytes.len()] = 0;
    }
    Ok(())
}

/// Read-only view of a frame delivered to a worker thread callback.
pub struct FrameView<'a> {
    raw: &'a sys::AwpFrame,
}

impl<'a> FrameView<'a> {
    /// Borrow the payload buffer slice.
    #[inline]
    pub fn payload(&self) -> &[u8] {
        &self.raw.payload[..self.raw.payload_len]
    }

    /// Monotonic sequence number.
    #[inline]
    pub fn seq(&self) -> u64 {
        self.raw.seq
    }

    /// Shard / worker index.
    #[inline]
    pub fn shard(&self) -> u32 {
        self.raw.shard
    }

    /// Monotonic submission timestamp in nanoseconds.
    #[inline]
    pub fn submit_ns(&self) -> u64 {
        self.raw.submit_ns
    }

    /// Frame flags (e.g. `AWP_FRAME_BROADCAST`).
    #[inline]
    pub fn flags(&self) -> u32 {
        self.raw.flags
    }

    /// Feed label as a string slice.
    #[inline]
    pub fn feed(&self) -> &str {
        unsafe {
            let cstr = CStr::from_ptr(self.raw.feed.as_ptr());
            cstr.to_str().unwrap_or("")
        }
    }

    /// Feed label as a CStr.
    #[inline]
    pub fn feed_cstr(&self) -> &CStr {
        unsafe { CStr::from_ptr(self.raw.feed.as_ptr()) }
    }

    /// Symbol label as a string slice.
    #[inline]
    pub fn symbol(&self) -> &str {
        unsafe {
            let cstr = CStr::from_ptr(self.raw.symbol.as_ptr());
            cstr.to_str().unwrap_or("")
        }
    }

    /// Symbol label as a CStr.
    #[inline]
    pub fn symbol_cstr(&self) -> &CStr {
        unsafe { CStr::from_ptr(self.raw.symbol.as_ptr()) }
    }

    /// Zero-copy read of a plain-old-data (POD) struct value from payload (handles unaligned memory safely).
    #[inline]
    pub fn payload_as<T: Copy>(&self) -> Option<T> {
        if self.raw.payload_len < std::mem::size_of::<T>() {
            return None;
        }
        unsafe {
            let ptr = self.raw.payload.as_ptr() as *const T;
            Some(ptr::read_unaligned(ptr))
        }
    }
}

/// Token representing a claimed slot in the worker ring for Zero-Copy in-place writing.
pub struct ClaimGuard<'a> {
    pool: &'a AsyncWorkerPool,
    claim: sys::AwpClaim,
    committed: bool,
}

impl<'a> ClaimGuard<'a> {
    /// Direct mutable access to payload buffer for zero-copy writes.
    #[inline]
    pub fn payload_mut(&mut self) -> &mut [u8] {
        unsafe {
            let f = &mut *self.claim.frame;
            &mut f.payload[..]
        }
    }

    /// Set payload length before committing.
    #[inline]
    pub fn set_payload_len(&mut self, len: usize) {
        unsafe {
            let f = &mut *self.claim.frame;
            f.payload_len = len.min(sys::AWP_PAYLOAD_MAX);
        }
    }

    /// Set feed label in-place without heap allocation.
    #[inline]
    pub fn set_feed(&mut self, feed: &str) -> Result<(), AwpError> {
        let f = unsafe { &mut *self.claim.frame };
        copy_str_to_fixed_cstr(feed, &mut f.feed)
    }

    /// Set symbol label in-place without heap allocation.
    #[inline]
    pub fn set_symbol(&mut self, symbol: &str) -> Result<(), AwpError> {
        let f = unsafe { &mut *self.claim.frame };
        copy_str_to_fixed_cstr(symbol, &mut f.symbol)
    }

    /// Set custom frame flags.
    #[inline]
    pub fn set_flags(&mut self, flags: u32) {
        unsafe {
            let f = &mut *self.claim.frame;
            f.flags = flags;
        }
    }

    /// Direct zero-copy serialization of POD types into the payload buffer.
    #[inline]
    pub fn write_struct<T: Copy>(&mut self, value: &T) -> Result<(), AwpError> {
        let size = std::mem::size_of::<T>();
        if size > sys::AWP_PAYLOAD_MAX {
            return Err(AwpError::TooBig);
        }
        unsafe {
            let f = &mut *self.claim.frame;
            let dest_ptr = f.payload.as_mut_ptr() as *mut T;
            ptr::write_unaligned(dest_ptr, *value);
            f.payload_len = size;
        }
        Ok(())
    }

    /// Commit the frame to the worker queue.
    #[inline]
    pub fn commit(mut self) -> Result<(), AwpError> {
        let rc = unsafe { sys::awp_commit_frame(self.pool.handle, &self.claim) };
        if rc == 0 {
            self.committed = true;
            Ok(())
        } else {
            Err(AwpError::from(rc))
        }
    }

    /// Explicitly abort/discard the claim without committing to the worker.
    #[inline]
    pub fn abort(mut self) {
        self.committed = true;
    }
}

impl<'a> Drop for ClaimGuard<'a> {
    fn drop(&mut self) {
        // Safe: if dropped without explicit commit, do not commit half-written frames.
    }
}

type CallbackBox = Box<dyn Fn(FrameView) -> i32 + Send + Sync + 'static>;
type ErrorCallbackBox = Box<dyn Fn(FrameView, i32) + Send + Sync + 'static>;

struct CallbackContext {
    process: CallbackBox,
    on_error: Option<ErrorCallbackBox>,
}

/// Fluent builder for constructing an [`AsyncWorkerPool`] with custom microarchitectural parameters.
pub struct PoolBuilder {
    workers: u32,
    queue_capacity: u32,
    frame_pool_size: u32,
    ring_mode: AwpRingMode,
    enable_supervisor: bool,
    enable_restart: bool,
    shutdown_deadline_ms: u32,
    supervisor_interval_ms: u32,
    stall_threshold_ms: u32,
    n_broadcast_workers: u32,
    broadcast_feeds: Vec<CString>,
}

impl Default for PoolBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PoolBuilder {
    /// Create a new builder with production defaults.
    pub fn new() -> Self {
        Self {
            workers: 32,
            queue_capacity: 256,
            frame_pool_size: 4096,
            ring_mode: AwpRingMode::Mpsc,
            enable_supervisor: false,
            enable_restart: false,
            shutdown_deadline_ms: 10_000,
            supervisor_interval_ms: 500,
            stall_threshold_ms: 5_000,
            n_broadcast_workers: 0,
            broadcast_feeds: Vec::new(),
        }
    }

    /// Set number of worker threads.
    pub fn workers(mut self, workers: u32) -> Self {
        self.workers = workers;
        self
    }

    /// Set queue capacity per worker (must be power of two).
    pub fn queue_capacity(mut self, capacity: u32) -> Self {
        self.queue_capacity = capacity;
        self
    }

    /// Set total frame pool size.
    pub fn frame_pool_size(mut self, size: u32) -> Self {
        self.frame_pool_size = size;
        self
    }

    /// Set atomic ring concurrency mode (`Spsc`, `Mpsc`, `Spmc`, `Mpmc`).
    pub fn ring_mode(mut self, mode: AwpRingMode) -> Self {
        self.ring_mode = mode;
        self
    }

    /// Enable or disable supervisor thread for heartbeat tracking.
    pub fn supervisor(mut self, enable: bool) -> Self {
        self.enable_supervisor = enable;
        self
    }

    /// Enable worker automatic restarts on stall/crash.
    pub fn restart(mut self, enable: bool) -> Self {
        self.enable_restart = enable;
        self
    }

    /// Set maximum absolute shutdown wait deadline in milliseconds.
    pub fn shutdown_deadline_ms(mut self, deadline_ms: u32) -> Self {
        self.shutdown_deadline_ms = deadline_ms;
        self
    }

    /// Add a dedicated broadcast feed.
    pub fn add_broadcast_feed(mut self, feed: &str) -> Self {
        if let Ok(c) = CString::new(feed) {
            self.broadcast_feeds.push(c);
        }
        self
    }

    /// Build the pool with a processing callback.
    pub fn build<F>(self, callback: F) -> Result<AsyncWorkerPool, AwpError>
    where
        F: Fn(FrameView) -> i32 + Send + Sync + 'static,
    {
        self.build_with_error_handler(callback, None::<fn(FrameView, i32)>)
    }

    /// Build the pool with both process and error callbacks.
    pub fn build_with_error_handler<F, E>(
        self,
        callback: F,
        on_error: Option<E>,
    ) -> Result<AsyncWorkerPool, AwpError>
    where
        F: Fn(FrameView) -> i32 + Send + Sync + 'static,
        E: Fn(FrameView, i32) + Send + Sync + 'static,
    {
        let err_box: Option<ErrorCallbackBox> = on_error.map(|e| Box::new(e) as ErrorCallbackBox);

        let ctx = Box::new(CallbackContext {
            process: Box::new(callback),
            on_error: err_box,
        });
        let user_ptr = (&*ctx as *const CallbackContext) as *mut c_void;

        let mut cfg: sys::AwpConfig = unsafe {
            let mut c = std::mem::zeroed();
            sys::awp_config_init(&mut c);
            c
        };

        cfg.n_workers = self.workers;
        cfg.queue_capacity = self.queue_capacity;
        cfg.frame_pool_size = if self.frame_pool_size > 0 {
            self.frame_pool_size
        } else {
            self.workers * self.queue_capacity * 2
        };
        cfg.ring_mode = self.ring_mode;
        cfg.enable_supervisor = if self.enable_supervisor { 1 } else { 0 };
        cfg.enable_restart = if self.enable_restart { 1 } else { 0 };
        cfg.shutdown_deadline_ms = self.shutdown_deadline_ms;
        cfg.supervisor_interval_ms = self.supervisor_interval_ms;
        cfg.stall_threshold_ms = self.stall_threshold_ms;
        cfg.n_broadcast_workers = self.n_broadcast_workers;

        // Keep raw pointers alive
        let raw_feed_ptrs: Vec<*const c_char> = self
            .broadcast_feeds
            .iter()
            .map(|s| s.as_ptr())
            .chain(std::iter::once(ptr::null()))
            .collect();

        if !self.broadcast_feeds.is_empty() {
            cfg.broadcast_feeds = raw_feed_ptrs.as_ptr() as *mut *const c_char;
        }

        cfg.process = Some(rust_process_trampoline);
        if ctx.on_error.is_some() {
            cfg.on_error = Some(rust_error_trampoline);
        }
        cfg.user = user_ptr;

        let mut handle: *mut sys::AwpPool = ptr::null_mut();
        let rc = unsafe { sys::awp_pool_create(&cfg, &mut handle) };
        if rc != 0 || handle.is_null() {
            return Err(AwpError::from(rc));
        }

        Ok(AsyncWorkerPool {
            handle,
            _context: ctx,
        })
    }
}

/// Safe RAII wrapper for the Async Worker Pool.
pub struct AsyncWorkerPool {
    handle: *mut sys::AwpPool,
    _context: Box<CallbackContext>,
}

unsafe impl Send for AsyncWorkerPool {}
unsafe impl Sync for AsyncWorkerPool {}

unsafe extern "C" fn rust_process_trampoline(
    frame: *const sys::AwpFrame,
    user: *mut c_void,
) -> c_int {
    let ctx_ptr = user as *const CallbackContext;
    let ctx = &*ctx_ptr;
    let view = FrameView { raw: &*frame };
    (ctx.process)(view) as c_int
}

unsafe extern "C" fn rust_error_trampoline(
    frame: *const sys::AwpFrame,
    err: c_int,
    user: *mut c_void,
) {
    let ctx_ptr = user as *const CallbackContext;
    let ctx = &*ctx_ptr;
    if let Some(ref err_handler) = ctx.on_error {
        let view = FrameView { raw: &*frame };
        err_handler(view, err);
    }
}

impl AsyncWorkerPool {
    /// Create a new worker pool with basic parameters using default settings.
    pub fn new<F>(
        workers: u32,
        queue_capacity: u32,
        mode: AwpRingMode,
        callback: F,
    ) -> Result<Self, AwpError>
    where
        F: Fn(FrameView) -> i32 + Send + Sync + 'static,
    {
        PoolBuilder::new()
            .workers(workers)
            .queue_capacity(queue_capacity)
            .ring_mode(mode)
            .build(callback)
    }

    /// Submit a message by copying payload (Zero-Allocation stack string parsing).
    pub fn submit(
        &self,
        feed: &str,
        symbol: &str,
        payload: &[u8],
        flags: u32,
    ) -> Result<(), AwpError> {
        let mut feed_buf = [0 as c_char; sys::AWP_FEED_MAX + 1];
        let mut sym_buf = [0 as c_char; sys::AWP_SYMBOL_MAX + 1];

        copy_str_to_fixed_cstr(feed, &mut feed_buf)?;
        copy_str_to_fixed_cstr(symbol, &mut sym_buf)?;

        let rc = unsafe {
            sys::awp_submit(
                self.handle,
                feed_buf.as_ptr(),
                sym_buf.as_ptr(),
                payload.as_ptr() as *const c_void,
                payload.len(),
                flags,
            )
        };

        if rc == 0 {
            Ok(())
        } else {
            Err(AwpError::from(rc))
        }
    }

    /// Submit a message with zero-overhead pre-formatted `&CStr` parameters.
    pub fn submit_cstr(
        &self,
        feed: &CStr,
        symbol: &CStr,
        payload: &[u8],
        flags: u32,
    ) -> Result<(), AwpError> {
        let rc = unsafe {
            sys::awp_submit(
                self.handle,
                feed.as_ptr(),
                symbol.as_ptr(),
                payload.as_ptr() as *const c_void,
                payload.len(),
                flags,
            )
        };

        if rc == 0 {
            Ok(())
        } else {
            Err(AwpError::from(rc))
        }
    }

    /// Fast-path submission using precomputed 64-bit hash key.
    pub fn submit_keyed(
        &self,
        hash_key: u64,
        feed: &str,
        symbol: &str,
        payload: &[u8],
        flags: u32,
    ) -> Result<(), AwpError> {
        let mut feed_buf = [0 as c_char; sys::AWP_FEED_MAX + 1];
        let mut sym_buf = [0 as c_char; sys::AWP_SYMBOL_MAX + 1];

        copy_str_to_fixed_cstr(feed, &mut feed_buf)?;
        copy_str_to_fixed_cstr(symbol, &mut sym_buf)?;

        let rc = unsafe {
            sys::awp_submit_keyed(
                self.handle,
                hash_key,
                feed_buf.as_ptr(),
                sym_buf.as_ptr(),
                payload.as_ptr() as *const c_void,
                payload.len(),
                flags,
            )
        };

        if rc == 0 {
            Ok(())
        } else {
            Err(AwpError::from(rc))
        }
    }

    /// Claim an enqueue slot for Zero-Copy in-place writing directly in the ring slab.
    pub fn claim(&self, shard: u32) -> Result<ClaimGuard<'_>, AwpError> {
        let mut claim: sys::AwpClaim = unsafe { std::mem::zeroed() };
        let rc = unsafe { sys::awp_claim_frame(self.handle, shard, &mut claim) };
        if rc == 0 {
            Ok(ClaimGuard {
                pool: self,
                claim,
                committed: false,
            })
        } else {
            Err(AwpError::from(rc))
        }
    }

    /// Determine shard index for a given feed and symbol.
    pub fn shard_of(&self, feed: &str, symbol: &str, flags: u32) -> Result<u32, AwpError> {
        let mut feed_buf = [0 as c_char; sys::AWP_FEED_MAX + 1];
        let mut sym_buf = [0 as c_char; sys::AWP_SYMBOL_MAX + 1];

        copy_str_to_fixed_cstr(feed, &mut feed_buf)?;
        copy_str_to_fixed_cstr(symbol, &mut sym_buf)?;

        let shard = unsafe {
            sys::awp_shard_of(self.handle, feed_buf.as_ptr(), sym_buf.as_ptr(), flags)
        };
        Ok(shard)
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
