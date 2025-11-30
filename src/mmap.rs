use std::{fs::File, os::fd::AsRawFd, ptr, slice};

use anyhow::{Result, bail};

pub const PROT_READ: i32 = 0x1;
pub const MAP_PRIVATE: i32 = 0x02;
pub const MAP_FAILED: *mut std::ffi::c_void = !0 as *mut std::ffi::c_void;

unsafe extern "C" {
    fn mmap(
        addr: *mut std::ffi::c_void,
        len: usize,
        prot: i32,
        flags: i32,
        fd: i32,
        offset: i64,
    ) -> *mut std::ffi::c_void;
    fn munmap(addr: *mut std::ffi::c_void, len: usize) -> i32;
}

pub struct Mmap {
    ptr: *mut u8,
    len: usize,
}

impl Mmap {
    pub fn new(filename: &str) -> Result<Self> {
        let file = File::open(filename)?;
        let len = file.metadata()?.len() as usize;
        let fd = file.as_raw_fd();

        if len == 0 {
            bail!("file is empty");
        }

        let ptr = unsafe {
            mmap(
                ptr::null_mut(), // Address (null = let OS choose)
                len,
                PROT_READ,
                MAP_PRIVATE,
                fd,
                0,
            )
        };

        if ptr == MAP_FAILED {
            bail!("Mapping failed");
        }

        Ok(Self {
            ptr: ptr as *mut u8,
            len,
        })
    }

    pub fn leak(self) -> &'static [u8] {
        let ptr = self.ptr;
        let len = self.len;

        // 1. Forget self so Drop is NOT called.
        // If we didn't do this, munmap would fire at the end of this function,
        // making the pointer invalid.
        std::mem::forget(self);

        // 2. Reconstruct the slice.
        // Because we manually ensured the memory will never be unmapped
        // (until process exit), we can treat the lifetime as 'static.
        unsafe { slice::from_raw_parts(ptr, len) }
    }
}

unsafe impl Send for Mmap {}

impl Drop for Mmap {
    fn drop(&mut self) {
        unsafe {
            munmap(self.ptr as *mut std::ffi::c_void, self.len);
        }
    }
}

impl std::ops::Deref for Mmap {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { slice::from_raw_parts(self.ptr, self.len) }
    }
}
