use std::alloc::{GlobalAlloc, Layout};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::task_local;

pub struct TaskLocalAllocator;

static GLOBAL_ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[repr(transparent)]
pub struct TaskLocalBytesAllocated(NonNull<AtomicUsize>);

impl Default for TaskLocalBytesAllocated {
    fn default() -> Self {
        unsafe {
            TaskLocalBytesAllocated(NonNull::new_unchecked(Box::leak(Box::new(
                AtomicUsize::new(0),
            ))))
        }
    }
}

unsafe impl Send for TaskLocalBytesAllocated {}

impl TaskLocalBytesAllocated {
    pub fn val(&self) -> usize {
        unsafe { self.0.as_ref().load(Ordering::Relaxed) }
    }
}

task_local! {
    pub static BYTES_ALLOCATED: TaskLocalBytesAllocated;
}

struct TaskLocalAlloc;

unsafe impl GlobalAlloc for TaskLocalAlloc {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let new_layout =
            Layout::from_size_align_unchecked(layout.size() + usize::BITS as usize, layout.align());
        BYTES_ALLOCATED
            .try_with(|bytes| {
                bytes.0.as_ref().fetch_add(layout.size(), Ordering::Relaxed);
                let ptr = GLOBAL_ALLOC.alloc(new_layout);
                dbg!(bytes.0.as_ptr() as usize, ptr);
                *(ptr as *mut usize) = bytes.0.as_ptr() as usize;
                let ptr = ptr.add(usize::BITS as usize);
                ptr
            })
            .unwrap_or_else(|_| {
                let ptr = GLOBAL_ALLOC.alloc(new_layout);
                dbg!((1, ptr));
                *(ptr as *mut usize) = 0;
                let ptr = ptr.add(usize::BITS as usize);
                ptr
            })
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        let new_layout =
            Layout::from_size_align_unchecked(layout.size() + usize::BITS as usize, layout.align());
        let ptr = ptr.sub(usize::BITS as usize);
        dbg!((2, ptr));
        let bytes = (*(ptr as *const usize)) as *const AtomicUsize;
        if let Some(bytes) = bytes.as_ref() {
            bytes.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        GLOBAL_ALLOC.dealloc(ptr, new_layout)
    }

    unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
        let new_layout =
            Layout::from_size_align_unchecked(layout.size() + usize::BITS as usize, layout.align());
        BYTES_ALLOCATED
            .try_with(|bytes| {
                bytes.0.as_ref().fetch_add(layout.size(), Ordering::Relaxed);
                let ptr = GLOBAL_ALLOC.alloc_zeroed(new_layout);
                *(ptr as *mut usize) = bytes.0.as_ptr() as usize;
                dbg!(bytes.0.as_ptr() as usize, ptr);
                let ptr = ptr.add(usize::BITS as usize);
                ptr
            })
            .unwrap_or_else(|_| {
                let ptr = GLOBAL_ALLOC.alloc_zeroed(new_layout);
                let ptr = ptr.add(usize::BITS as usize);
                ptr
            })
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, new_size: usize) -> *mut u8 {
        let new_layout =
            Layout::from_size_align_unchecked(layout.size() + usize::BITS as usize, layout.align());
        let ptr = ptr.sub(usize::BITS as usize);
        let bytes = (*(ptr as *const usize)) as *const AtomicUsize;
        if let Some(bytes) = bytes.as_ref() {
            bytes.fetch_add(new_size, Ordering::Relaxed);
            bytes.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        let new_size = new_size + usize::BITS as usize;
        GLOBAL_ALLOC.realloc(ptr, new_layout, new_size)
    }
}

#[global_allocator]
static TASK_LOCAL_ALLOC: TaskLocalAlloc = TaskLocalAlloc;
