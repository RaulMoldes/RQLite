use crate::io::frames::MemFrame;
use crate::storage::page::MemPage;
use crate::types::PageId;
use std::clone::Clone;
use std::collections::{HashMap, VecDeque};

#[derive(Debug, Default)]
pub(crate) struct MemoryStats {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub frames_evicted: u64,
    pub frames_written: u64,
}

impl std::fmt::Display for MemoryStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, " Cache hits: {}", self.cache_hits)?;
        writeln!(f, " Cache misses: {}", self.cache_misses)?;
        writeln!(f, " frames evicted: {}", self.frames_evicted)?;
        writeln!(f, " frames written: {}", self.frames_written)?;
        Ok(())
    }
}

pub(crate) struct PageCache {
    capacity: usize,
    frames: HashMap<PageId, MemFrame<MemPage>>,
    free_list: VecDeque<PageId>, // Queue of frames in eviction order
    stats: MemoryStats,          // Tracking stats
}

impl PageCache {
    pub fn new() -> Self {
        PageCache {
            capacity: 0,
            frames: HashMap::new(),
            free_list: VecDeque::new(),
            stats: MemoryStats::default(),
        }
    }

    pub fn stats(&self) -> &MemoryStats {
        &self.stats
    }

    pub fn num_frames(&self) -> usize {
        self.frames.len()
    }
}

impl PageCache {
    pub fn with_capacity(capacity: usize) -> Self {
        PageCache {
            capacity,
            frames: HashMap::with_capacity(capacity),
            free_list: VecDeque::with_capacity(capacity),
            stats: MemoryStats::default(),
        }
    }

    pub fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity;
        self.frames.reserve(self.capacity - self.frames.capacity());
        self.free_list
            .reserve(self.capacity - self.free_list.capacity())
    }

    /// Add a list of frames to the buffer pool.
    pub fn insert(&mut self, id: PageId, frame: MemFrame<MemPage>) -> Option<MemFrame<MemPage>> {
        // If the frame already exists, update it
        if self.frames.contains_key(&id) {
            if let Some(old_frame) = self.frames.get_mut(&id) {
                self.stats.frames_written += 1;
                self.stats.cache_hits += 1;
                *old_frame = frame
            }
            return None;
        }

        let evicted = if self.capacity <= self.frames.len() {
            self.evict(None)
        } else {
            None
        };

        self.free_list.push_back(id);
        self.frames.insert(id, frame);
        evicted
    }

    fn evict(&mut self, frame: Option<PageId>) -> Option<MemFrame<MemPage>> {
        if let Some(frame_id) = frame {
            if let Some(frame_data) = self.frames.remove(&frame_id) {
                self.stats.frames_evicted += 1;
                return Some(frame_data);
            }
        }

        // TODO: I NEED I WAY TO CHECK FOR OOM ERRORS HERE.
        //if self.frames.len() >= self.capacity {
        // Pop from the queue until there is one page that we can evict.
        while let Some(id) = self.free_list.pop_front() {
            if let Some(frame) = self.frames.remove(&id) {
                if frame.is_free() {
                    self.stats.frames_evicted += 1;

                    return Some(frame);
                }
                self.free_list.push_back(id);
            }
        }
        // }
        None
    }

    pub fn get(&mut self, id: &PageId) -> Option<MemFrame<MemPage>> {
        if let Some(frame) = self.frames.get(id) {
            self.stats.cache_hits += 1;
            return Some((*frame).clone());
        }

        self.stats.cache_misses += 1;

        None
    }

    pub fn clear(&mut self) -> Vec<MemFrame<MemPage>> {
        let mut remaining_frames = Vec::with_capacity(self.frames.len());
        for (_, frame) in self.frames.drain() {
            // We do not do any checks here because this is like a force-eviction of all pages, useful when a crash happens and we need to write everything to the disk as is.
            // For safe eviction use [evict].
            remaining_frames.push(frame);
        }

        self.free_list.clear();
        self.capacity = 0;
        remaining_frames
    }
}
