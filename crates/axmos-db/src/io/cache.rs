use crate::{io::frames::MemFrame, types::PageId};

use indexmap::IndexMap;
use std::{
    clone::Clone,
    io::{self, Error as IoError, ErrorKind},
};

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

#[derive(Debug)]
pub(crate) struct PageCache {
    capacity: usize,
    frames: IndexMap<PageId, MemFrame>,
    cursor: usize,
    stats: MemoryStats, // Tracking stats
}

impl PageCache {
    pub fn new() -> Self {
        PageCache {
            capacity: 0,
            frames: IndexMap::new(),
            cursor: 0,
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
            frames: IndexMap::with_capacity(capacity),
            cursor: 0,
            stats: MemoryStats::default(),
        }
    }

    pub fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity;

        if self.frames.capacity() < capacity {
            self.frames.reserve(capacity - self.frames.capacity());
        } else {
            self.frames.shrink_to(capacity);
        }
    }

    /// Add a list of frames to the buffer pool.
    pub fn insert(&mut self, id: PageId, frame: MemFrame) -> io::Result<Option<MemFrame>> {
        // If the frame already exists, update it
        if self.frames.contains_key(&id) {
            if let Some(old_frame) = self.frames.get_mut(&id) {
                self.stats.frames_written += 1;
                self.stats.cache_hits += 1;
                *old_frame = frame
            }
            return Ok(None);
        }

        let evicted = if self.capacity <= self.frames.len() {
            self.evict()?
        } else {
            None
        };

        self.frames.insert(id, frame);
        Ok(evicted)
    }

    pub fn evict(&mut self) -> io::Result<Option<MemFrame>> {
        if self.frames.is_empty() {
            return Ok(None);
        };

        let mut found_victim = None;
        // Attempt to iterate over all the frames.
        while self.cursor <= self.frames.len() && found_victim.is_none() {
            if let Some((pid, frame)) = self.frames.get_index(self.cursor) {
                if frame.is_free() {
                    self.stats.frames_evicted += 1;

                    let (_, victim) = self.frames.swap_remove_index(self.cursor).unwrap();

                    found_victim = Some(victim);
                }
            };

            // Not evictable.
            self.cursor += 1;
        }

        if found_victim.is_some() {
            return Ok(found_victim);
        };

        Err(IoError::new(
            ErrorKind::OutOfMemory,
            "Buffer pool got out of memory for new frames.",
        ))
    }

    pub fn get(&mut self, id: &PageId) -> Option<MemFrame> {
        if let Some(frame) = self.frames.get(id) {
            self.stats.cache_hits += 1;
            return Some((*frame).clone());
        }

        self.stats.cache_misses += 1;

        None
    }

    pub fn clear(&mut self) -> Vec<MemFrame> {
        let mut remaining_frames = Vec::with_capacity(self.frames.len());
        for (_, frame) in self.frames.drain(..) {
            // We do not do any checks here because this is like a force-eviction of all pages, useful when a crash happens and we need to write everything to the disk as is.
            // For safe eviction use [evict].
            remaining_frames.push(frame);
        }

        self.cursor = 0;
        self.capacity = 0;
        remaining_frames
    }
}
