use crate::{
    DEFAULT_PAGE_SIZE,
    io::cache::PageCache,
    multithreading::frames::{Frame, MemFrame},
    param_tests,
    storage::{core::traits::Allocatable, page::BtreePage},
    types::PageId,
};

fn insert_page(id: PageId, cache: &mut PageCache, should_evict: bool) -> (PageId, Option<PageId>) {
    let frame = MemFrame::from(Frame::from(BtreePage::alloc(id, DEFAULT_PAGE_SIZE)));
    let id = frame.page_number();
    let result = cache.insert(frame);
    assert!(result.is_ok(), "Cache insertion failed");

    let evicted = if should_evict {
        let optional = result.unwrap();
        assert!(optional.is_some(), "Cache eviction should happen");
        let page = optional.unwrap();
        Some(page.page_number())
    } else {
        assert!(
            result.unwrap().is_none(),
            "Cache eviction should not happen"
        );
        None
    };

    (id, evicted)
}

fn insert_pages_no_evict(cache: &mut PageCache) -> Vec<PageId> {
    let num_pages = cache.get_capacity();
    let mut inserted = Vec::new();
    for i in 0..num_pages {
        let (id, _) = insert_page(i as PageId, cache, false);
        inserted.push(id);
    }
    inserted
}

fn assert_get_all(cache: &mut PageCache, pages: &[PageId]) {
    for id in pages {
        assert!(cache.get(id).is_some(), "Page not found {id}")
    }
}

fn test_cache_insert_get(capacity: usize) {
    let mut cache = PageCache::with_capacity(capacity);
    assert_eq!(cache.num_frames(), 0);
    let inserted: Vec<PageId> = insert_pages_no_evict(&mut cache);
    assert!(
        cache.num_frames() == inserted.len(),
        "Invalid number of frames found on the page cache"
    );
    assert_get_all(&mut cache, &inserted);
}

fn test_cache_eviction(capacity: usize) {
    let mut cache = PageCache::with_capacity(capacity);
    let inserted: Vec<PageId> = insert_pages_no_evict(&mut cache);
    assert!(
        cache.num_frames() == inserted.len(),
        "Invalid number of frames found on the page cache"
    );

    let next_id = inserted.len();
    let (id, evicted) = insert_page(next_id as PageId, &mut cache, true);
    assert!(evicted.is_some());
    let id = evicted.unwrap();
    assert!(id == inserted[0], "Should have evicted the first page.");
    assert!(cache.get(&id).is_none(), "Evicted page found {id}!")
}

fn test_cache_clear(capacity: usize) {
    let mut cache = PageCache::with_capacity(capacity);
    assert!(
        cache.num_frames() == 0,
        "Invalid number of frames found on the page cache"
    );
    let inserted: Vec<PageId> = insert_pages_no_evict(&mut cache);
    assert!(
        cache.num_frames() == inserted.len(),
        "Invalid number of frames found on the page cache"
    );
    cache.clear();
    assert!(
        cache.num_frames() == 0,
        "Invalid number of frames found on the page cache"
    );
}

param_tests!(test_cache_insert_get, cap => [1, 2, 5, 10, 50], miri_safe);
param_tests!(test_cache_eviction, cap => [1, 2, 5, 10], miri_safe);
param_tests!(test_cache_clear, cap => [1, 5, 10, 50], miri_safe);
