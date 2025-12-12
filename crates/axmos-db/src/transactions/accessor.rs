use crate::{
    io::{
        frames::{FrameAccessMode, FrameStack, Position},
        pager::SharedPager,
        wal::OwnedLogRecord,
    },
    storage::{
        buffer::MemBlock,
        latches::Latch,
        page::{MemPage, Page},
    },
    types::PageId,
};

use std::{
    cell::{Ref, RefCell, RefMut},
    io::{self, Error as IoError, ErrorKind as IoErrorKind},
    rc::Rc,
    thread::{self, ThreadId},
};

#[derive(Debug)]
pub(crate) struct PageAccessor {
    /// Thread id.
    thread_id: ThreadId,

    /// Shared access to the pager
    pager: SharedPager,

    /// Private stack for transactions
    stack: FrameStack,
}

#[derive(Debug)]
pub(crate) struct RcPageAccessor(Rc<RefCell<PageAccessor>>);

impl RcPageAccessor {
    pub(crate) fn new(pager: SharedPager) -> Self {
        Self(Rc::new(RefCell::new(PageAccessor::new(pager))))
    }

    pub(crate) fn borrow(&self) -> Ref<'_, PageAccessor> {
        self.0.borrow()
    }

    pub(crate) fn borrow_mut(&self) -> RefMut<'_, PageAccessor> {
        self.0.borrow_mut()
    }
}

impl Clone for RcPageAccessor {
    fn clone(&self) -> Self {
        Self(Rc::clone(&self.0))
    }
}

impl PageAccessor {
    pub(crate) fn new(pager: SharedPager) -> Self {
        Self {
            thread_id: thread::current_id(),
            pager,
            stack: FrameStack::new(),
        }
    }

    pub(crate) fn thread_id(&self) -> ThreadId {
        self.thread_id
    }

    fn stack_mut(&mut self) -> &mut FrameStack {
        &mut self.stack
    }

    fn stack_ref(&self) -> &FrameStack {
        &self.stack
    }

    pub(crate) fn get_page_size(&self) -> usize {
        self.pager.read().page_size() as usize
    }

    pub(crate) fn clear_stack(&mut self) {
        self.stack_mut().clear()
    }

    pub(crate) fn release_latch(&mut self, page_id: PageId) {
        self.stack_mut().release(page_id);
    }

    pub(crate) fn last(&self) -> Option<&Position> {
        self.stack_ref().last()
    }

    pub(crate) fn pop(&mut self) -> Option<Position> {
        self.stack_mut().pop()
    }

    // Track the current position in the stack
    pub(crate) fn visit(&mut self, position: Position) -> std::io::Result<()> {
        self.stack_mut().visit(position);
        Ok(())
    }

    // Acquire a lock on the stack for a page using the given [AccessMode]
    pub(crate) fn acquire<P: Page>(
        &mut self,
        page_id: PageId,
        access_mode: FrameAccessMode,
    ) -> std::io::Result<()>
    where
        MemPage: From<MemBlock<P::Header>>,
    {
        let latch_page = self.pager.write().read_page::<P>(page_id)?;
        self.stack_mut().acquire(page_id, latch_page, access_mode);
        Ok(())
    }

    pub(crate) fn alloc_page<P: Page>(&mut self) -> std::io::Result<PageId>
    where
        MemPage: From<P>,
        MemBlock<P::Header>: Into<MemPage>,
    {
        let page = self.pager.write().alloc_frame()?;
        self.pager.write().cache_frame(page)
    }

    pub(crate) fn dealloc_page<P: Page>(&mut self, id: PageId) -> std::io::Result<()>
    where
        MemPage: From<MemBlock<P::Header>>,
    {
        let cloned = self.pager.write().read_page::<P>(id)?.deep_copy();
        debug_assert!(
            cloned.page_number() == id,
            "MEMORY CORRUPTION. ID IN CACHE DOES NOT MATCH PHYSICAL ID"
        );
        self.pager.write().dealloc_page::<P>(id)?;

        Ok(())
    }

    // Allows to execute a mutating callback on the requested page.
    // The page latch must have been already acquired.
    pub(crate) fn write_page<P, F, R>(&mut self, id: PageId, callback: F) -> io::Result<R>
    where
        P: Page + Clone,
        F: FnOnce(&mut P) -> R,
        for<'a> &'a mut Latch<MemPage>: TryInto<&'a mut P, Error = IoError>,
        MemPage: From<MemBlock<P::Header>>,
    {
        if let Some(guard) = self.stack_mut().get_mut(id) {
            let page_mut: &mut P = guard.try_into()?;
            // Execute the callback
            let result = callback(page_mut);
            return Ok(result);
        }
        Err(IoError::new(
            IoErrorKind::NotFound,
            format!("Page id: {id}, not found in the latch stack of current transaction"),
        ))
    }

    // Allows to execute a read only callback on the given page.
    pub(crate) fn read_page<P, F, R>(&self, id: PageId, callback: F) -> io::Result<R>
    where
        P: Page + Clone,
        F: FnOnce(&P) -> R,
        for<'a> &'a Latch<MemPage>: TryInto<&'a P, Error = IoError>,
        MemPage: From<MemBlock<P::Header>>,
    {
        if let Some(guard) = self.stack_ref().get(id) {
            let page: &P = guard.try_into()?;
            // Execute the callback
            let result = callback(page);
            return Ok(result);
        }
        Err(IoError::new(
            IoErrorKind::NotFound,
            format!("Page id: {id}, not found in the latch stack of current transaction"),
        ))
    }

    // Same as [`write_page`] but the callback must return io::Result, propagating the error o the caller.
    // The page latch must have been already acquired.
    pub(crate) fn try_write_page<P, F, R>(&mut self, id: PageId, callback: F) -> io::Result<R>
    where
        P: Page + Clone,
        F: FnOnce(&mut P) -> io::Result<R>,
        for<'a> &'a mut Latch<MemPage>: TryInto<&'a mut P, Error = IoError>,
        MemPage: From<P>,
    {
        if let Some(guard) = self.stack_mut().get_mut(id) {
            let page_mut: &mut P = guard.try_into()?;
            // Execute the callback
            let result = callback(page_mut);

            return result;
        }
        Err(IoError::new(
            IoErrorKind::NotFound,
            format!("Page id: {id}, not found in the latch stack of current transaction"),
        ))
    }

    // Same as [`read_page`] but the callback must return io::Result, propagating the error o the caller.
    // Allows to execute a read only callback on the given page.
    pub(crate) fn try_read_page<P, F, R>(&self, id: PageId, callback: F) -> io::Result<R>
    where
        P: Page + Clone,
        F: FnOnce(&P) -> io::Result<R>,
        for<'a> &'a Latch<MemPage>: TryInto<&'a P, Error = IoError>,
        MemPage: From<MemBlock<P::Header>>,
    {
        if let Some(guard) = self.stack_ref().get(id) {
            let page: &P = guard.try_into()?;
            // Execute the callback
            let result = callback(page);
            return result;
        }
        Err(IoError::new(
            IoErrorKind::NotFound,
            format!("Page id: {id}, not found in the latch stack of current transaction"),
        ))
    }

    pub(crate) fn push_to_log(&self, record: OwnedLogRecord) -> io::Result<()> {
        self.pager.write().push_to_log(record)
    }
}
