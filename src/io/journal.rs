use crate::io::disk::{Buffer, FileOps};
use crate::serialization::Serializable;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub struct Journal {
    page_size: u32,
    buffer: Buffer,
    file: File,
}

impl Journal {
    pub fn push_page(&mut self, page: MemFrame) -> std::io::Result<()> {
        page.write_to(&mut self.buffer)?;
        Ok(())
    }

    fn push_pages(&mut self, pages: Vec<IOFrame>) -> std::io::Result<()> {
        for page in pages {
            self.push_page(page)?;
        }
        Ok(())
    }

    pub fn set_page_size(&mut self, page_size: u32) {
        self.page_size = page_size
    }

    pub fn iter(&mut self) -> JournalIterator<'_> {
        JournalIterator { journal: self }
    }
}

impl FileOps for Journal {
    fn create(path: impl AsRef<Path>) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let file = File::create(&path)?;
        let buffer = Buffer::create(&path)?;
        Ok(Self {
            page_size: 0,
            buffer,
            file,
        })
    }

    fn open(path: impl AsRef<Path>) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        let buffer = Buffer::open(&path)?;
        // The buffer will automatically read the contents of the file.
        let file = File::open(&path)?;
        Ok(Self {
            page_size: 0,
            file,
            buffer,
        })
    }

    fn remove(path: impl AsRef<Path>) -> std::io::Result<()> {
        File::remove(path)
    }

    fn sync_all(&self) -> std::io::Result<()> {
        self.file.sync_all()
    }

    fn truncate(&mut self) -> std::io::Result<()> {
        self.buffer.truncate()?;
        self.file.truncate()
    }
}

impl Write for Journal {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.truncate()?;
        self.file.seek(SeekFrom::Start(0))?;

        self.file.write_all(self.buffer.as_slice())?;
        self.buffer.truncate()?;
        Ok(())
    }
}

impl Seek for Journal {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.buffer.seek(pos)
    }
}

impl Read for Journal {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.buffer.read(buf)
    }
}

pub(crate) struct JournalIterator<'a> {
    journal: &'a mut Journal,
}

impl<'a> Iterator for JournalIterator<'a> {
    type Item = IOFrame;

    fn next(&mut self) -> Option<Self::Item> {
        IOFrame::read_from(&mut self.journal).ok()
    }
}
