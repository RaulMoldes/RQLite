//! # Disk Module
//!
//! This module implements the required functionality to manage the low-level operations
//! of a database file. It provides the necessary methods to read and write pages,
//! manage the database header, and allocate new pages as needed.
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::header::{Header, SQLITE_HEADER_STRING};

/// The `DiskManager` struct is the main component for managing the database file on disk.
/// It provides methods to read and write pages, manage the database header, and allocate new pages as needed.
/// Does not interact with the buffer pool or any higher-level abstractions. This is basically disk I/O operations.
pub struct DiskManager {
    /// Path to the database file.
    pub path: PathBuf,
    /// Handler for the database file.
    file: File,
    /// Page size in bytes. Page size is fixed for the entire database.
    /// It is set when the database is created and cannot be changed later.
    page_size: u32,
}

impl DiskManager {
    /// Creates a new DiskManager instance, and opens an existing database file.
    /// It is a way of creating a new instance of DiskManager that is already connected to an existing database file.
    /// Note that the page size is not set until the header is read. The page size is set up from the header of the existing database.
    /// This method will only work on database files that are already created and have a valid header (See the header.rs module for details).
    ///
    /// # Parameters
    /// * `path` - Path to the database file.
    ///
    /// # Errors
    /// Returns an error if the file cannot be opened or if the header is invalid.
    ///
    /// # Returns
    /// A new instance of DiskManager connected to the specified database file.
    ///
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        let mut disk_manager = DiskManager {
            path: path.as_ref().to_path_buf(),
            file,
            page_size: 0, // Initialized as 0, will be set when reading the header
        };

        // Read the header to get the page size
        // and set the page size in the DiskManager instance.
        let header = disk_manager.read_header()?;
        disk_manager.page_size = header.page_size;

        Ok(disk_manager)
    }

    /// Creates a new database file and initializes it with the specified page size.
    /// This method will create a new file at the specified path and write the initial header to it.
    /// It will also allocate the first page of the database.
    ///
    /// # Parameters
    /// * `path` - Path to the database file.
    /// * `page_size` - Size of each page in bytes. This value must be a power of 2 and between 512 and 65536.
    ///
    /// # Errors
    /// Returns an error if the file cannot be created or if the page size is invalid.
    ///
    /// # Returns
    /// A new instance of DiskManager connected to the newly created database file.
    ///
    pub fn create<P: AsRef<Path>>(path: P, page_size: u32) -> io::Result<Self> {
        // Create the file with write and read permissions
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        let mut disk_manager = DiskManager {
            path: path.as_ref().to_path_buf(),
            file,
            page_size,
        };

        // Create the header with the specified page size
        // The header is written to the first 100 bytes of the file.
        let header = Header::with_page_size(page_size)?;
        disk_manager.write_header(&header)?;

        // Write the first page. Required in SQLite to have at least one page allocated.
        // I am not sure why the SQLite developers decided to do this, but let's not ask questions for this time.
        // It is probably to ensure that the database file is not empty and has a valid structure.
        disk_manager.allocate_pages(1)?;

        Ok(disk_manager)
    }

    /// Reads the header of the database file. Will fail if the database file is corrupted or the header is invalid (aka we do not have the magic string).
    ///
    /// Parameters
    /// * `&mut self` - A mutable reference to the DiskManager instance.
    ///     
    /// # Errors
    /// Returns an error if the file cannot be read or if the header is invalid.
    ///
    /// # Returns
    /// A Header instance containing the database header information.
    ///
    pub fn read_header(&mut self) -> io::Result<Header> {
        self.file.seek(SeekFrom::Start(0))?;

        // Verify the signature
        // Read the first 16 bytes to check the signature
        let mut signature = [0u8; 16];
        self.file.read_exact(&mut signature)?;

        if &signature != SQLITE_HEADER_STRING {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid signature: expected SQLITE_HEADER_STRING",
            ));
        }

        // Back to the beginning of the file to read the header
        self.file.seek(SeekFrom::Start(0))?;
        Header::read_from(&mut self.file)
    }

    /// Writes the header to the database file.
    /// This method will overwrite the existing header in the file.
    ///
    /// # Parameters
    /// * `header` - A reference to the Header instance to write.
    ///
    /// # Errors
    /// Returns an error if the file cannot be written to or if the header is invalid.
    ///
    pub fn write_header(&mut self, header: &Header) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        header.write_to(&mut self.file)
    }

    ///  Reads and entire page from the database file.
    /// This method will read the specified page number from the file and store it in the provided buffer.
    ///
    /// # Parameters
    /// * `page_number` - The page number to read (starting from 1).
    /// * `buffer` - A mutable reference to the buffer where the page data will be stored.
    ///     
    /// # Errors
    /// Returns an error if the file cannot be read or if the page number is invalid.
    ///
    /// # Returns
    /// A result indicating success or failure.
    ///
    /// # Note
    /// The page number is 1-based, meaning the first page is page 1.
    /// The buffer size must match the page size of the database.
    /// We cannot read blocks from the file that are not aligned with the page size. This is why this should be the only accessor to the database file.
    pub fn read_page(&mut self, page_number: u32, buffer: &mut [u8]) -> io::Result<()> {
        if page_number == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid page number: 0 (pages start from 1)",
            ));
        }

        // Compute the page offset
        // The offset is calculated as (page_number - 1) * page_size
        // This is a common way to calculate the offset for fixed-size pages in a file. As all pages are the same size, we can use this formula.
        let offset = self.page_offset(page_number);
        // println!("Reading page {} at offset {}", page_number, offset);

        self.file.seek(SeekFrom::Start(offset))?;

        if buffer.len() != self.page_size as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Buffer size is incorrect: expected {}, obtained {}",
                    self.page_size,
                    buffer.len()
                ),
            ));
        }

        self.file.read_exact(buffer)
    }

    /// Writes an entire page to the database file.
    /// This method will write the specified page number to the file using the provided buffer.
    ///
    /// # Parameters
    /// * `page_number` - The page number to write (starting from 1).
    /// * `buffer` - A reference to the buffer containing the page data to write.
    ///
    /// # Errors
    /// Returns an error if the file cannot be written to or if the page number is invalid.
    ///
    /// # Returns
    /// A result indicating success or failure.
    ///
    pub fn write_page(&mut self, page_number: u32, buffer: &[u8]) -> io::Result<()> {
        if page_number == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid page number: 0 (pages start from 1)",
            ));
        }

        let offset = self.page_offset(page_number);
        self.file.seek(SeekFrom::Start(offset))?;

        if buffer.len() != self.page_size as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Buffer size is incorrect: expected {}, obtained {}",
                    self.page_size,
                    buffer.len()
                ),
            ));
        }

        self.file.write_all(buffer)
    }

    /// Allocates new pages in the database file.
    /// This method will increase the size of the file by the specified number of pages.
    /// It will also initialize the new pages with zeros and update the header to reflect the new size.
    /// It is cool because it is kind of like a malloc but in disk.
    ///
    /// # Parameters
    /// * `count` - The number of pages to allocate.
    ///
    /// # Errors
    /// Returns an error if the file cannot be resized or if the allocation fails.
    ///
    pub fn allocate_pages(&mut self, count: u32) -> io::Result<u32> {
        // Get the current file size
        // This is important because we need to know how many pages we have already allocated.
        // We will use this to calculate the new size of the file.
        let file_size = self.file.metadata()?.len();

        // Calculate the current number of pages. We cannot use page_count() here, because it would cause an error at diskmanager creation.
        // We will use the file size to calculate the number of pages.
        // We add self.page_size - 1 to ensure we round up to the next page size.
        let current_pages = (file_size - 100) / self.page_size as u64;
        let first_new_page = current_pages as u32 + 1;

        // Calculate the new size of the file
        let new_size = file_size + (count as u64 * self.page_size as u64);

        // Update the file size
        // This is important because we need to ensure that the file is large enough to accommodate the new pages.
        self.file.set_len(new_size)?;

        // Initialize the new pages with zeros
        let zeros = vec![0u8; self.page_size as usize];
        for page_number in first_new_page..(first_new_page + count) {
            self.write_page(page_number, &zeros)?;
        }

        // Update the header to reflect the new size
        let mut header = self.read_header()?;

        header.database_size = first_new_page + count - 1;

        self.write_header(&header)?;

        Ok(first_new_page)
    }

    /// Computes the offset of a page in the database file.
    /// This method calculates the offset based on the page number and the page size.
    /// The offset is calculated as (page_number - 1) * page_size.
    ///
    /// # Parameters
    /// * `page_number` - The page number to calculate the offset for (starting from 1).
    ///
    /// # Returns
    /// The offset of the specified page in the database file.
    ///
    /// # Note
    /// The page number is 1-based, meaning the first page is page 1.
    /// The offset is calculated as (page_number - 1) * page_size. Therefore, page 1 starts at offset 0, page 2 starts at offset page_size, and so on.
    /// This is a common way to calculate the offset for fixed-size pages in a file.
    fn page_offset(&self, page_number: u32) -> u64 {
        (page_number as u64 - 1) * self.page_size as u64 + 100 // We add 100 bytes to account for the header and other metadata.
    }

    /// Obtains the number of pages in the database file.
    /// This method calculates the number of pages by dividing the file size by the page size.
    pub fn page_count(&self) -> io::Result<u32> {
        let file_size = self.file.metadata()?.len();
        // We subtract 100 bytes to account for the header and other metadata.
        Ok(((file_size - 100) / self.page_size as u64) as u32)
    }

    /// Syncs the file to ensure all data is written to disk.
    /// This is important for ensuring data integrity, especially after writing.
    /// Not sure if this bypasses the OS cache. According to CMU database course,
    /// we should bypass the OS cache as it is not reliable. This can be achieved by using the `O_DIRECT` flag in Linux
    ///
    /// (See https://15445.courses.cs.cmu.edu/fall2024/slides/06-bufferpool.pdf for details).
    /// # Parameters
    /// * `&mut self` - A mutable reference to the DiskManager instance.
    ///
    /// # Errors
    /// Returns an error if the file cannot be synced.
    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_create_database() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create a new database
        let result = DiskManager::create(&db_path, 4096);
        assert!(result.is_ok());

        // Verify that the database file was created
        assert!(db_path.exists());

        // Verify that the file has the correct size
        let metadata = fs::metadata(&db_path).unwrap();
        assert!(metadata.len() >= 4096);
    }

    #[test]
    fn test_open_database() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create a new database
        {
            let _disk_manager = DiskManager::create(&db_path, 4096).unwrap();
        }

        // Open the existing database
        let result = DiskManager::open(&db_path);
        assert!(result.is_ok());

        let disk_manager = result.unwrap();
        assert_eq!(disk_manager.page_size, 4096);
    }

    #[test]
    fn test_read_write_header() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create a new database
        let mut disk_manager = DiskManager::create(&db_path, 4096).unwrap();

        // Read the header
        let mut header = disk_manager.read_header().unwrap();
        assert_eq!(header.page_size, 4096);

        // Modify the header
        header.user_version = 42;

        // Write the modified header back to the file
        disk_manager.write_header(&header).unwrap();

        // Read the header again to verify the changes
        let header2 = disk_manager.read_header().unwrap();
        assert_eq!(header2.user_version, 42);
    }

    #[test]
    fn test_read_write_page() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create a new database
        let mut disk_manager = DiskManager::create(&db_path, 4096).unwrap();

        // Prepare some data to write
        let mut data = vec![0u8; 4096];
        for i in 0..100 {
            data[i] = i as u8;
        }

        // Write the data to page 1
        disk_manager.write_page(1, &data).unwrap();

        // Read again from page 1
        let mut buffer = vec![0u8; 4096];
        disk_manager.read_page(1, &mut buffer).unwrap();

        // Verify that the data was written correctly
        assert_eq!(&buffer[0..100], &data[0..100]);
    }

    #[test]
    fn test_allocate_pages() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create a new database
        let mut disk_manager = DiskManager::create(&db_path, 4096).unwrap();

        // Verify that the initial page count is 1 (the first page is allocated)
        assert_eq!(disk_manager.page_count().unwrap(), 1);

        // Asign 2 new pages
        let first_new_page = disk_manager.allocate_pages(2).unwrap();
        assert_eq!(first_new_page, 2);

        // Verify that the page count is now 3 (1 initial + 2 allocated)
        assert_eq!(disk_manager.page_count().unwrap(), 3);

        // Verifiy that the header was updated correctly
        let header = disk_manager.read_header().unwrap();
        assert_eq!(header.database_size, 3);
    }

    #[test]
    fn test_invalid_operations() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        // Create a new database
        let mut disk_manager = DiskManager::create(&db_path, 4096).unwrap();

        // Attempt to read a page that does not exist (page 0)
        let mut buffer = vec![0u8; 4096];
        let result = disk_manager.read_page(0, &mut buffer);
        assert!(result.is_err());

        // Intentar escribir la página 0 (inválido)
        let result = disk_manager.write_page(0, &buffer);
        assert!(result.is_err());

        // Attempt to read a page with an invalid page number (e.g., page 2, which does not exist yet)
        let mut small_buffer = vec![0u8; 2048];
        let result = disk_manager.read_page(1, &mut small_buffer);
        assert!(result.is_err());

        // Attempt to write a page with an invalid buffer size (not matching the page size)
        let result = disk_manager.write_page(1, &small_buffer);
        assert!(result.is_err());
    }
}
