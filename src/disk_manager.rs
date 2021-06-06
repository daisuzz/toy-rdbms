use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::io;
use std::path::Path;

pub const PAGE_SIZE: usize = 4096;

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct PageId(pub u64);

impl Default for PageId {
    fn default() -> Self {
        Self::INVALID_PAGE_ID
    }
}

impl PageId {
    pub const INVALID_PAGE_ID: PageId = PageId(u64::MAX);
    pub fn to_u64(self) -> u64 { self.0 }
}

pub struct DiskManager {
    heap_file: File,
    next_page_id: u64,
}

impl DiskManager {
    // コンストラクタ
    pub fn new(heap_file: File) -> io::Result<Self> {
        let heap_file_size = heap_file.metadata()?.len();
        let next_page_id = heap_file_size / PAGE_SIZE as u64;
        Ok(Self {
            heap_file,
            next_page_id,
        })
    }

    // ファイルを開く
    pub fn open(heap_file_path: impl AsRef<Path>) -> io::Result<Self> {
        let heap_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(heap_file_path)?;
        Self::new(heap_file)
    }

    // ページを作成する
    pub fn allocate_page(&mut self) -> PageId {
        let page_id = self.next_page_id;
        self.next_page_id += 1;
        PageId(page_id)
    }

    // データをページから読み出す
    pub fn read_page_data(&mut self, page_id: PageId, data: &mut [u8]) -> io::Result<()> {
        let offset = PAGE_SIZE as u64 * page_id.to_u64(); // オフセットの計算
        self.heap_file.seek(SeekFrom::Start(offset))?; // ファイルの先頭からoffsetバイト目(ページの先頭)
        self.heap_file.read_exact(data) // データの読み込み
    }

    // データをページに書き込む
    pub fn write_page_data(&mut self, page_id: PageId, data: &[u8]) -> io::Result<()> {
        let offset = PAGE_SIZE as u64 * page_id.to_u64();  // オフセットの計算
        self.heap_file.seek(SeekFrom::Start(offset))?;  // ファイルの先頭からoffsetバイト目(ページの先頭)
        self.heap_file.write_all(data)  // データの書き込み
    }

    // ファイルシステムの内容をディスクに反映する
    pub fn sync(&mut self) -> io::Result<()> {
        self.heap_file.flush()?;
        self.heap_file.sync_all()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn test() {
        let (data_file, data_file_path) = NamedTempFile::new().unwrap().into_parts();
        let mut disk = DiskManager::new(data_file).unwrap();

        let mut hello = Vec::with_capacity(PAGE_SIZE);
        hello.extend_from_slice(b"hello");
        hello.resize(PAGE_SIZE, 0);
        let hello_page_id = disk.allocate_page();
        disk.write_page_data(hello_page_id, &hello).unwrap();

        let mut world = Vec::with_capacity(PAGE_SIZE);
        world.extend_from_slice(b"world");
        world.resize(PAGE_SIZE, 0);
        let world_page_id = disk.allocate_page();
        disk.write_page_data(world_page_id, &world).unwrap();

        drop(disk);

        let mut disk2 = DiskManager::open(&data_file_path).unwrap();
        let mut buf = vec![0; PAGE_SIZE];

        disk2.read_page_data(hello_page_id, &mut buf).unwrap();
        assert_eq!(hello, buf);

        disk2.read_page_data(world_page_id, &mut buf).unwrap();
        assert_eq!(world, buf);
    }
}
