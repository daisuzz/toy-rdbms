use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::io;
use std::path::Path;

pub struct PageId(pub u64);

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
        self.heap_file.seek(SeekFrom::Start(offset)); // ファイルの先頭からoffsetバイト目(ページの先頭)
        self.heap_file.read_exact(data) // データの読み込み
    }

    // データをページに書き込む
    pub fn write_page_data(&mut self, page_id: PageId, data: &[u8]) -> io::Result<()> {
        let offset = PAGE_SIZE as u64 * page_id.to_u64();  // オフセットの計算
        self.heap_file.seek(SeekFrom::Start(offset))?;  // ファイルの先頭からoffsetバイト目(ページの先頭)
        self.heap_file.write_all(data)  // データの書き込み
    }
}
