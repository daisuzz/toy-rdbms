use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

pub struct DiskManager {
    heap_file: File,
    next_page_id: u64,
}

impl DiskManager {
    // コンストラクタ
    pub fn new(data_file: File) -> io::Result<Self> {}

    // ファイルを開く
    pub fn open(data_file_path: impl AsRef<Path>) -> io::Result<Self> {

    }

    // ページを作成する
    pub fn allocate_page(&mut self) -> PageId {}

    // データをページから読み出す
    pub fn read_page_data(&mut self, page_id: PageId, data: &mut [u8]) -> io::Result<()> {}

    // データをページに書き込む
    pub fn write_page_data(&mut self, page_id: PageId, data: &[u8]) -> io::Result<()> {}
}
