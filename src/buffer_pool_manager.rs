use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::io;
use std::ops::{Index, IndexMut};
use std::rc::Rc;

use crate::disk_manager::{DiskManager, PAGE_SIZE, PageId};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("no free buffer available in buffer pool")]
    NoFreeBuffer,
}

// 実際のデータが格納されたバイト列
pub type Page = [u8; PAGE_SIZE];

// Frameに紐づくIndex
#[derive(Default, Clone, Copy)]
pub struct BufferId(usize);

// PageにIDと更新フラグを持たせたもの
pub struct Buffer {
    pub page_id: PageId,
    pub page: RefCell<Page>,
    pub is_dirty: Cell<bool>,
}

impl Default for Buffer {
    fn default() -> Self {
        Self {
            page_id: Default::default(),
            page: RefCell::new([0u8; PAGE_SIZE]),
            is_dirty: Cell::new(false),
        }
    }
}

// Bufferに参照カウントを持たせたもの
#[derive(Default)]
pub struct Frame {
    usage_count: u64,
    buffer: Rc<Buffer>,
}

// バッファの配列と、走査対象のバッファIDを持たせたもの
pub struct BufferPool {
    buffers: Vec<Frame>,
    next_victim_id: BufferId,
}

pub struct BufferPoolManager {
    disk: DiskManager,
    pool: BufferPool,
    page_table: HashMap<PageId, BufferId>,
}

impl BufferPool {
    pub fn new(pool_size: usize) -> Self {
        let mut buffers = vec![];
        buffers.resize_with(pool_size, Default::default);
        let next_victim_id = BufferId::default();
        Self {
            buffers,
            next_victim_id,
        }
    }
    fn size(&self) -> usize {
        self.buffers.len()
    }

    fn evict(&mut self) -> Option<BufferId> {
        let pool_size = self.size();
        let mut consecutive_pinned = 0;

        // 不要なバッファを探す
        let victim_id = loop {
            let next_victim_id = self.next_victim_id;
            let frame = &mut self[next_victim_id];

            // バッファがどこからも参照されていない場合、不要なバッファとする
            if frame.usage_count == 0 {
                break self.next_victim_id;
            }

            // バッファが貸し出し中ではない場合
            if Rc::get_mut(&mut frame.buffer).is_some() {
                frame.usage_count -= 1;
                consecutive_pinned = 0;
            } else {
                // バッファが貸し出し中の場合
                consecutive_pinned += 1;

                // 全てのバッファが貸し出し中の場合
                if consecutive_pinned >= pool_size {
                    return None;
                }
            }

            // 次のバッファを走査
            self.next_victim_id = self.increment_id(self.next_victim_id);
        };
        Some(victim_id)
    }

    fn increment_id(&self, buffer_id: BufferId) -> BufferId {
        BufferId((buffer_id.0 + 1) % self.size())
    }
}

impl Index<BufferId> for BufferPool {
    type Output = Frame;
    fn index(&self, index: BufferId) -> &Self::Output {
        &self.buffers[index.0]
    }
}

impl IndexMut<BufferId> for BufferPool {
    fn index_mut(&mut self, index: BufferId) -> &mut Self::Output {
        &mut self.buffers[index.0]
    }
}

impl BufferPoolManager {
    pub fn new(disk: DiskManager, pool: BufferPool) -> Self {
        let page_table = HashMap::new();
        Self {
            disk,
            pool,
            page_table,
        }
    }

    // ページをバッファから読み込む
    fn fetch_page(&mut self, page_id: PageId) -> Result<Rc<Buffer>, Error> {

        // 対象のページがバッファプールにある場合は、バッファを返す
        if let Some(&buffer_id) = self.page_table.get(&page_id) {
            let frame = &mut self.pool[buffer_id];
            frame.usage_count += 1;
            return Ok(frame.buffer.clone());
        }

        // 空いているバッファを取得
        let buffer_id = self.pool.evict().ok_or(Error::NoFreeBuffer)?;

        let frame = &mut self.pool[buffer_id];
        let evict_page_id = frame.buffer.page_id;
        {
            let buffer = Rc::get_mut(&mut frame.buffer).unwrap();

            // 捨てるバッファが更新されている場合、ディスクに反映
            if buffer.is_dirty.get() {
                self.disk.write_page_data(evict_page_id, buffer.page.get_mut())?;
            }

            // バッファにページを格納
            buffer.page_id = page_id;
            buffer.is_dirty.set(false);
            self.disk.read_page_data(page_id, buffer.page.get_mut())?;
            frame.usage_count = 1;
        }

        // ページテーブルを更新
        let page = Rc::clone(&frame.buffer);
        self.page_table.remove(&evict_page_id);
        self.page_table.insert(page_id, buffer_id);

        Ok(page)
    }

    // ページを作成する
    fn create_page(&mut self) -> Result<Rc<Buffer>, Error> {
        // 空いているバッファを取得
        let buffer_id = self.pool.evict().ok_or(Error::NoFreeBuffer)?;
        let frame = &mut self.pool[buffer_id];
        let evict_page_id = frame.buffer.page_id;

        let page_id = {
            let buffer = Rc::get_mut(&mut frame.buffer).unwrap();

            // 捨てるバッファが更新されている場合、ディスクに反映
            if buffer.is_dirty.get() {
                self.disk
                    .write_page_data(evict_page_id, buffer.page.get_mut())?;
            }

            // バッファを作成
            self.page_table.remove(&evict_page_id);
            let page_id = self.disk.allocate_page();
            *buffer = Buffer::default();
            buffer.page_id = page_id;
            buffer.is_dirty.set(true);
            frame.usage_count = 1;

            page_id
        };

        // ページテーブルを更新
        let page = Rc::clone(&frame.buffer);
        self.page_table.remove(&evict_page_id);
        self.page_table.insert(page_id, buffer_id);

        Ok(page)
    }

    // バッファプールの中身をディスクに反映する
    pub fn flush(&mut self) -> Result<(), Error> {
        for (&page_id, &buffer_id) in self.page_table.iter() {
            let frame = &self.pool[buffer_id];
            let mut page = frame.buffer.page.borrow_mut();
            self.disk.write_page_data(page_id, page.as_mut())?;
            frame.buffer.is_dirty.set(false);
        }
        self.disk.sync()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempfile;

    use super::*;

    #[test]
    fn test() {
        let mut hello = Vec::with_capacity(PAGE_SIZE);
        hello.extend_from_slice(b"hello");
        hello.resize(PAGE_SIZE, 0);
        let mut world = Vec::with_capacity(PAGE_SIZE);
        world.extend_from_slice(b"world");
        world.resize(PAGE_SIZE, 0);

        let disk = DiskManager::new(tempfile().unwrap()).unwrap();
        let pool = BufferPool::new(1);
        let mut bufmgr = BufferPoolManager::new(disk, pool);
        let page1_id = {
            let buffer = bufmgr.create_page().unwrap();
            assert!(bufmgr.create_page().is_err());
            let mut page = buffer.page.borrow_mut();
            page.copy_from_slice(&hello);
            buffer.is_dirty.set(true);
            buffer.page_id
        };
        {
            let buffer = bufmgr.fetch_page(page1_id).unwrap();
            let page = buffer.page.borrow();
            assert_eq!(&hello, page.as_ref());
        }
        let page2_id = {
            let buffer = bufmgr.create_page().unwrap();
            let mut page = buffer.page.borrow_mut();
            page.copy_from_slice(&world);
            buffer.is_dirty.set(true);
            buffer.page_id
        };
        {
            let buffer = bufmgr.fetch_page(page1_id).unwrap();
            let page = buffer.page.borrow();
            assert_eq!(&hello, page.as_ref());
        }
        {
            let buffer = bufmgr.fetch_page(page2_id).unwrap();
            let page = buffer.page.borrow();
            assert_eq!(&world, page.as_ref());
        }
    }
}
