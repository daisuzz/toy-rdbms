use anyhow::Result;

use toy_rdbms::buffer_pool_manager::{BufferPool, BufferPoolManager};
use toy_rdbms::disk_manager::{DiskManager, PageId};
use toy_rdbms::query::UniqueIndex;
use toy_rdbms::table::{Table};

fn main() -> Result<()> {
    // 初期化処理
    let disk = DiskManager::open("table.toy")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = BufferPoolManager::new(disk, pool);

    let mut table = Table {
        meta_page_id: PageId::INVALID_PAGE_ID,
        num_key_elements: 1,
        unique_indices: vec![UniqueIndex {
            meta_page_id: PageId::INVALID_PAGE_ID,
            skey: vec![2],
        }],
    };

    table.create(&mut bufmgr)?;

    // データの挿入
    table.insert(&mut bufmgr, &[b"z", b"Alice", b"Smith"])?;
    table.insert(&mut bufmgr, &[b"x", b"Bob", b"Johnson"])?;
    table.insert(&mut bufmgr, &[b"y", b"Charlie", b"Williams"])?;
    table.insert(&mut bufmgr, &[b"w", b"Dave", b"Miller"])?;
    table.insert(&mut bufmgr, &[b"v", b"Eve", b"Brown"])?;

    // ヒープファイルに反映
    bufmgr.flush()?;

    Ok(())
}
