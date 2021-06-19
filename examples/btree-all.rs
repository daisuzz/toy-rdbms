use anyhow::Result;

use toy_rdbms::btree::{BTree, SearchMode};
use toy_rdbms::buffer_pool_manager::{BufferPool, BufferPoolManager};
use toy_rdbms::disk_manager::{DiskManager, PageId};

fn main() -> Result<()> {
    let disk = DiskManager::open("simple.toy")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = BufferPoolManager::new(disk, pool);

    let btree = BTree::new(PageId(0));
    let mut iter = btree.search(&mut bufmgr, SearchMode::Start)?;

    while let Some((key, value)) = iter.next(&mut bufmgr)? {
        println!("{:02x?} = {:02x?}", key, value);
    }
    Ok(())
}
