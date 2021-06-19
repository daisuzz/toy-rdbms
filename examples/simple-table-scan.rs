use anyhow::Result;

use toy_rdbms::btree::{BTree, SearchMode};
use toy_rdbms::buffer_pool_manager::{BufferPool, BufferPoolManager};
use toy_rdbms::disk_manager::{DiskManager, PageId};
use toy_rdbms::tuple;

/*
  フルスキャン
  SELECT * WHERE last_name = 'Smith' と同等の処理
*/
fn main() -> Result<()> {
    // 初期化処理
    let disk = DiskManager::open("simple.toy")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = BufferPoolManager::new(disk, pool);

    // BTreeからデータを取得
    let btree = BTree::new(PageId(0));
    let mut iter = btree.search(&mut bufmgr, SearchMode::Start)?;

    // データを表示
    while let Some((key, value)) = iter.next(&mut bufmgr)? {
        let mut record = vec![];
        tuple::decode(&key, &mut record);
        tuple::decode(&value, &mut record);
        if record[2] == b"Smith" {
            println!("{:?}", tuple::Pretty(&record));
        }
    }

    Ok(())
}
