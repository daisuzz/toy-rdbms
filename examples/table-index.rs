use anyhow::Result;

use toy_rdbms::buffer_pool_manager::{BufferPool, BufferPoolManager};
use toy_rdbms::disk_manager::{DiskManager, PageId};
use toy_rdbms::query::{IndexScan, PlanNode, TupleSearchMode};
use toy_rdbms::tuple;

fn main() -> Result<()> {
    // 初期化
    let disk = DiskManager::open("table.toy")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = BufferPoolManager::new(disk, pool);

    // セカンダリインデックスを利用する実行計画を作成
    let plan = IndexScan {
        table_meta_page_id: PageId(0),
        index_meta_page_id: PageId(2),
        search_mode: TupleSearchMode::Key(&[b"Smith"]),
        while_cond: &|skey| skey[0].as_slice() == b"Smith",
    };

    // 実行計画から生成したクエリエクスキュータを実行してデータを取得、その後データを表示
    let mut exec = plan.start(&mut bufmgr)?;
    while let Some(record) = exec.next(&mut bufmgr)? {
        println!("{:?}", tuple::Pretty(&record));
    }
    Ok(())
}
