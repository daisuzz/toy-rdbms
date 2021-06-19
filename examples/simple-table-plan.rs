use anyhow::Result;

use toy_rdbms::btree::Error::Buffer;
use toy_rdbms::buffer_pool_manager::{BufferPool, BufferPoolManager};
use toy_rdbms::disk_manager::{DiskManager, PageId};
use toy_rdbms::query::{Filter, PlanNode, SeqScan, TupleSearchMode};
use toy_rdbms::tuple;

fn main() -> Result<()> {

    // 初期化
    let disk = DiskManager::open("simple.toy")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = BufferPoolManager::new(disk, pool);

    // 範囲検索を行って、その後条件に合致する行を取得する実行計画を作成
    let plan = Filter {
        cond: &|record| record[1].as_slice() < b"Dave",
        inner_plan: &SeqScan {
            table_meta_page_id: PageId(0),
            search_mode: TupleSearchMode::Key(&[b"w"]),
            while_cond: &|pkey| pkey[0].as_slice() < b"z",
        },
    };

    // 実行計画から生成したクエリエクスキュータを実行してデータを取得、その後データを表示
    let mut exec = plan.start(&mut bufmgr)?;
    while let Some(record) = exec.next(&mut bufmgr)? {
        println!("{:?}", tuple::Pretty(&record));
    }
    Ok(())
}
