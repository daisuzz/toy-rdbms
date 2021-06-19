use anyhow::Result;

use crate::btree::BTree;
use crate::buffer_pool_manager::BufferPoolManager;
use crate::disk_manager::PageId;
use crate::query::UniqueIndex;
use crate::tuple;

pub struct SimpleTable {
    pub meta_page_id: PageId,
    pub num_key_elements: usize, //左からいくつの列がprimary keyかを表す
}

impl SimpleTable {
    pub fn create(&mut self, bufmgr: &mut BufferPoolManager) -> Result<()> {
        let btree = BTree::create(bufmgr)?;
        self.meta_page_id = btree.meta_page_id;
        Ok(())
    }

    pub fn insert(&self, bufmgr: &mut BufferPoolManager, record: &[&[u8]]) -> Result<()> {
        let btree = BTree::new(self.meta_page_id);

        let mut key = vec![];
        tuple::encode(record[..self.num_key_elements].iter(), &mut key);

        let mut value = vec![];
        tuple::encode(record[self.num_key_elements..].iter(), &mut value);

        btree.insert(bufmgr, &key, &value)?;

        Ok(())
    }
}

pub struct Table {
    pub meta_page_id: PageId,

    //左からいくつの列がprimary keyかを表す
    pub num_key_elements: usize,

    pub unique_indices: Vec<UniqueIndex>,
}

impl Table {
    pub fn create(&mut self, bufmgr: &mut BufferPoolManager) -> Result<()> {
        let btree = BTree::create(bufmgr)?;
        self.meta_page_id = btree.meta_page_id;
        for unique_index in &mut self.unique_indices {
            unique_index.create(bufmgr)?;
        }
        Ok(())
    }

    pub fn insert(&self, bufmgr: &mut BufferPoolManager, record: &[&[u8]]) -> Result<()> {
        let btree = BTree::new(self.meta_page_id);

        let mut key = vec![];
        tuple::encode(record[..self.num_key_elements].iter(), &mut key);

        let mut value = vec![];
        tuple::encode(record[self.num_key_elements..].iter(), &mut value);

        btree.insert(bufmgr, &key, &value)?;

        for unique_index in &self.unique_indices {
            unique_index.insert(bufmgr, &key, record)?;
        }

        Ok(())
    }
}
