#include "storage/table_heap.h"

/*向堆表中插入一条记录，插入记录后生成的RowId需要通过row对象返回（即row.rid_)*/
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  if (row.GetSerializedSize(schema_) > PAGE_SIZE - 32)
    return false;
  // 查找可以包含此元组的页面
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(GetFirstPageId()));
  while (true) {
    if (page == nullptr)
      return false;
    // 若insert完成,则返回true
    if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
      return true;
    }
    // 否则继续找下一个page是否可以insert
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    page_id_t next = page->GetNextPageId();
    // 若当前page不是最后一个page则继续循环,是最后一个就new一个page
    if (next != INVALID_PAGE_ID) {
      page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next));
    } else {
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next));
      page->SetNextPageId(next);
      new_page->Init(next, page->GetPageId(), log_manager_, txn);
      new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
      buffer_pool_manager_->UnpinPage(next, true);
      return true;
    }
  }
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr)
    return false;
  // 标记需要删除的页
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/*将RowId为rid的记录old_row替换成新的记录new_row，并将new_row的RowId通过new_row.rid_返回*/
bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  // 得到old_row
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr)
      return false;
  Row old_row(rid);
  if (!GetTuple(&old_row, txn))
    return false;
  // 更新old_row
  page->WLatch();
  int type = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  if (type == 0) {
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
  } else {
      // 空间不够，先删除再插入
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false;
  }
}

/*从物理意义上删除这条记录*/
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
    //ASSERT(page != nullptr, "Page should not be null!");
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
    //ASSERT(page != nullptr, "Page should not be null!");
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/* 获取RowId为row->rid_的记录 */
bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if (page == nullptr)
      return false;
  page->RLatch();
  if (!page->GetTuple(row, schema_, txn, lock_manager_)) {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return false;
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return true;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/*获取堆表的首迭代器；*/
TableIterator TableHeap::Begin(Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  RowId rid;
  page->RLatch();
  page->GetFirstTupleRid(&rid);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(first_page_id_, false);
  return TableIterator(this, rid);
}
/*获取堆表的尾迭代器*/
TableIterator TableHeap::End() {
  return TableIterator(this,INVALID_ROWID);//rowid=(page_id,slot_id)=(-1,0)
}