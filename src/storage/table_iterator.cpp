#include "storage/table_iterator.h"
#include "common/macros.h"
#include "storage/table_heap.h"

// 注意，我修改了构造函数的参数
TableIterator::TableIterator(TableHeap *t, RowId rid) : table_heap_(t) {
  if (rid.GetPageId() != INVALID_PAGE_ID) {
    row_ = new Row(rid);
    table_heap_->GetTuple(row_, nullptr);
  } else {
    row_ = new Row(INVALID_ROWID);
  }
}

TableIterator::TableIterator(const TableIterator &other) {
  table_heap_ = other.table_heap_;
  row_ = other.row_;
}

TableIterator::~TableIterator() {}

bool TableIterator::operator==(const TableIterator &itr) const {
  if(itr.row_->GetRowId()==INVALID_ROWID&&row_->GetRowId()==INVALID_ROWID){
    return true;
  }
  else if(itr.row_->GetRowId()==INVALID_ROWID||row_->GetRowId()==INVALID_ROWID){

    return false;
  }
  else if(itr.row_->GetRowId().GetPageId()==row_->GetRowId().GetPageId()&&row_->GetRowId().GetSlotNum()==itr.row_->GetRowId().GetSlotNum()){
    return true;
  }
  else{
    return false;
  }
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  if(itr.row_->GetRowId()==INVALID_ROWID&&row_->GetRowId()==INVALID_ROWID){
    return false;
  }
  else if(itr.row_->GetRowId()==INVALID_ROWID||row_->GetRowId()==INVALID_ROWID){
    return true;
  }
  else if(itr.row_->GetRowId().GetPageId()==row_->GetRowId().GetPageId()&&row_->GetRowId().GetSlotNum()==itr.row_->GetRowId().GetSlotNum()){
    return false;
  }
  else {
    return true;
  }
}

const Row &TableIterator::operator*(){
    return *(row_);
}

Row *TableIterator::operator->(){
    return row_;
}

TableIterator &TableIterator::operator++() {
  if (row_ == nullptr||row_->GetRowId() == INVALID_ROWID) {
    delete row_;
    row_ = new Row(INVALID_ROWID);
    return *this;
  }
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(row_->GetRowId().GetPageId()));
  RowId id_new;
  page->RLatch();
  if (page->GetNextTupleRid(row_->GetRowId(), &id_new)) { // 直接找到了那么就直接读取
    delete row_;
    row_ = new Row(id_new);
    if (*this != table_heap_->End()) {
      table_heap_->GetTuple(row_, nullptr);
    }
    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(row_->GetRowId().GetPageId(), false);
  }
  else { // 本页没有合适的，去找下一页
    page_id_t next_page_id = page->GetNextPageId();
    // 搜索，直到最后一页
    while (next_page_id != INVALID_PAGE_ID) {
      auto new_page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(next_page_id));
      page->RUnlatch();
      table_heap_->buffer_pool_manager_->UnpinPage(row_->GetRowId().GetPageId(), false);
      page = new_page;
      page->RLatch();
      if (page->GetFirstTupleRid(&id_new)) {
        delete row_;
        row_ = new Row(id_new);
        break; // 找到了就退出
      }
      next_page_id = page->GetNextPageId();
    }
    // 如果next_page_id不是非法的则可以读取tuple,否则需要返回nullptr构成的iter
    if (next_page_id != INVALID_PAGE_ID) {
      table_heap_->GetTuple(row_, nullptr);
    } else {
      delete row_;
      row_ = new Row(INVALID_ROWID);
    }
    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(row_->GetRowId().GetPageId(), false);
  }
  return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator p(table_heap_, row_->GetRowId());
  ++(*this);
  return TableIterator{p};
}

// TODO:
TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  table_heap_ = itr.table_heap_;
  row_ = itr.row_;
  //    txn_ = itr.txn_;
  return *this;
};