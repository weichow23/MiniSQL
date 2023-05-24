#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm) {
  page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id));
}

IndexIterator::~IndexIterator() {
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}
/* yf 改 */
std::pair<GenericKey *, RowId> IndexIterator::operator*() {
//  ASSERT(false, "Not implemented yet.");
  return page->GetItem(item_index);
}
/* yf 改 */
IndexIterator &IndexIterator::operator++() {
//  ASSERT(false, "Not implemented yet.");
  item_index++;
  if (item_index == this->page->GetSize() && this->page->GetNextPageId() != -1) {
    page_id_t next = page->GetNextPageId();
    Page *Next_page = this->buffer_pool_manager->FetchPage(next);
    LeafPage *next_node = reinterpret_cast<LeafPage *>(Next_page);
    page = next_node;
    this->buffer_pool_manager->UnpinPage(Next_page->GetPageId(), false);
    item_index = 0;
  }
  return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const {
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const {
  return !(*this == itr);
}