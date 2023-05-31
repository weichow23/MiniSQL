#include "page/b_plus_tree_page.h"

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
/* IsLeafPage() */
bool BPlusTreePage::IsLeafPage() const {
  if (page_type_ == IndexPageType::LEAF_PAGE)
    return true;
  else
    return false;
}

/* IsRootPage */
bool BPlusTreePage::IsRootPage() const {
  if (parent_page_id_ == INVALID_FRAME_ID)
    return true;
  else
    return false;
}

/* SetPageType */
void BPlusTreePage::SetPageType(IndexPageType page_type) {
  page_type_ = page_type;
}
int BPlusTreePage::GetKeySize() const {
  return key_size_;
}

void BPlusTreePage::SetKeySize(int size) {
  key_size_ = size;
}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
int BPlusTreePage::GetSize() const {
  return size_;
}

void BPlusTreePage::SetSize(int size) {
  size_ = size;
}

void BPlusTreePage::IncreaseSize(int amount) {
  size_ += amount;
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
/* GetMaxSize() */
int BPlusTreePage::GetMaxSize() const {
  return max_size_;
}

/* SetMaxSize */
void BPlusTreePage::SetMaxSize(int size) {
  max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
/* GetMinSize */
int BPlusTreePage::GetMinSize() const {
//  return max_size_ / 2;
  if(IsRootPage())
    return IsLeafPage()?1:2;
  else
    return (max_size_+1)/2;
}

/*
 * Helper methods to get/set parent page id
 */
/* GetParentPageId */
page_id_t BPlusTreePage::GetParentPageId() const {
  return parent_page_id_;
}

void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) {
  parent_page_id_ = parent_page_id;
}

/*
 * Helper methods to get/set self page id
 */
page_id_t BPlusTreePage::GetPageId() const {
  return page_id_;
}

void BPlusTreePage::SetPageId(page_id_t page_id) {
  page_id_ = page_id;
}

/*
 * Helper methods to set lsn  数据页的日志序列号，我不会用到
 */
void BPlusTreePage::SetLSN(lsn_t lsn) {
  lsn_ = lsn;
}