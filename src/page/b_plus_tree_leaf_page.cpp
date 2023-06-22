#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
/* Init */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetSize(0);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetKeySize(key_size);
  SetNextPageId(INVALID_PAGE_ID);
}
/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/* KeyIndex */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  int st = 0, ed = GetSize() - 1;
  while (st <= ed) {  // find the first key in array >= input
    int mid = (ed - st) / 2 + st;
    if (KM.CompareKeys(KeyAt(mid), key) >= 0)
      ed = mid - 1;
    else
      st = mid + 1;
  }
  return ed + 1;
}
/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
/* GetItem */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) {
  return make_pair(KeyAt(index), ValueAt(index));
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
/* Insert */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int size = GetSize();
  int now_key = KeyIndex(key, KM);
  for (int i = size; i > now_key; i--) {
    SetKeyAt(i, KeyAt(i - 1));
    SetValueAt(i, ValueAt(i - 1));
  }
  SetKeyAt(now_key, key);
  SetValueAt(now_key, value);
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
/* MoveHalfTo */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int size = GetSize();
  int start = GetMaxSize() / 2;
  int length = size - start;
  recipient->CopyNFrom(pairs_off + start*GetKeySize(), length);
  SetSize(start);
}
/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
/* CopyNFrom */
void LeafPage::CopyNFrom(void *src, int size) {
  //PairCopy(pairs_off + GetSize() * pair_size, src, size);
    PairCopy(pairs_off, src, size);
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
/* Lookup */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
//  int size = GetSize();
//  int now_key = KeyIndex(key, KM);
//  if (now_key < size && KM.CompareKeys(key, KeyAt(now_key)) == 0) {
//    value = ValueAt(now_key);
//    return true;
//  }
//  return false;
  // 优化为二分查找
  int left = 0;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = (left + right) / 2;
    if (KM.CompareKeys(key, KeyAt(mid)) == 0) {
      value = ValueAt(mid);
      return true;
    } else if (KM.CompareKeys(key, KeyAt(mid)) > 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
/* RemoveAndDeleteRecord */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int size = GetSize();
  int now_key = KeyIndex(key, KM);
  if (now_key < size && KM.CompareKeys(key, KeyAt(now_key)) == 0) {
    for (int i = now_key; i < size - 1; i++) {
      SetKeyAt(i, KeyAt(i + 1));
      SetValueAt(i, ValueAt(i + 1));
    }
    IncreaseSize(-1);
    return GetSize();
  } else
    return size;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
/* MoveAllTo */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  int size = GetSize();
  recipient->CopyNFrom(pairs_off, size);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
/* MoveFirstToEndOf */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  int size = GetSize();
  recipient->CopyLastFrom(KeyAt(0), ValueAt(0));
  IncreaseSize(-1);
  for (int i = 0; i < size - 1; i++) {
    SetKeyAt(i, KeyAt(i + 1));
    SetValueAt(i, ValueAt(i + 1));
  }
}
/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
/* CopyLastFrom */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  int size = GetSize();
  SetKeyAt(size, key);
  SetValueAt(size, value);
  IncreaseSize(1);
}
/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
/* MoveLastToFrontOf */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  int size = GetSize();
  recipient->CopyFirstFrom(KeyAt(size - 1), ValueAt(size - 1));
  IncreaseSize(-1);
}
/*
 * Insert item at the front of my items. Move items accordingly.
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  int size = GetSize();
  for (int i = size - 1; i >= 0; i--) {
    SetKeyAt(i + 1, KeyAt(i));
    SetValueAt(i + 1, ValueAt(i));
  }
  SetKeyAt(0, key);
  SetValueAt(0, value);
  IncreaseSize(1);
}