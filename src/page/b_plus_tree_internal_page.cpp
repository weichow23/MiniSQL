#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetKeySize(key_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int left = 1; 
  int right = GetSize() - 1; 

  while (left <= right) {
    int mid = (left + right) / 2; 
    GenericKey *current_key = KeyAt(mid);
    int compare_result = KM.CompareKeys(key, current_key);

    if (compare_result <0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  return ValueAt(left-1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  SetValueAt(0,old_value);
  SetKeyAt(1,new_key);
  SetValueAt(1,new_value);
  SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int temp_index = ValueIndex(old_value) + 1;
  for (int i = GetSize();i>temp_index;i--)
  {
    SetKeyAt(i,KeyAt(i-1));
    SetValueAt(i,ValueAt(i-1));
  }
  SetKeyAt(temp_index,new_key);
  SetValueAt(temp_index,new_value);
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int half=GetSize()/2;
  int move=GetSize()-GetSize()/2;
  recipient->CopyNFrom(PairPtrAt(half),move,buffer_pool_manager);
  SetSize(half);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  PairCopy(PairPtrAt(GetSize()),src,size);
  IncreaseSize(size);
  for(int i=0;i<GetSize();i++) 
  {
    auto childpage=buffer_pool_manager->FetchPage(ValueAt(i));
    BPlusTreePage *child_page=reinterpret_cast<BPlusTreePage *>(childpage->GetData());
    child_page->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(ValueAt(i),true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  assert(index >= 0 && index < GetSize());
  for (int i = index+1; i < GetSize(); i++) {
    SetKeyAt(i - 1, KeyAt(i));
    SetValueAt(i - 1, ValueAt(i));
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  IncreaseSize(-1);//足够了吗？
  return ValueAt(0);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  // the separation key from parent
  SetKeyAt(0, middle_key);
  recipient->CopyNFrom(pairs_off,GetSize(),buffer_pool_manager);
  SetSize(0); 
}

/*****************************************************************************
 * REDISTRIBUTE 
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
  GenericKey* key = KeyAt(0);
  page_id_t value = ValueAt(0);
  for(int i = 0; i < GetSize();i++){
    recipient->SetKeyAt(i,this->KeyAt(i+1));
    recipient->SetValueAt(i,this->ValueAt(i+1));
  }
  IncreaseSize(-1);
  recipient->CopyLastFrom(key,value,buffer_pool_manager);
  // update child parent page id
  page_id_t childPageId = value;
  Page *page = buffer_pool_manager->FetchPage(childPageId);
  assert (page != nullptr);
  BPlusTreePage *child = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child->SetParentPageId(recipient->GetPageId());
  assert(child->GetParentPageId() == recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child->GetPageId(), true);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  SetKeyAt(size, key);
  SetValueAt(size, value);
  IncreaseSize(1);
  Page *page = buffer_pool_manager->FetchPage(value);
  assert(page != nullptr);
  BPlusTreePage *childPage = reinterpret_cast<BPlusTreePage *>(page->GetData());
  childPage->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  GenericKey* key = KeyAt(GetSize() - 1);
  page_id_t value = ValueAt(GetSize() - 1);
  recipient->SetKeyAt(0, middle_key);
  recipient->CopyLastFrom(key,value,buffer_pool_manager);
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() + 1 < GetMaxSize());
  // Shift the existing entries to make room for the new entry
  for (int i = GetSize() - 1; i >= 0; i--) {
    SetKeyAt(i+1, KeyAt(i));
    SetValueAt(i+1, ValueAt(i));
  }
  // Insert the new entry at the beginning
  SetKeyAt(0, nullptr);
  SetValueAt(0, value);
  
  // Update the parent page ID of the moved entry
  Page *page = buffer_pool_manager->FetchPage(value);
  assert(page != nullptr);
  BPlusTreePage *child = reinterpret_cast<BPlusTreePage *>(page->GetData());
  child->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);

  Page *parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
  BPlusTreeInternalPage *parent = reinterpret_cast<BPlusTreeInternalPage *>(parent_page->GetData());
  parent->SetKeyAt(parent->ValueIndex(GetPageId()), KeyAt(0));
  buffer_pool_manager->UnpinPage(parent_page->GetPageId(), true);

  IncreaseSize(1);
}