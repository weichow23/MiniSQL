#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUKReplacer(pool_size_);
  // 使用clockreplacer
  //replacer_ = new CLOCKReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/*根据逻辑页号获取对应的数据页，如果该数据页不在内存中，则需要从磁盘中进行读取；*/
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  frame_id_t frame_id;
  Page *page_ptr;
  auto page_itr = page_table_.find(page_id);
  // 找到page_id对应的frame_id
  if (page_itr != page_table_.end()){
    frame_id = page_itr->second;
    page_ptr = &pages_[frame_id];
    page_ptr->pin_count_++;
    replacer_->Pin(frame_id);
    return page_ptr;
  }
  // 如果page没有找到，则从空闲列表或替换器中找到替换页
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Victim(&frame_id)) {
    return nullptr;
  }
  // 把脏页写入磁盘
  page_ptr = &pages_[frame_id];
  if (page_ptr->IsDirty()) {
    disk_manager_->WritePage(page_ptr->page_id_, page_ptr->data_);
    page_ptr->is_dirty_ = false;
  }
  // 从page_table中删除 再插入
  page_table_.erase(page_ptr->page_id_);
  page_table_.insert(pair<page_id_t, frame_id_t>(page_id, frame_id));
  //更新 P 的元数据，从磁盘中读入页面内容，然后返回一个指向 P 的指针
  page_ptr->page_id_ = page_id;
  page_ptr->pin_count_++;
  replacer_->Pin(frame_id);
  disk_manager_->ReadPage(page_ptr->page_id_, page_ptr->data_);
  return page_ptr;
}

/*分配一个新的数据页，并将逻辑页号于page_id中返回*/
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 查看缓冲池中是否有空闲页（数Pin的个数）
  size_t i;
  frame_id_t frame_id;
  Page *page_ptr;
  for (i = 0; i < pool_size_ && pages_[i].GetPinCount() > 0; i++);
  if (i == pool_size_)
    return nullptr;
  // 在空闲列表或替代者中挑一个被替换的页面，首先从空闲列表中选择
  if (!free_list_.empty()){
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Victim(&frame_id)){
    return nullptr;
  }
  // 更新元信息
  page_ptr = &pages_[frame_id];
  if (page_ptr->IsDirty()) {
    disk_manager_->WritePage(page_ptr->page_id_, page_ptr->data_);
    page_ptr->is_dirty_ = false;
  }
  // 分配页
  auto new_page_id = AllocatePage();
  if(new_page_id == INVALID_PAGE_ID)
    return nullptr;
  page_table_.erase(page_ptr->page_id_);
  page_table_.insert(pair<page_id_t, frame_id_t>(new_page_id, frame_id));
  // 设置页面 ID 输出参数。 返回指向 P 的指针
  page_ptr->ResetMemory();
  page_ptr->page_id_ = new_page_id;
  page_ptr->pin_count_++;
  replacer_->Pin(frame_id);
  page_id = new_page_id;
  return page_ptr;
}

/*释放一个数据页*/
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  frame_id_t frame_id;
  Page *page_ptr;
  // 查找要删除的页
  auto page_itr = page_table_.find(page_id);
  if (page_itr == page_table_.end())
    return true;
  frame_id = page_itr->second;
  page_ptr = &pages_[frame_id];
  //页面还在使用中，不能被删除
  if (page_ptr->pin_count_ > 0)
    return false;
  // 页可以被删除
  if (page_ptr->IsDirty()) {
    disk_manager_->WritePage(page_ptr->page_id_, page_ptr->data_);
    page_ptr->is_dirty_ = false;
  }
  //从页表和磁盘中回收页
  page_table_.erase(page_ptr->page_id_);
  DeallocatePage(page_ptr->page_id_);
  // 修改元信息
  page_ptr->ResetMemory();
  page_ptr->page_id_ = INVALID_PAGE_ID;
  page_ptr->pin_count_ = 0;
  page_ptr->is_dirty_ = false;
  // 添加到free_list_
  free_list_.emplace_back(frame_id);
  return true;
}

/*取消固定一个数据页*/
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  frame_id_t frame_id;
  Page *page_ptr;
  auto page_itr = page_table_.find(page_id);
  if (page_itr == page_table_.end())
    return true;
  frame_id = page_itr->second;
  page_ptr = &pages_[frame_id];
  page_ptr->is_dirty_ = is_dirty;
  if (page_ptr->pin_count_ == 0)
    return false;
  else
    page_ptr->pin_count_--;
  if (page_ptr->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
    free_list_.emplace_back(frame_id);
  }
  return true;
}


/*将数据页转储到磁盘中*/
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  auto page_itr = page_table_.find(page_id);
  frame_id_t frame_id;
  Page *page_ptr;

  if (page_itr == page_table_.end()) return false;

  frame_id = page_itr->second;
  page_ptr = &pages_[frame_id];

  disk_manager_->WritePage(page_ptr->page_id_, page_ptr->data_);
  page_ptr->is_dirty_ = false;

  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}