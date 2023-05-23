#include "catalog/catalog.h"
#include <algorithm>

void CatalogMeta::SerializeTo(char *buf) const {
    ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto iter : table_meta_pages_) {
        MACH_WRITE_TO(table_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    for (auto iter : index_meta_pages_) {
        MACH_WRITE_TO(index_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
    // check valid
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++) {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++) {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    return meta;
}

/* yf */
//uint32_t CatalogMeta::GetSerializedSize() const {
//  ASSERT(false, "Not Implemented yet");
//  return 0;
//}
uint32_t CatalogMeta::GetSerializedSize() const {
  // ASSERT(false, "Not Implemented yet");
  return sizeof(CATALOG_METADATA_MAGIC_NUM) + sizeof(decltype(index_meta_pages_.size())) * 2 +
         index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t)) +
         table_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t));
}

CatalogMeta::CatalogMeta() {}

/* yf */
//CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
//                               LogManager *log_manager, bool init)
//    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
////    ASSERT(false, "Not Implemented yet");
//}
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager),
      lock_manager_(lock_manager),
      log_manager_(log_manager) {
  if (!init) {
        // 需要重新加载数据
        // 反序列化，获取catalog_meta
        auto catalog_page_raw = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
        catalog_meta_ = CatalogMeta::DeserializeFrom(reinterpret_cast<char *>(catalog_page_raw->GetData()));

        next_index_id_ = catalog_meta_->GetNextIndexId();
        next_table_id_ = catalog_meta_->GetNextTableId();
        // load tables and indexs
        for_each(catalog_meta_->table_meta_pages_.begin(), catalog_meta_->table_meta_pages_.end(),
                 [&](auto &it) -> void { LoadTable(it.first, it.second); });
        for_each(catalog_meta_->index_meta_pages_.begin(), catalog_meta_->index_meta_pages_.end(),
                 [&](auto &it) -> void { LoadIndex(it.first, it.second); });
  } else {
        // 全新的数据库
        next_table_id_ = next_index_id_ = 0;
        catalog_meta_ = CatalogMeta::NewInstance();
  }
}

CatalogManager::~CatalogManager() {
 /** After you finish the code for the CatalogManager section,
 *  you can uncomment the commented code. Otherwise it will affect b+tree test
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
  **/
}

/* gg */
//dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
//                                    Transaction *txn, TableInfo *&table_info) {
//  // ASSERT(false, "Not Implemented yet");
//  return DB_FAILED;
//}
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  if (table_names_.find(table_name) != table_names_.end()) return DB_TABLE_ALREADY_EXIST;
  /* Get table id*/
  auto table_id = catalog_meta_->GetNextTableId();
  table_names_[table_name] = table_id;  // update table_names_
  /*New a page for table meta data*/
  page_id_t page_id;
  auto table_meta_page = buffer_pool_manager_->NewPage(page_id);
  catalog_meta_->table_meta_pages_[table_id] = page_id;
  /*Initialize a new table heap*/
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_);
  /*Construct table meta data*/
  auto table_meta = TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), schema);
  table_meta->SerializeTo(table_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(page_id, true);
  table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);
  tables_[table_id] = table_info;  // update tables_
  return DB_SUCCESS;
}

/* yf */
//dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
//  // ASSERT(false, "Not Implemented yet");
//  return DB_FAILED;
//}
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // ASSERT(false, "Not Implemented yet");
  auto temp = table_names_.find(table_name);
  if (temp == table_names_.end()) {
        return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_.find(temp->second)->second;
  return DB_SUCCESS;
}

/* yf */
//dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
//  // ASSERT(false, "Not Implemented yet");
//  return DB_FAILED;
//}
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  for (auto &it : tables_) {
        tables.push_back(it.second);
  }
  return DB_SUCCESS;
}

/* gg */
//dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
//                                    const std::vector<std::string> &index_keys, Transaction *txn,
//                                    IndexInfo *&index_info, const string &index_type) {
//  // ASSERT(false, "Not Implemented yet");
//  return DB_FAILED;
//}
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  // ASSERT(false, "Not Implemented yet");
  auto temp = table_names_.find(table_name);
  if (temp == table_names_.end()) {
        return DB_TABLE_NOT_EXIST;
  }
  auto judge = index_names_.find(table_name);
  if (judge != index_names_.end() && judge->second.find(index_name) != judge->second.end()) {
        return DB_INDEX_ALREADY_EXIST;
  }

  auto table_info = tables_.find(temp->second);
  auto *schema = table_info->second->GetSchema();
  bool tag = false;
  for (auto &it : index_keys) {
        uint32_t index__ = 0;
        auto result = schema->GetColumnIndex(it, index__);
        if (result != DB_SUCCESS) {
          return result;
        }
        if (schema->GetColumn(index__)->IsUnique()) {
          tag = true;
          break;
        }
  }
  if (!tag) {
        //如果没有唯一属性，那么必须在主键上建立索引
        // __primary
        if (index_name.length() < 9) {
          std::cout << "Cannot index on non-unique properties" << std::endl;
          return DB_FAILED;
        }
        string check_str = index_name.substr(index_name.length() - 9, index_name.length());
        if (check_str != "__primary") {
          std::cout << "Cannot index on non-unique properties" << std::endl;
          return DB_FAILED;
        }
  }
  std::vector<uint32_t> keys(index_keys.size());
  for (size_t i = 0; i < index_keys.size(); i++) {
        if (schema->GetColumnIndex(index_keys[i], keys[i]) == DB_COLUMN_NAME_NOT_EXIST) {
          return DB_COLUMN_NAME_NOT_EXIST;
        }
  }
  //遍历已有的所有index，查看是否有完全相同的索引
  for (auto &it : indexes_) {
        auto index_schema = it.second->GetIndexKeySchema();
        if (index_schema->GetColumnCount() == keys.size()) {
          bool flag = true;
          auto cols = index_schema->GetColumns();
          for (uint32_t i = 0; flag && i < keys.size(); i++) {
            if (keys[i] != cols[i]->GetTableInd()) {
              flag = false;
            }
          }
          if (flag) {
            //完全相同
            return DB_INDEX_ALREADY_EXIST;
          }
        }
  }
  auto index_meta = IndexMetadata::Create(next_index_id_, index_name, temp->second, keys);

  index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info->second, buffer_pool_manager_);

  indexes_.emplace(next_index_id_, index_info);
  auto find_table_all_index_map = index_names_.find(table_name);
  if (find_table_all_index_map == index_names_.end()) {
        // 创建新的map
        std::unordered_map<std::string, index_id_t> newMap;
        newMap.emplace(index_name, next_index_id_++);
        index_names_.emplace(table_name, newMap);
  } else {
        (index_names_.find(table_name)->second).emplace(index_name, next_index_id_++);
  }
  //插入表中的数据
  auto table_heap = table_info->second->GetTableHeap();
  vector<Field> f;
  for (auto it = table_heap->Begin(nullptr); it != table_heap->End(); it++) {
        f.clear();
        for (auto pos : keys) {
          f.push_back(*(it->GetField(pos)));
        }
        Row row(f);
        index_info->GetIndex()->InsertEntry(row, it->GetRowId(), nullptr);
  }
  next_index_id_++;
  //将新的index写入磁盘中
  page_id_t new_index_page_id = INVALID_PAGE_ID;
  auto new_index_page = buffer_pool_manager_->NewPage(new_index_page_id);
  assert(new_index_page != nullptr);
  index_meta->SerializeTo(reinterpret_cast<char *>(new_index_page));

  catalog_meta_->index_meta_pages_.emplace(index_meta->GetIndexId(), new_index_page_id);

  buffer_pool_manager_->UnpinPage(new_index_page_id, true);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  return FlushCatalogMetaPage();
}



/* yf */
//dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
//                                 IndexInfo *&index_info) const {
//  // ASSERT(false, "Not Implemented yet");
//  return DB_FAILED;
//}
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  auto index_vec = index_names_.find(table_name);
  if (index_vec == index_names_.end()) {
        return DB_TABLE_NOT_EXIST;
  }
  auto index = index_vec->second.find(index_name);
  if (index == index_vec->second.end()) {
        return DB_INDEX_NOT_FOUND;
  }
  index_info = indexes_.find(index->second)->second;
  return DB_SUCCESS;
}

/* yf */
//dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
//  // ASSERT(false, "Not Implemented yet");
//  return DB_FAILED;
//}
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  auto index_vec = index_names_.find(table_name);
  if (index_vec == index_names_.end()) {
        return DB_INDEX_NOT_FOUND;
  }
  indexes.clear();
  for (auto &it : index_vec->second) {
        indexes.push_back(indexes_.find(it.second)->second);
  }
  return DB_SUCCESS;
}

/* yf */
//dberr_t CatalogManager::DropTable(const string &table_name) {
//  // ASSERT(false, "Not Implemented yet");
//  return DB_FAILED;
//}
dberr_t CatalogManager::DropTable(const string &table_name) {
  auto temp = table_names_.find(table_name);
  if (temp == table_names_.end()) {
        return DB_TABLE_NOT_EXIST;
  }
  // 删除对应的记录
  auto table_info = tables_.find(temp->second);
  assert(table_info != tables_.end());
  // 改变metapage
  catalog_meta_->table_meta_pages_.erase(temp->second);
  // 修改内存中的map
  tables_.erase(temp->second);
  table_names_.erase(table_name);
  return FlushCatalogMetaPage();
}

/* yf */
//dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
//  // ASSERT(false, "Not Implemented yet");
//  return DB_FAILED;
//}
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  auto temp = index_names_.find(table_name);
  if (temp == index_names_.end()) {
        return DB_TABLE_NOT_EXIST;
  }
  if ((temp->second.find(index_name) == temp->second.end())) {
        return DB_INDEX_NOT_FOUND;
  }
  index_id_t index_id = temp->second.find(index_name)->second;
  indexes_.erase(index_id);
  (index_names_[table_name]).erase(index_name);
  catalog_meta_->index_meta_pages_.erase(index_id);
  return FlushCatalogMetaPage();
}

/* yf */
//dberr_t CatalogManager::FlushCatalogMetaPage() const {
//  // ASSERT(false, "Not Implemented yet");
//  return DB_FAILED;
//}
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  auto CatalogMetaPage = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  assert(CatalogMetaPage != nullptr);
  catalog_meta_->SerializeTo(reinterpret_cast<char *>(CatalogMetaPage->GetData()));
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  return DB_SUCCESS;
}

/* yf */
//dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
//  // ASSERT(false, "Not Implemented yet");
//  return DB_FAILED;
//}
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  if (tables_.count(table_id) != 0) {
        return DB_SUCCESS;
  }
  auto catalog_meta_page_raw = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  assert(catalog_meta_page_raw != nullptr);
  auto table_page_raw = buffer_pool_manager_->FetchPage(page_id);
  TableMetadata *table_page = nullptr;
  TableMetadata::DeserializeFrom(reinterpret_cast<char *>(table_page_raw->GetData()), table_page);
  table_names_.emplace(table_page->GetTableName(), table_id);
  TableInfo *table_info = TableInfo::Create();
  TableHeap *tableheap = TableHeap::Create(buffer_pool_manager_, table_page->GetFirstPageId(), table_page->GetSchema(),
                                           nullptr, nullptr);
  table_info->Init(table_page, tableheap);
  tables_.emplace(table_id, table_info);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

/* yf */
//dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
//  // ASSERT(false, "Not Implemented yet");
//  return DB_FAILED;
//}
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  if (indexes_.count(index_id) != 0) {
        return DB_SUCCESS;
  }
  auto catalog_meta_page_raw = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  assert(catalog_meta_page_raw != nullptr);
  auto index_page_raw = buffer_pool_manager_->FetchPage(page_id);
  IndexMetadata *index_page = nullptr;
  IndexMetadata::DeserializeFrom(reinterpret_cast<char *>(index_page_raw->GetData()), index_page);
  assert(index_page != nullptr);
  auto index_info = IndexInfo::Create();
  TableInfo *tableinfo = nullptr;
  GetTable(index_page->GetTableId(), tableinfo);
  assert(tableinfo != nullptr);
  index_info->Init(index_page, tableinfo, buffer_pool_manager_);
  indexes_.emplace(index_id, index_info);
  auto temp = index_names_.find(index_info->GetTableInfo()->GetTableName());
  if (temp == index_names_.end()) {
        std::unordered_map<std::string, index_id_t> newMap;
        newMap.emplace(index_info->GetIndexName(), index_id);
        index_names_.emplace(index_info->GetTableInfo()->GetTableName(), newMap);
  } else {
        temp->second.emplace(index_info->GetIndexName(), index_id);
  }
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return DB_SUCCESS;
}

/* yf */
//dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
//  // ASSERT(false, "Not Implemented yet");
//  return DB_FAILED;
//}
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto temp = tables_.find(table_id);
  if (temp == tables_.end()) {
        return DB_TABLE_NOT_EXIST;
  }
  table_info = temp->second;
  return DB_SUCCESS;
}