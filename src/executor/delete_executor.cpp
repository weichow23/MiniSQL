#include "executor/executors/delete_executor.h"

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  // 把table和indexes存储起来
  child_executor_->Init();
  std::string table_name = plan_->GetTableName();
  CatalogManager *catalog = exec_ctx_->GetCatalog();
  dberr_t ret = catalog->GetTable(table_name, table_info_);
  catalog->GetTableIndexes(table_name,index_info_);
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  // 如果后面还有用到，不能删除
  if(!child_executor_->Next(row, rid)) 
    return false;
  // MarkDelete
  // 删除失败
  if(!table_info_->GetTableHeap()->MarkDelete(row->GetRowId(), nullptr)) 
    return false;
  // 删索引
  for( auto index : index_info_ ){
      // 取出key
      vector<Field> key_contain;
      for( auto col : index->GetIndexKeySchema()->GetColumns() ){
          uint32_t col_index;
          table_info_->GetSchema()->GetColumnIndex(col->GetName(),col_index);
          key_contain.push_back(*(row->GetField(col_index)));
      }
      // 删除index
      index->GetIndex()->RemoveEntry(Row(key_contain),row->GetRowId(), nullptr);
  }
  row = nullptr;
  return true;
}