#include "executor/executors/delete_executor.h"

/**
* TODO: Student Implement
*/

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
        : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
    child_executor_->Init();
    std::string table_name = plan_->GetTableName();
    CatalogManager *catalog = exec_ctx_->GetCatalog();
    dberr_t ret = catalog->GetTable(table_name, table_info_);
    catalog->GetTableIndexes(table_name,index_info_);
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
    if(!child_executor_->Next(row, rid))
        return false;
    TableHeap* table_heap = table_info_->GetTableHeap();
    // ApplyDelete 还是 MarkDelete？
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
        // 删除
        index->GetIndex()->RemoveEntry(Row(key_contain),row->GetRowId(), nullptr);
    }
    row = nullptr;
    return true;
}