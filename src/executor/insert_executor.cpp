#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
        : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
    child_executor_->Init();
    // 把catalog
    CatalogManager *catalog = exec_ctx_->GetCatalog();
    dberr_t ret = catalog->GetTable(plan_->table_name_, table_info_);
    catalog->GetTableIndexes(plan_->table_name_,index_info_);
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
    Row* insert = new Row();
    // 已经存完了
    if( !child_executor_->Next(insert, nullptr))
        return false;
    // 先判断插入值是否已经存在
    for( auto index : index_info_ ){
        // 取出key
        vector<Field> key_contain;
        for( auto col : index->GetIndexKeySchema()->GetColumns() ){
            uint32_t col_index;
            table_info_->GetSchema()->GetColumnIndex(col->GetName(),col_index);
            key_contain.push_back(*(insert->GetField(col_index)));
        }
        // 判断index是否存在
        std::vector<RowId> result(0);
        index->GetIndex()->ScanKey(Row(key_contain),result, nullptr);
        if (!result.empty()){
            return false;
        }
    }
    // 要插入的值不存在
    table_info_->GetTableHeap()->InsertTuple(*insert, nullptr);
    // 更新index
    for( auto index : index_info_ ){
        // 取出key
        vector<Field> key_contain;
        for( auto col : index->GetIndexKeySchema()->GetColumns() ){
            uint32_t col_index;
            table_info_->GetSchema()->GetColumnIndex(col->GetName(),col_index);
            key_contain.push_back(*(insert->GetField(col_index)));
        }
        // 更新index
        index->GetIndex()->InsertEntry(Row(key_contain),insert->GetRowId(), nullptr);
    }

    return true;
}