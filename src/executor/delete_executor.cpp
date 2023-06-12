//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"

/**
* TODO: Student Implement
*/

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
    // 把table和indexes存储起来
    child_executor_->Init();
    GetExecutorContext()->GetCatalog()->GetTable(plan_->table_name_,table_info_);
    GetExecutorContext()->GetCatalog()->GetTableIndexes(plan_->table_name_,indexes_);
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
    // 删除可以先删row后删index
    // row若有index肯定有
    // 先提取row
    Row* delete_row = new Row();
    // 如果还有
    if( child_executor_->Next(delete_row,nullptr) ){
        // 决定只mark，具体没考虑太多
        // 删除失败也false
        if(!table_info_->GetTableHeap()->MarkDelete(delete_row->GetRowId(), nullptr)) return false;
        // 删索引，遍历索引
        for( auto index : indexes_ ){
            // 取出key
            vector<Field> key_contain;
            for( auto col : index->GetIndexKeySchema()->GetColumns() ){
                uint32_t col_index;
                table_info_->GetSchema()->GetColumnIndex(col->GetName(),col_index);
                key_contain.push_back(*(delete_row->GetField(col_index)));
            }
            // 删
            // 第二个参数吊用没有为啥要有
            index->GetIndex()->RemoveEntry(Row(key_contain),delete_row->GetRowId(), nullptr);
        }
        // 收
        return true;
    } else  return false;
}