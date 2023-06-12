//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/**
* TODO: Student Implement
*/
void UpdateExecutor::Init() {
    // 把table和indexes存储起来
    child_executor_->Init();
    GetExecutorContext()->GetCatalog()->GetTable(plan_->table_name_,table_info_);
    GetExecutorContext()->GetCatalog()->GetTableIndexes(plan_->table_name_,indexes_);
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
    // 先判断能不能取道row
    Row *src_row = new Row(), *update_row = new Row();
    if( !child_executor_->Next(src_row,nullptr))  return false;
    // 取得更新后的row
    update_row = new Row(GenerateUpdatedTuple(*src_row)) ;
    // 判断更新后的row有没有重复
    for( auto index : indexes_ ){
        // 取出key
        vector<Field> key_contain;
        for( auto col : index->GetIndexKeySchema()->GetColumns() ){
            uint32_t col_index;
            table_info_->GetSchema()->GetColumnIndex(col->GetName(),col_index);
            key_contain.push_back(*(update_row->GetField(col_index)));
        }
        vector<RowId> result;
        index->GetIndex()->ScanKey(Row(key_contain),result, nullptr);
        // 如果新的row有存在，就更新不了
        if( !(result.empty() || result[0] == src_row->GetRowId()) )  return false;
    }
    // 可以更新了
    // 先删后加
    table_info_->GetTableHeap()->MarkDelete(src_row->GetRowId(), nullptr);
    table_info_->GetTableHeap()->InsertTuple(*update_row, nullptr);
    // 索引更新
    for( auto index : indexes_ ){
        // 取出key
        vector<Field> remove_key;
        vector<Field> insert_key;
        for( auto col : index->GetIndexKeySchema()->GetColumns() ){
            uint32_t col_index;
            table_info_->GetSchema()->GetColumnIndex(col->GetName(),col_index);
            remove_key.push_back(*(src_row->GetField(col_index)));
            insert_key.push_back(*(update_row->GetField(col_index)));
        }
        // 删去之前的index
        index->GetIndex()->RemoveEntry(Row(remove_key), RowId(), nullptr);
        // 加上新的index
        index->GetIndex()->InsertEntry(Row(insert_key),update_row->GetRowId(), nullptr);
    }
    return true;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
    // 先copy一份之前的
    Row update_row(src_row);
    // update部分
    for( auto pair : plan_->GetUpdateAttr() ){
        // 用pair first来确定更改的索引，second来确定内容
        update_row.GetFields()[pair.first] = new Field(pair.second->Evaluate(nullptr));
    }
    return update_row;
}