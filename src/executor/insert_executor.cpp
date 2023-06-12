//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
    child_executor_->Init();
    // 把table和indexes存储起来
    GetExecutorContext()->GetCatalog()->GetTable(plan_->table_name_,table_info_);
    GetExecutorContext()->GetCatalog()->GetTableIndexes(plan_->table_name_,indexes_);
}

bool if_contain_key( IndexInfo* index_info, const Row &key ){
    // 先找载体
    std::vector<RowId> result;result.clear();
    index_info->GetIndex()->ScanKey(key,result, nullptr);
    return !result.empty();
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
    Row* insert = new Row();
    // 如果存完了就结束
    if( !child_executor_->Next(insert, nullptr) )   return false;
    // 存
    else{
        // 把index先存进去
        // 存进去前先判断index是不是有了
        for( auto index : indexes_ ){
            // 取出key
            vector<Field> key_contain;
            for( auto col : index->GetIndexKeySchema()->GetColumns() ){
                uint32_t col_index;
                table_info_->GetSchema()->GetColumnIndex(col->GetName(),col_index);
                key_contain.push_back(*(insert->GetField(col_index)));
            }
            // 判断index是否有
            if(if_contain_key(index,Row(key_contain))){
                // 如果有，就存入失败
                return false;
            }
        }
        // 此时index都过了
        // 先存进去
        table_info_->GetTableHeap()->InsertTuple(*insert, nullptr);
        // 更新index
        for( auto index : indexes_ ){
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
        // 结束
        return true;
    }
}