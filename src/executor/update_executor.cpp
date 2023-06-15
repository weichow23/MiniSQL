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
    child_executor_->Init();
    std::string table_name = plan_->GetTableName();
    CatalogManager *catalog = exec_ctx_->GetCatalog();
    dberr_t ret = catalog->GetTable(table_name, table_info_);
    catalog->GetTableIndexes(table_name,index_info_);
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
    if(!child_executor_->Next(row, rid))
        return false;
    TableHeap* table_heap = table_info_->GetTableHeap();
    //获取新元组
    Row* new_row = new Row(GenerateUpdatedTuple(*row));
    new_row->SetRowId(row->GetRowId());
    //修改表
    table_heap->UpdateTuple(*new_row,*rid, nullptr);
    //修改索引

    for( auto index : index_info_ ){
        // 取出key
        vector<Field> remove_key;
        vector<Field> insert_key;
        for( auto col : index->GetIndexKeySchema()->GetColumns() ){
            uint32_t col_index;
            table_info_->GetSchema()->GetColumnIndex(col->GetName(),col_index);
            remove_key.push_back(*(row->GetField(col_index)));
            insert_key.push_back(*(new_row->GetField(col_index)));
        }
        // 删去之前的index
        index->GetIndex()->RemoveEntry(Row(remove_key), RowId(), nullptr);
        // 加上新的index
        index->GetIndex()->InsertEntry(Row(insert_key), new_row->GetRowId(), nullptr);
    }
    row = nullptr;
    return true;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
    auto UpdatedAddr = plan_->GetUpdateAttr();
    Row row(src_row);
    auto fields = row.GetFields();
    vector<Field> new_fields;
    //fields.clear();
    for(int i = 0; i < fields.size(); i++)
        if(UpdatedAddr.count(i)) //如果此属性需要被更改
            new_fields.emplace_back(UpdatedAddr[i]->Evaluate(&row));
        else
            new_fields.emplace_back(*row.GetField(i));
    return Row(new_fields);
}