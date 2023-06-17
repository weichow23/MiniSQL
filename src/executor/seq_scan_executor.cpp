#include "executor/executors/seq_scan_executor.h"

SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
        : AbstractExecutor(exec_ctx),
          plan_(plan){}

void SeqScanExecutor::Init() {
    // 获取表名
    std::string table_name = plan_->GetTableName();
    // 获取执行上下文中的事务和目录管理器
    Transaction *txn = exec_ctx_->GetTransaction();
    CatalogManager *catalog = exec_ctx_->GetCatalog();
    // 检查表是否存在
    TableInfo *table_info = nullptr;
    dberr_t ret = catalog->GetTable(table_name, table_info);
    table_info_=table_info;
    // 获取表的迭代器
    TableHeap* table_heap = table_info->GetTableHeap();
    table_iter_ = table_heap->Begin(txn);
    end_ = table_heap->End();
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
    // 找符合条件的row
    while( table_iter_ != end_ ){
        // 如果返回的是kTypeInt的1，即正确
        if (  plan_->filter_predicate_ == nullptr ||
              Field(TypeId::kTypeInt,1).CompareEquals(plan_->filter_predicate_->Evaluate(&(*table_iter_))) ){
            vector<Field> output;output.clear();
            for( auto col : plan_->OutputSchema()->GetColumns() ){
                ::uint32_t col_index;
                table_info_->GetSchema()->GetColumnIndex(col->GetName(),col_index);
                output.push_back(*(*table_iter_).GetField(col_index));
            }
            *row = Row(output);
            auto t=*table_iter_;
            row->SetRowId((*table_iter_).GetRowId());
            ++table_iter_;
            *rid = row->GetRowId();
            return true;
        }
        else ++table_iter_;
    }
    return false;
}
