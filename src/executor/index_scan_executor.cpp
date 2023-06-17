#include "executor/executors/index_scan_executor.h"

IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
        : AbstractExecutor(exec_ctx), plan_(plan) {result_rid.clear();result_row.clear();}
// 递归地dfs遍历子节点来寻找比较的项
void dfs(vector<ComparisonExpression*>& vec, AbstractExpressionRef curr){
    //只有and的情况
    if(curr.get()->GetType() == ExpressionType::LogicExpression){
        for( auto child : curr->GetChildren() ){
            dfs(vec,child); //遍历每个字节点
        }
    }
    // 如果是compare类型的，就压入用于分析
    else if(curr.get()->GetType() == ExpressionType::ComparisonExpression){
        vec.emplace_back(dynamic_cast<ComparisonExpression*>(curr.get()));
        return;
    }
    else{
        throw "Scan for index error";
    }
}

bool cmp(const RowId& A, const RowId& B){
    return A.Get() < B.Get();
}

void IndexScanExecutor::Init() {
    // 获取表名, 目录管理器
    std::string table_name = plan_->GetTableName();
    CatalogManager *catalog = exec_ctx_->GetCatalog();
    // 检查表是否存在
    TableInfo *table_info = nullptr;
    auto predicate_root = plan_->GetPredicate();
    vector<ComparisonExpression*> predicates(0);
    dfs(predicates, predicate_root);
    // 遍历索引
    for (auto predicate : predicates){
        auto col_expr = dynamic_cast<ColumnValueExpression*>(predicate->GetChildAt(0).get());
        for (auto index : plan_->indexes_){
            auto col_id = index->GetIndexKeySchema()->GetColumn(0)->GetTableInd() - 1;
            // 找到索引对应的条件，并存到fields中
            vector<Field> fields;
            fields.clear();
            fields.emplace_back(Field(predicate->GetChildAt(1)->Evaluate(nullptr)));
            Row key(fields);
            vector<RowId> result_temp(0);
            if(col_expr->GetColIdx() == col_id){
                index->GetIndex()->ScanKey(key, result_temp, nullptr, predicate->GetComparisonType());
                //与之前的结果求交集
                vector<RowId> temp; // 创建暂存向量
                // 对要处理的两个向量进行排序
                std::sort(result_rid.begin(), result_rid.end(), cmp);
                std::sort(result_temp.begin(), result_temp.end(), cmp);
                // 取交集
                set_intersection(result_rid.begin(),result_rid.end(),result_temp.begin(),result_temp.end(),insert_iterator<vector<RowId>>(temp,temp.begin()), cmp);
                result_rid.clear();
                result_rid = temp;
            }
        }
    }
    vector<RowId> temp(0);
    for(int i = 0; i < result_rid.size(); i++){
        auto row = Row(result_rid[i]);
        table_info->GetTableHeap()->GetTuple(&row, nullptr);
        //最后用所有谓词扫描一遍
        if(!plan_->need_filter_ || plan_->GetPredicate()->Evaluate(&row).CompareEquals(Field(kTypeInt, 1)) == kTrue){
            result_row.emplace_back(row);
            temp.emplace_back(result_rid[i]);
        }
    }
    result_rid.swap(temp);
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
    // 找下一个节点
    if (cursor_ < result_rid.size()){
        *row = result_row[cursor_];
        *rid = result_rid[cursor_];
        cursor_++;
    }
    return false;
}
