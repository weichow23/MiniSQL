#include "executor/executors/index_scan_executor.h"
#include <algorithm>
/**
* TODO: Student Implement
*/
IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

// 遍历子节点来寻找比较的项
void dfs_predict( AbstractExpressionRef root, unordered_map<string,Field*> &compare, unordered_map<string,string> conditions, Schema* schema ){
    // 如果是compare类型的，就分析
    if( root->GetType() == ExpressionType::ComparisonExpression ){
        // 转化为ComparisionExpression类型
        ComparisonExpression* temp = (ComparisonExpression*)(root.get());
        string col_name;
        Field* compare_field;
        string condition = temp->GetComparisonType();
        // 获得子节点的信息
        for( auto child : temp->GetChildren() ){
            if( child->GetType() == ExpressionType::ConstantExpression ){
                compare_field =new Field(child->Evaluate(nullptr)) ;
            }
            if( child->GetType() == ExpressionType::ColumnExpression ){
                col_name = schema->GetColumn(((ColumnValueExpression*)(child.get()))->GetColIdx())->GetName();
            }
        }
        // 存储
        compare[col_name] = compare_field;
        conditions[col_name] = condition;
    }
    // 否则就继续搜
    else{
        for( auto child : root->GetChildren() ){
            dfs_predict(child,compare ,conditions,schema);
        }
    }
}

// 自定义的rowid排序
bool compare_RowId( RowId a, RowId b ){
    if( a.GetPageId() != b.GetPageId() ){
        return a.GetPageId() < b.GetPageId();
    }
    else    return a.GetSlotNum() < b.GetSlotNum();
}

void IndexScanExecutor::Init() {
    auto disk_manage = GetExecutorContext()->GetBufferPoolManager();
    ::uint32_t  index_size = plan_->indexes_.size();
    TableInfo* now_table = nullptr;
    GetExecutorContext()->GetCatalog()->GetTable(plan_->table_name_,now_table);
    // 遍历谓词树来找到所有的条件
    unordered_map<string,Field*> compare;
    unordered_map<string,string> conditions;
    dfs_predict(plan_->GetPredicate(),compare,conditions,now_table->GetSchema());
    vector<RowId> result_row[index_size];
    // 遍历索引
    for( ::uint32_t i=0; i<index_size; i++ ){
        // 找到索引对应的条件，并存到result_row中
        auto index = plan_->indexes_[i];
        string col_name = index->GetIndexKeySchema()->GetColumn(0)->GetName();
        vector<Field> keys = {Field(*compare[col_name])};
        Row key(keys);
        index->GetIndex()->ScanKey(key,result_row[i], nullptr,conditions[col_name]);
    }
    // 如果得到的最后的rowid集大于1个，则求交集
    if( index_size > 1 ){
        for( uint32_t i=1; i<index_size; i++ ){
            // 创建暂存向量
            vector<RowId> temp;
            // 对要处理的两个向量进行排序
            std::sort(result_row[0].begin(), result_row[0].end(), compare_RowId);
            std::sort(result_row[i].begin(),result_row[i].end(), compare_RowId);
            // 取交集
            set_intersection(result_row[0].begin(),result_row[0].end(),result_row[i].begin(),result_row[i].end(),insert_iterator<vector<RowId>>(temp,temp.begin()), compare_RowId);
            // 结果存储到result_row[0]中去
            result_row[0].clear();
            result_row[0] = temp;
        }
    }
    // 最后初始化结果
    contain_ = result_row[0];
    now_ = contain_.begin();
    end_ = contain_.end();
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
    TableInfo* now_table = nullptr;
    // 得到table来获取信息
    GetExecutorContext()->GetCatalog()->GetTable(plan_->table_name_,now_table);
    // 找下一个节点
    while( now_ != end_ ){
        // 把节点地址信息存进去
        Row* now_row = new Row(*now_);
        now_table->GetTableHeap()->GetTuple(now_row, nullptr);
        // 如果need_filter就还需要判断一下，否则就可以直接放行
        if( !plan_->need_filter_ || Field(TypeId::kTypeInt,1).CompareEquals(plan_->GetPredicate()->Evaluate(now_row)) ){
            row = new Row(*now_row);
            rid = new RowId(now_row->GetRowId());
            break;
        }
    }
    if( now_ == end_ )  return false;
    else{
        now_++;
        return true;
    }
}
