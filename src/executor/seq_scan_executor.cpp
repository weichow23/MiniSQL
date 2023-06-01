#include "executor/executors/seq_scan_executor.h"

/**
* TODO: Student Implement
*/
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan){}

void SeqScanExecutor::Init() {

}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
//  auto predicate = plan_->GetPredicate();
//  if(predicate == nullptr){
//    iterator++;
//  }
  return false;
}
