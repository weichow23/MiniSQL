#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>
#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"
#include <algorithm>



// @wjf 2023.6.3 太折磨了，要死了，一大堆代码一环套一环的，看了两天才看懂什么是什么

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** After you finish the code for the CatalogManager section,
   *  you can uncomment the commented code.   **/
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }

  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
      std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
      if (result_set != nullptr) {
          result_set->clear();
      }
      return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if(!current_db_.empty())
    context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
      // 去重
      std::sort(result_set.begin(), result_set.end());
      auto iter = std::unique(result_set.begin(), result_set.end());
      result_set.erase(iter, result_set.end());
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
      case DB_FAILED:
          cout << "Something unexpected happens." << endl;
          break;
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}
/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
    // get the database name
    ast = ast->child_;
    string DBname(ast->val_);
    auto it = dbs_.find(DBname);
    if( it != dbs_.end() )  return DB_ALREADY_EXIST;
    auto db = new DBStorageEngine(DBname);
    // 如果建立失败
    if( db == nullptr ) return  DB_FAILED;
    dbs_[DBname] = db;
    cout << "Database " << DBname << " has been established !" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
    // get the drop database name
    ast = ast->child_;
    string DBname(ast->val_);
    // 判断是不是当前正在打开的database
    // 如果是，就重置当前的数据库
    if( DBname == current_db_ ) current_db_ = "";
    auto it = dbs_.find(DBname);
    // 如果没有找到db
    if( it == dbs_.end() )  return  DB_NOT_EXIST;
    // 找到的话就把他删除
    it->second->RemoveDBStorageEngine();
    dbs_.erase(it);
    cout << "Database " << DBname << " has been dropped !" << endl;
 return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
    vector<string> Databases;
    Databases.clear();
    ::uint32_t longest = 0;
    // 遍历
    for( auto database: dbs_ ) {
        string temp(database.first);
        // 找到最长的那个名称
        if (longest < temp.length()) longest = temp.length();
        Databases.push_back(temp);
    }
    // 按格式输出
    string top = "+" + string(longest + 2, '-') + "+";
    cout << "there are " << dbs_.size() << " databases" << endl;
    if(dbs_.size()) {
        cout << top << endl;
        for( string db : Databases ){
            cout << "| ";
            cout << setw(longest) << left << db;
            cout << " |" << endl;
        }
        cout << top << endl;
    }
    Databases.clear();
    return  DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
    // 得到名字
    string DBname(ast->child_->val_);
    // 更新
    auto it = dbs_.find(DBname);
    // 确定这个dababase是不是在
    if( it == dbs_.end() )  return DB_NOT_EXIST;
    // 或者更新
    current_db_ = DBname;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
    // 得到当前的数据库
    auto it = dbs_.find(current_db_);
    // 检验
    if( it == dbs_.end() )  return DB_NOT_EXIST;
    // 找到所有的tables
    vector<TableInfo *> Tables;
    vector<string> TableName;
    uint32_t longest = 0;
    TableName.clear();
    Tables.clear();
    it->second->catalog_mgr_->GetTables(Tables);
    for( auto table : Tables ){
        if( table->GetTableName().length() > longest )  longest = table->GetTableName().length();
        TableName.push_back(table->GetTableName());
    }
    // 按格式输出
    string top = "+" + string(longest + 2, '-') + "+";
    cout << "there are " << TableName.size() << " tables" << endl;
    if(TableName.size()) {
        cout << top << endl;
        for( string name : TableName ){
            cout << "| ";
            cout << setw(longest) << left << name;
            cout << " |"  << endl;
        }
        cout << top << endl;
    }
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */


dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
    // 得到当前的数据库
    auto it = dbs_.find(current_db_);
    // 检验
    if( it == dbs_.end() )  return DB_NOT_EXIST;
    // 得到名字和创建新的tableinfo
    string table_name(ast->child_->val_);
    TableInfo* table_info = nullptr;
    dberr_t if_exist = it->second->catalog_mgr_->GetTable(table_name,table_info);
    if( if_exist == DB_SUCCESS )    return  DB_TABLE_ALREADY_EXIST;

    pSyntaxNode ColumnDefineList = ast->child_->next_;
    pSyntaxNode ColumnDefine = ColumnDefineList->child_;

    // 存信息的map
    vector<string> columns_name;
    columns_name.clear();
    unordered_map<string , bool> if_unique;
    unordered_map<string, bool> if_primary_key;
    unordered_map<string, string> type_of_column;
    unordered_map<string, int> char_size;
    vector<string> uni_keys;
    vector<string> pri_keys;

    // 下面开始定义table里的属性
    // 只有nullptr和非kNodekNodeColumnDefinition的时候结束
    while( ColumnDefine != nullptr && ColumnDefine->type_ == kNodeColumnDefinition ){
        // 开始定义
        // 找到type和name
        string col_name(ColumnDefine->child_->val_);
        string type(ColumnDefine->child_->next_->val_);

        columns_name.push_back(col_name);
        type_of_column[col_name] = type;
        // 看看val存不存在
        if( ColumnDefine->val_ != nullptr ) {
            if_unique[col_name] = true;
            uni_keys.push_back(col_name);
        }
        if( type == "char" ) {
            string temp(ColumnDefine->child_->next_->child_->val_);
            char_size[col_name] = ::atoi(temp.data());
            if( char_size[col_name] <= 0 )  return DB_FAILED;
        }
        ColumnDefine = ColumnDefine->next_;
    }

    // 判断有没有primary key
    if( ColumnDefine != nullptr ){
        // 开始加primary key
        pSyntaxNode primary_key = ColumnDefine->child_;
        while( primary_key != nullptr ){
            if( primary_key->type_ != kNodeIdentifier )   return  DB_FAILED;
            pri_keys.push_back(primary_key->val_);
            if_primary_key[primary_key->val_] = true;
            primary_key = primary_key->next_;
        }
    }

    // 开始建表
    vector<Column*> column_list;column_list.clear();
    ::uint32_t column_index = 0;
    for( string column_name : columns_name ){
        // 创建新的Column结构
        Column* new_column;
        // 判断类型
        if( type_of_column[column_name] == "int" ){
            // 如果是主键或者标注了unique的话
//            if( if_unique[column_name] || if_primary_key[column_name] ){
//                new_column = new Column(column_name,TypeId::kTypeInt,column_index, false ,true);
//            }
//            else{
//                new_column = new Column(column_name,TypeId::kTypeInt,column_index, false , false);
//            }
            new_column = new Column(column_name,TypeId::kTypeInt,column_index, false, if_unique[column_name]);
        }
        else if( type_of_column[column_name] == "float" ){
            // 同上
//            if( if_unique[column_name] || if_primary_key[column_name] ){
//                new_column = new Column(column_name,TypeId::kTypeFloat,column_index, false ,true);
//            }
//            else{
//                new_column = new Column(column_name,TypeId::kTypeFloat,column_index, false , false);
//            }
            new_column = new Column(column_name,TypeId::kTypeFloat,column_index, false, if_unique[column_name]);
        }
        else if( type_of_column[column_name] == "char" ){
            // 同上，但是要加上char的长度
//            if( if_unique[column_name] || if_primary_key[column_name] ){
//                new_column = new Column(column_name,TypeId::kTypeChar, char_size[column_name],column_index, false ,true);
//            }
//            else{
//                new_column = new Column(column_name,TypeId::kTypeChar, char_size[column_name],column_index, false ,
//                                        false);
//            }
            new_column = new Column(column_name,TypeId::kTypeChar, char_size[column_name],column_index, false, if_unique[column_name]);
        }
        // 其他的无法识别
        else{
            return DB_FAILED;
        }
        // 列的序列号增加，并压入vector中
        column_index ++ ;
        column_list.push_back(new_column);
    }

    // 下面开始创建schema
    Schema* new_schema = new Schema(column_list);
    TableInfo* new_table = nullptr;
    // 创建table
    dberr_t create_table = it->second->catalog_mgr_->CreateTable(table_name,new_schema, nullptr,new_table);
    // 如果没有成功
    if( create_table != DB_SUCCESS ) return create_table;

    // 为unique添加index
    for( string unique_name : uni_keys ){
        string index_name = table_name + "_" + unique_name + "_unique";
        vector<string> index_column_name = {unique_name};
        IndexInfo * new_index = nullptr;
        dberr_t index_create = it->second->catalog_mgr_->CreateIndex(table_name,index_name,index_column_name, nullptr,new_index,
                                                                     "");
        if( index_create != DB_SUCCESS ) return index_create;
    }

    // 开始处理主键
    vector<string> index_column_name;
    index_column_name.clear();
    for( string column_name:columns_name ){
        if( if_primary_key[column_name] ){
            index_column_name.push_back(column_name);
        }
    }
    string index_name = table_name + "_primary_key";
    IndexInfo* new_index = nullptr;
    // index type并未使用
    // 创建index
    dberr_t index_create = it->second->catalog_mgr_->CreateIndex(table_name,index_name,index_column_name, nullptr,new_index, "");
    return index_create;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
    // 得到要删除的table名
    string table_name(ast->child_->val_);
    // 判断db是否存在
    if( dbs_.find(current_db_) == dbs_.end() )  return DB_FAILED;
    // 直接调用
    return dbs_.find(current_db_)->second->catalog_mgr_->DropTable(table_name);
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
    // 判断db是否存在
    if( dbs_.find(current_db_) == dbs_.end() )  return DB_FAILED;
    vector<TableInfo *> tables;
    vector<IndexInfo *> indexes;
    // 得到所有的tables
    dbs_.find(current_db_)->second->catalog_mgr_->GetTables(tables);
    // 准备输出
    vector<string> index_names;
    uint32_t longest = 0;
    index_names.clear();

    for(auto table : tables) {
        // 对每个table获取所有的index
        indexes.clear();
        dbs_.find(current_db_)->second->catalog_mgr_->GetTableIndexes(table->GetTableName(),indexes);

        for( auto index : indexes ){
            string index_name(index->GetIndexName());
            if( longest < index_name.length() ) longest = index_name.length();
            index_names.push_back(index_name);
        }
    }

    // 按格式输出
    string top = "+" + string(longest + 2, '-') + "+";
    cout << "there are " << index_names.size() << " indexes" << endl;
    if(index_names.size()) {
        cout << top << endl;
        for( string name : index_names ){
            cout << "| ";
            cout << setw(longest) << left << name;
            cout << " |" << endl;
        }
        cout << top << endl;
    }

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
    // 先得到索引的名字
    string Index_name( ast->child_->val_);
    // 再得到表的名字
    string Table_name( ast->child_->next_->val_);
    // 先找表
    TableInfo* Table_info = nullptr;
    if( dbs_.find(current_db_) == dbs_.end() || dbs_.find(current_db_)->second->catalog_mgr_->GetTable(Table_name,Table_info) != DB_SUCCESS ){
        return DB_TABLE_NOT_EXIST;
    }
    IndexInfo* new_index_info = nullptr;

    pSyntaxNode Column_name = ast->child_->next_->next_->child_;
    vector<string> columns_name;columns_name.clear();
    // 得到索引的列的名
    while( Column_name != nullptr ){
        columns_name.push_back(Column_name->val_);
        Column_name = Column_name->next_;
    }
    // 创建索引
    dberr_t create_index = dbs_.find(current_db_)->second->catalog_mgr_->CreateIndex(Table_name,Index_name,columns_name, nullptr,new_index_info,
                                                              "");
    return create_index;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
    // 找到index的名字
    string index_name(ast->child_->val_);
    // 检验存在性
    if( dbs_.find(current_db_) == dbs_.end() )  return DB_NOT_EXIST;
    // 得到所有的table
    vector<TableInfo*> tables;
    tables.clear();
    dbs_.find(current_db_)->second->catalog_mgr_->GetTables(tables);
    // 遍历找index，不考虑重名
    dberr_t drop_index = DB_FAILED;
    for( auto table : tables ){
        IndexInfo* index_info = nullptr;
        if( dbs_.find(current_db_)->second->catalog_mgr_->DropIndex(table->GetTableName(),index_name) == DB_SUCCESS ){
            drop_index = DB_SUCCESS;
            break;
        }
    }
    return drop_index;
}


dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

static void InputCommand(char *input, const int len, fstream &exe) {
    memset(input, 0, len);
    int i = 0;
    char ch;
    while (!exe.eof() && (ch = exe.get()) != ';') {
        input[i++] = ch;
    }
    if(exe.eof()) {
        memset(input, 0, len);
        return;
    }
    input[i] = ch;  // ;
    exe.get();      // remove enter

    // test
    printf("minisql > ");
    puts(input);
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
    // get the name
    string file_name(ast->child_->val_);
    fstream exe(file_name);
    if( !exe.is_open() ) {
        cout << "open file " << file_name << " fail!" << endl;
        return DB_FAILED;
    }

    int MAXSIZE = 1024;
    char* command = new char[MAXSIZE];
    // 在读取所有命令前
    while( !exe.eof() ){
        InputCommand(command, MAXSIZE, exe);
        if(command[0] == 0) break;
//        exe.getline(command,MAXSIZE);
//        LOG(INFO) << command << std::endl;
        // 开始处理sql语句
        YY_BUFFER_STATE bp = yy_scan_string(command);
        if (bp == nullptr) {
            LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
            exit(1);
        }
        yy_switch_to_buffer(bp);
        // init parser module
        MinisqlParserInit();

        // parse
        yyparse();

        auto result = this->Execute(MinisqlGetParserRootNode());

        // clean memory after parse
        MinisqlParserFinish();
        yy_delete_buffer(bp);
        yylex_destroy();

        // quit condition
        this->ExecuteInformation(result);
        if (result == DB_QUIT) {
            cout << "Bye!" << endl;
            break;
        }
    }

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
    // 直接返回，没啥好说的
    ASSERT(ast->type_ == kNodeQuit , "Unexpected node type");
    return DB_QUIT;
}
