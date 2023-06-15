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

extern "C" {
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
    char path[] = "./databases";
    DIR *dir;
    if((dir = opendir(path)) == nullptr) {
        mkdir("./databases", 0777);
        dir = opendir(path);
    }
    /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.

     **/
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
            //std::cout<<row.GetField(0)->toString()<<std::endl;
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
    //ASSERT(ast== nullptr || ast->child_== nullptr, "ExecuteEngine::ExecuteCreateDatabase error: null ast or ast child");
    ASSERT(ast->type_ == kNodeCreateDB, "ExecuteEngine::ExecuteCreateDatabase error: wrong input ast type");
    string db_name=ast->child_->val_;
    if (dbs_.find(db_name)!=dbs_.end())
    {
        cout<<"Database "<<db_name<<" already exists."<<endl;
        return DB_ALREADY_EXIST;
    }
    DBStorageEngine *new_db = new DBStorageEngine(db_name.data());
    if (new_db== nullptr)
        return DB_FAILED;
    dbs_.emplace(db_name,new_db);
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
    ASSERT(ast== nullptr || ast->child_== nullptr, "ExecuteEngine::ExecuteDropDatabase error: null ast or ast child");
    ASSERT(ast->type_ == kNodeDropDB, "ExecuteEngine::ExecuteDropDatabase error: wrong input ast type");
    string db_name=ast->child_->val_;
    auto db=dbs_.find(db_name);
    if (db==dbs_.end())
    {
        cout<<"Database "<<db_name<<" doesn't exist."<<endl;
        return DB_FAILED;
    }
    db->second->~DBStorageEngine();
    dbs_.erase(db_name);
    if (current_db_==db_name) current_db_="";
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
    ASSERT(ast->type_ == kNodeShowDB, "ExecuteEngine::ExecuteShowDatabase error: wrong input ast type");
    for (auto db:dbs_)
        cout<<db.first<<endl;
    cout<<"total number: "<<dbs_.size()<<endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
    //ASSERT(ast== nullptr || ast->child_== nullptr, "ExecuteEngine::ExecuteUseDatabase error: null ast or ast child");
    //ASSERT(ast->type_ == kNodeUseDB, "ExecuteEngine::ExecuteUseDatabase error: wrong input ast type");
    string db_name=ast->child_->val_;
    auto db=dbs_.find(db_name);
    if (db==dbs_.end())
    {
        cout<<"Database "<<db_name<<" doesn't exist."<<endl;
        return DB_NOT_EXIST;
    }
    cout<<"Switch to DB: "<<db_name<<endl;
    current_db_=db_name;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
    ASSERT(ast->type_ == kNodeShowTables, "ExecuteEngine::ExecuteShowTables error: wrong input ast type");
    auto db=dbs_.find(current_db_);
    if (db==dbs_.end())
    {
        cout<<"No database selected."<<endl;
        return DB_NOT_EXIST;
    }
    std::vector<TableInfo*>table_info;
    db->second->catalog_mgr_->GetTables(table_info);
    for (auto table:table_info)
    {
        cout<<table->GetTableName()<<endl;
    }
    cout<<"total number: "<<table_info.size()<<endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
    //ASSERT(ast== nullptr || ast->child_== nullptr, "ExecuteEngine::ExecuteShowIndexes error: null ast or ast child");
    //ASSERT(ast->type_ == kNodeCreateTable, "ExecuteEngine::ExecuteUseShowIndexes error: wrong input ast type");
    auto db=dbs_.find(current_db_);
    if (db==dbs_.end())
    {
        cout<<"No database selected."<<endl;
        return DB_NOT_EXIST;
    }

    CatalogManager* db_catalog=db->second->catalog_mgr_;
    string table_name=ast->child_->val_;
    TableInfo *table_info= nullptr;
    dberr_t table_exist=db_catalog->GetTable(table_name,table_info);
    if (!table_exist)
        return DB_ALREADY_EXIST;

    vector<string> names,unique;
    unordered_map<string ,bool> if_uni;
    unordered_map<string ,string> col_type;
    unordered_map<string ,int> col_length;

    //columns
    auto column=ast->child_->next_->child_;
    while (column!= nullptr && column->type_ == kNodeColumnDefinition)
    {
        string column_name=column->child_->val_;
        string column_type=column->child_->next_->val_;

        names.push_back(column_name);
        col_type.emplace(column_name,column_type);

        if (column->val_!= nullptr)
        {
            unique.push_back(column_name);
            if_uni.emplace(column_name, true);
        }

        if (column_type=="char")
        {
            string len=column->child_->next_->child_->val_;
            int length=atoi(len.data());
            if (length<=0) return DB_FAILED;
            col_length.emplace(column_name,length);
        }

        column=column->next_;
    }

    //create column
    vector<Column*> columns;
    columns.clear();
    uint32_t col_index=0;
    for (auto &name:names)
    {
        TypeId type;
        if (col_type[name]=="int") type=kTypeInt;
        else if (col_type[name]=="float") type=kTypeFloat;
        else if (col_type[name]=="char") type=kTypeChar;
        else return DB_FAILED;

        Column* new_column;
        if (col_type[name]=="char")
            new_column=new Column(name,type,col_length[name],col_index, false,if_uni[name]);
        else
            new_column=new Column(name,type,col_index, false,if_uni[name]);

        col_index++;
        columns.push_back(new_column);
    }

    //create schema and table
    Schema *schema=new Schema(columns);
    TableInfo *table_info_new=nullptr;
    dberr_t create_table = db_catalog->CreateTable(table_name,schema, nullptr,table_info_new);
    if (create_table) return create_table;

    //create index for primary key
    vector<string >index_key;
    if (column!= nullptr) column=column->child_;
    while (column!= nullptr && column->type_==kNodeIdentifier)
    {
        index_key.push_back(column->val_);
        column=column->next_;
    }
    string index_name=table_name + "_primary";
    IndexInfo *index_info= nullptr;
    dberr_t create_index=db_catalog->CreateIndex(table_name,index_name,index_key,nullptr,index_info,"bptree");
    if( create_index ) return create_index;

    //create index for unique keys
    index_key.clear();
    for (auto &uni:unique)
    {
        index_key.push_back(uni);
        index_name = table_name + "_" + uni + "_unique";
        IndexInfo * new_index = nullptr;
        dberr_t create_index = db_catalog->CreateIndex(table_name,index_name,index_key, nullptr,new_index,"bptree");
        if( create_index ) return create_index;
        index_key.pop_back();
    }

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
//  ASSERT(ast== nullptr || ast->child_== nullptr, "ExecuteEngine::ExecuteDropTable error: null ast or ast child");
//  ASSERT(ast->type_ == kNodeDropTable, "ExecuteEngine::ExecuteUseDropTable error: wrong input ast type");
    auto db=dbs_.find(current_db_);
    if (db==dbs_.end())
    {
        cout<<"No database selected."<<endl;
        return DB_NOT_EXIST;
    }
    string table_name=ast->child_->val_;
    //catalog_manager里已经drop table对应的index了所以这里没有drop index
    return db->second->catalog_mgr_->DropTable(table_name);
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
    ASSERT(ast== nullptr || ast->child_== nullptr, "ExecuteEngine::ExecuteShowIndexes error: null ast or ast child");
    ASSERT(ast->type_ == kNodeShowIndexes, "ExecuteEngine::ExecuteUseShowIndexes error: wrong input ast type");
    auto db=dbs_.find(current_db_);
    if (db==dbs_.end())
    {
        cout<<"No database selected."<<endl;
        return DB_NOT_EXIST;
    }

    CatalogManager* db_catalog=db->second->catalog_mgr_;
    vector<TableInfo*> tables;
    db_catalog->GetTables(tables);

    vector<IndexInfo*> indexes;
    int total=0;
    for (auto table:tables)
    {
        dberr_t err=db_catalog->GetTableIndexes(table->GetTableName(),indexes);
        if (err) return err;
        total+=indexes.size();
        for (auto index:indexes)
            cout<<index->GetIndexName()<<endl;
    }

    cout<<"total number: "<<total<<endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
    ASSERT(ast== nullptr || ast->child_== nullptr, "ExecuteEngine::ExecuteCreateIndex error: null ast or ast child");
    ASSERT(ast->type_ == kNodeCreateIndex, "ExecuteEngine::ExecuteUseCreateIndex error: wrong input ast type");
    auto db=dbs_.find(current_db_);
    if (db==dbs_.end())
    {
        cout<<"No database selected."<<endl;
        return DB_NOT_EXIST;
    }

    string index_name=ast->child_->val_,table_name=ast->child_->next_->val_;
    CatalogManager* db_catalog=db->second->catalog_mgr_;

    vector<string> keymap;
    keymap.clear();
    auto key_node=ast->child_->next_->next_->child_;
    while (key_node!= nullptr)
    {
        keymap.push_back(string(key_node->val_));
        key_node=key_node->next_;
    }

    IndexInfo *index_info= nullptr;
    dberr_t create_index=db_catalog->CreateIndex(table_name,index_name,keymap, nullptr,index_info,"bptree");
    return create_index;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
    ASSERT(ast== nullptr || ast->child_== nullptr, "ExecuteEngine::ExecuteDropIndex error: null ast or ast child");
    ASSERT(ast->type_ == kNodeDropIndex, "ExecuteEngine::ExecuteUseDropIndex error: wrong input ast type");

    string index_name=ast->child_->val_;
    auto db=dbs_.find(current_db_);
    if (db==dbs_.end())
    {
        cout<<"No database selected."<<endl;
        return DB_NOT_EXIST;
    }
    auto db_catalog=db->second->catalog_mgr_;

    //get table
    vector<TableInfo*> tables;
    tables.clear();
    dberr_t err=db_catalog->GetTables(tables);
    if (err) return err;

    //find index
    for (auto table:tables)
    {
        err=db_catalog->DropIndex(table->GetTableName(),index_name);
        if (!err)
        {
            cout<<"Drop index sucessfully!"<<endl;
            return DB_SUCCESS;
        }
    }

    cout<<"Index doesn't exist."<<endl;
    return DB_INDEX_NOT_FOUND;
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

/**
 * TODO: Student Implement
 */

//refer to main.cpp

void InputCommand(char *input, const int len,fstream &file) {
    memset(input, 0, len);
    printf("minisql > ");
    int i = 0;
    char ch;
    while (!file.eof() && (ch = file.get()) != ';') {
        input[i++] = ch;
    }
    if (file.eof())
    {
        memset(input,0,len);
        return;
    }
    input[i] = ch;  // ;
    file.get();      // remove enter
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
//  ASSERT(ast== nullptr || ast->child_== nullptr, "ExecuteEngine::ExecuteExecfile error: null ast or ast child");
//  ASSERT(ast->type_ == kNodeExecFile, "ExecuteEngine::ExecuteExecfile error: wrong input ast type");
    string file_name=ast->child_->val_;
    fstream file(file_name);
    if (!file.is_open())
    {
        cout<<"Open file "<<file_name<<" fail!"<<endl;
        return DB_FAILED;
    }

    // command buffer
    const int buf_size = 1024;
    char cmd[buf_size];

    while (!file.eof()) {
        // read from buffer
        InputCommand(cmd, buf_size,file);
        // create buffer for sql input
        YY_BUFFER_STATE bp = yy_scan_string(cmd);
        if (bp == nullptr) {
            LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
            exit(1);
        }
        yy_switch_to_buffer(bp);

        // init parser module
        MinisqlParserInit();

        // parse
        yyparse();

        // parse result handle
        if (MinisqlParserGetError()) {
            // error
            printf("%s\n", MinisqlParserGetErrorMessage());
        } else {
            printf("[INFO] Sql syntax parse ok!\n");
        }

        auto result = this->Execute(MinisqlGetParserRootNode());

        // clean memory after parse
        MinisqlParserFinish();
        yy_delete_buffer(bp);
        yylex_destroy();

        // quit condition
        this->ExecuteInformation(result);
        if (result == DB_QUIT) {
            cout<<"Bye!"<<endl;
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
    ASSERT(ast->type_ == kNodeQuit, "ExecuteEngine::ExecuteQuit error: wrong input ast type");
    return DB_QUIT;
}
