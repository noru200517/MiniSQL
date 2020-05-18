#include "Interpreter.h"
#include <iostream>
#include <sstream>
#include <vector>
using namespace std;

Interpreter::Interpreter(CatalogManager* catalogManager, API* api)
{
    myCatalogManager = catalogManager;
    myAPI = api;
}

Interpreter::~Interpreter()
{
    
}

/**
 *   Get command string, process and operate according to it
 */
void Interpreter::InterpretCommand()
{
    char command[SQL_LINE_BUFSIZE];
    while (1) {
        commandString = "";
        //get input command
        cout << "minisql>>";
        while (1) {
            cin.getline(command, SQL_LINE_BUFSIZE, '\n');
            commandString += (string)command;
            if (commandString[commandString.size() - 1] == ';') {
                break;
            }
            else {
                cout << "       >>";
            }
        }
        //test* see the input command string
        cout << "Command string is: " << commandString << endl;
        if (commandString.compare("quit;") == 0) {
            break;
        }

        //classify command (create table, drop table, create index, drop index
        //                  select, indert, delete, execfile)
        int commandType = ClassifyCommand();

        //process commands
        switch (commandType) {
        case CREATE_TABLE:
            ProcessCreateTableCommand();
            break;
        case CREATE_INDEX:
            ProcessCreateIndexCommand();
            break;
        case DROP_TABLE:
            ProcessDropTableCommand();
            break;
        case DROP_INDEX:
            ProcessDropIndexCommand();
            break;
        case SELECT:
            ProcessSelectCommand();
            break;
        case INSERT:
            ProcessInsertCommand();
            break;
        case DELETE:
            ProcessDeleteCommand();
            break;
        case EXEC_FILE:
            ProcessExecfileCommand();
            break;
        default:
            break;
        }
    }
}

/** 
 *  Decide which category(create table, create index, drop table, drop index, select, insert, delete) the query falls into.
 *
 *   @return int: the type of the query.
 */
int Interpreter::ClassifyCommand()
{
    stringstream commandStream;
    string commandType;

    commandStream.str(commandString);
    getline(commandStream, commandType, ' ');
    if (commandType.compare("create") == 0) {
        getline(commandStream, commandType, ' ');
        if (commandType.compare("table") == 0) {
            return CREATE_TABLE;
        }
        else {
            return CREATE_INDEX;
        }
    }
    else if (commandType.compare("drop") == 0) {
        getline(commandStream, commandType, ' ');
        if (commandType.compare("table") == 0) {
            return DROP_TABLE;
        }
        else {
            return DROP_INDEX;
        }
    }
    else if (commandType.compare("select") == 0) {
        return SELECT;
    }
    else if (commandType.compare("insert") == 0) {
        return INSERT;
     }
    else if (commandType.compare("delete") == 0) {
        return DELETE;
    }
    else if(commandType.compare("execfile") == 0){
        return EXEC_FILE;
    }
    else {
        cerr << "Syntex error: no existing command." << endl;
        return SYNTAX_ERROR;
    }
    return 0;
}

/**
 *  Process create table command.
 *
 *  @return int: if return STATUS_ALRIGHT, the table has been created successfully.
 *  @            if return STATUS_ERROR, the command failed.
 */
int Interpreter::ProcessCreateTableCommand()
{
    //variables used to parse the command
    string tableName = "";          //name of the table
    string head = "", content = ""; //string of the head part(create table tableName) and the content part
    string tmp = "";
    vector <string> headInfo;       //store words in the head part
    stringstream commandStream;     //stringstream for the whole command
    stringstream infoStream;        //stringstream for the head part
    stringstream contentStream;     //stringstream for the content part
    vector <string> contentInfo;    //used when process each attribute
    int hasCreated;
    bool uniqueFlag = false;

    //variables used to generate a catalog block                
    int recordSize = 0;                 
    int attributeNum = 0;               
    vector <string> attributeName;  
    unsigned char* recordFormat = new unsigned char[64]; 
    unsigned char* recordUnique = new unsigned char[32];  
    vector <string> primaryKey;

    //clear buffer
    memset(recordFormat, 0, 64);
    memset(recordUnique, 0, 32);

    //preprocessing: eliminate the ");"
    int pos = commandString.find(");");
    if (pos == commandString.npos) {
        cerr << "Syntax error: wrong format" << endl;
        return STATUS_ERROR;
    }
    else {
        commandString = commandString.substr(0, commandString.size() - 2);
        if (commandString[commandString.size() - 1] == ',') {
            cerr << "Syntax error: wrong format" << endl;
            return STATUS_ERROR;
        }
    }

    //generate headInfo to get the name of the table
    commandStream.str(commandString);
    getline(commandStream, head, '(');
    infoStream.str(head);
    while (getline(infoStream, tmp, ' ')) {
        headInfo.push_back(tmp);
    }

    pos = 0;
    //get table information
    while (getline(commandStream, content, ',')) {    //divide by line
        contentStream.clear();
        contentStream.str(content);
        tmp = "";
        contentInfo.clear();
        while (getline(contentStream, tmp, ' ')) {    //divide by space
            contentInfo.push_back(tmp);
        }
        if (contentInfo[0].compare("primary") == 0) { //primary key
            //TODO: add primary key info to catalog manager
        }
        else {                                        //get a new attribute
            string attrName = contentInfo[0];
            attributeName.push_back(attrName);
            attributeNum++;

            //check whether the attribute is unique
            tmp = "";
            if (contentInfo[contentInfo.size() - 1].compare("unique") == 0) {
                for (size_t i = 1; i < contentInfo.size() - 1; i++)tmp += contentInfo[i];
                uniqueFlag = true;
            }
            else {
                for (size_t i = 1; i < contentInfo.size(); i++)tmp += contentInfo[i];
            }
            for (size_t i = 0; i < tmp.size(); i++) {
                if (tmp[i] == ' ')tmp.erase(i, 1);
            }

            //get attribute's type
            int type = GetTypeCode(tmp);
            if (type < 0) {   //error
                cerr << "Syntax error: no such type exist." << endl;
                return SYNTAX_ERROR;
            }
            else {            //update recordUnique, recordSize and recordFormat
                if (uniqueFlag)recordUnique[attributeNum - 1] = 1;
                else recordUnique[attributeNum - 1] = 0;
                switch (type) {
                case INT:     //int
                    recordSize += 4;
                    recordFormat[pos++] = INT;
                    recordFormat[pos++] = 0;
                    break;
                case FLOAT:   //float
                    recordSize += 4;
                    recordFormat[pos++] = FLOAT;
                    recordFormat[pos++] = 0;
                    break;
                default:      //char
                    recordSize += type - CHAR;
                    recordFormat[pos++] = CHAR;
                    recordFormat[pos++] = type - CHAR;
                    break;
                }
            }
        }
    }

    //recordSize is always a multiple of 2, so it will not be stored across two blocks
    for (int i = 1; i <= 8192; i = i << 1) {
        if (recordSize <= i && recordSize > (i >> 1)) {
            recordSize = i;
            break;
        }
    }

    //create tables and catalogs
    //get table name and call API module
    tableName = headInfo[headInfo.size() - 1];
    hasCreated = myAPI->CreateTable(tableName);
    if (hasCreated == 0) {
        cerr << "Error: table has been created" << endl;
        return STATUS_ERROR;
    }
    else {
        //call Catalog manager to generate a catalog block
        myCatalogManager->CreateTableCatalog(tableName, recordSize, attributeNum, attributeName, recordFormat, recordUnique);
    }

    return STATUS_ALLRIGHT;
}

/**
 *  Process drop table command.
 *
 *  @return int: if return STATUS_ALRIGHT, the table has been created successfully.
 *  @            if return STATUS_ERROR, the command failed.
 */
int Interpreter::ProcessDropTableCommand()
{
    string tableName = "";          //name of the table
    stringstream commandStream;     
    string tmp = "";
    vector <string> headInfo;
    int position = 0;

    //get the name of the table
    commandStream.str(commandString);
    while (getline(commandStream, tmp, ' ')) {
        headInfo.push_back(tmp);
    }
    tableName = headInfo[headInfo.size() - 1];
    tableName = tableName.substr(0, tableName.size() - 1);

    //check whether the table has been created (if not, we can't delete it)
    position = myCatalogManager->FindTable(tableName);
    if (position == TABLE_NOT_FOUND) {
        cerr << "Error: table has not been created" << endl;
        return STATUS_ERROR;
    }
    else {
        //delete table file
        if (myAPI->DropTable(tableName) == 0) {
            cerr << "Error : drop table failed" << endl;
            return STATUS_ERROR;
        }
        //drop catalog first
        if (myCatalogManager->DropTableCatalog(tableName, position) == 0) {
            cerr << "Error : drop table failed" << endl;
            return STATUS_ERROR;
        }
    }

    return STATUS_ALLRIGHT;
}

/**
 *  To be continued...
 */
int Interpreter::ProcessCreateIndexCommand()
{
    //test
    cout << "CREATE INDEX" << endl;
    return 0;
}

/**
 *  To be continued...
 */
int Interpreter::ProcessDropIndexCommand()
{
    //test
    cout << "DROP INDEX" << endl;
    return 0;
}

/*
 *  Process select command.
 *   
 *  @return int: the number of rows selected.
 */
int Interpreter::ProcessSelectCommand()
{
    string tableName = "";          //name of the table
    string requirement = "";
    string tmp = "";
    stringstream commandStream;
    stringstream infoStream;
    vector <string> headInfo;
    int hasCreated = 0;
    bool whereFlag = false;
    tableInfoNode* tableInfo;

    //variables used when processing each requirements
    string attribute;
    string op;
    string val;
    string tmpTableName;
    int result = 0;

    //decide whether "where" is presented
    int pos = commandString.find("where");
    if (pos != commandString.npos) {
        whereFlag = true;
    }

    //divide demands into words
    commandStream.str(commandString);
    while (getline(commandStream, tmp, ' ')) {
        headInfo.push_back(tmp);
    }
    //if it is not a "select *", report error
    if (headInfo[1].compare("*") != 0) {
        cerr << "Error: wrong format" << endl;
        return STATUS_ERROR;
    }
    //eliminate the ";" at the end of the last word
    tmp = headInfo[headInfo.size() - 1];
    tmp = tmp.substr(0, tmp.size() - 1);
    headInfo[headInfo.size() - 1] = tmp;

    if (!whereFlag) {   //present all records
        tableName = headInfo[headInfo.size() - 1];
        //if table has not been created, return
        if (myCatalogManager->FindTable(tableName) == TABLE_NOT_FOUND) {
            cerr << "Error: table has not been created" << endl;
            return STATUS_ERROR;
        }
        myAPI->ShowAll(tableName);
    }
    else {              //decide which record to present
        tableName = headInfo[3];
        tmpTableName = tableName;
        //if table has not been created, return
        if (myCatalogManager->FindTable(tableName) == TABLE_NOT_FOUND) {
            cerr << "Error: table has not been created" << endl;
            return STATUS_ERROR;
        }
        for (int i = 5; i < headInfo.size(); i += 4) {
            //char型的应该还要把两边的引号摘掉哈
            attribute = headInfo[i];
            op = headInfo[i + 1];
            val = headInfo[i + 2];
            //rip off quotation marks
            if (val[0] == '\'' || val[0] == '"')val = val.substr(1, val.size() - 1);
            if (val[val.size() - 1] == '\'' || val[val.size() - 1] == '"')val = val.substr(0, val.size() - 1);

            if (op.compare("=") == 0)       tmpTableName = myAPI->SelectRecord(EQUAL, attribute, val, tmpTableName);
            else if (op.compare("<>") == 0) tmpTableName = myAPI->SelectRecord(UNEQUAL, attribute, val, tmpTableName);
            else if (op.compare("<") == 0)  tmpTableName = myAPI->SelectRecord(LESS_THAN, attribute, val, tmpTableName);
            else if (op.compare(">") == 0)  tmpTableName = myAPI->SelectRecord(GREATER_THAN, attribute, val, tmpTableName);
            else if (op.compare("<=") == 0) tmpTableName = myAPI->SelectRecord(LESS_THAN_OR_EQUAL, attribute, val, tmpTableName);
            else if (op.compare(">=") == 0) tmpTableName = myAPI->SelectRecord(GREATER_THAN_OR_EQUAL, attribute, val, tmpTableName);
            else {
                cerr << "Error: wrong input format" << endl;
                return STATUS_ERROR;
            }
            if (tmpTableName.empty())break;
        }
        if (tmpTableName == "") {
            return 0;
        }
        else {
            //不需要两个table你得改一改
            result = myAPI->ShowAll(tmpTableName); //the first table: catalog table, decide the output format. the second table: decide which records to output.
            pos = myCatalogManager->FindTable(tmpTableName);
            if (pos == TABLE_NOT_FOUND)return 0;
            else {
                myAPI->DropTable(tmpTableName);
                myCatalogManager->DropTableCatalog(tmpTableName, pos);
            }
        }
    }

    return result;
}

/*
 *  Process insert command.
 *
 *  @return int: the number of rows affected.
 */
int Interpreter::ProcessInsertCommand()
{
    string tableName = "";          //name of the table
    string head = "";
    string tmp = "";
    stringstream commandStream;
    stringstream infoStream;
    vector <string> headInfo;
    vector <string> contentInfo;
    int hasCreated = 0;
    tableInfoNode* tableInfo;

    //preprocessing: eliminate the ");"
    int pos = commandString.find(");");
    if (pos == commandString.npos) {
        cerr << "Syntax error: wrong format" << endl;
        return STATUS_ERROR;
    }
    else {
        commandString = commandString.substr(0, commandString.size() - 2);
        if (commandString[commandString.size() - 1] == ',') {
            cerr << "Syntax error: wrong format" << endl;
            return STATUS_ERROR;
        }
    }

    //get the name of the table
    commandStream.str(commandString);
    getline(commandStream, head, '(');
    infoStream.str(head);
    while (getline(infoStream, tmp, ' ')) {
        headInfo.push_back(tmp);
    }
    //insert into tableName values: there have to be 4 words
    if (headInfo.size() != 4) {
        cerr << "Syntax error: wrong format" << endl;
        return STATUS_ERROR;
    }
    tableName = headInfo[headInfo.size() - 2];

    //check whether the table has been created(else we can't do insertion)
    hasCreated = myCatalogManager->FindTable(tableName);
    if (hasCreated == TABLE_NOT_FOUND) {
        cerr << "Error: table has not been created" << endl;
        return STATUS_ERROR;
    }
    else {
        tableInfo = myCatalogManager->GetTableInfo(tableName);
        if (tableInfo == nullptr) {
            cerr << "Error: fail to get table info" << endl;
            return STATUS_ERROR;
        }
    }

    //get the values to insert
    pos = 0;
    while (getline(commandStream, tmp, ',')) {
        contentInfo.push_back(tmp);
    }
    //if the number of attributes is not the same as the one specified in catalog manager, report an error
    if (contentInfo.size() != tableInfo->attributeNum) {
        cerr << "Error: wrong number of attributes" << endl;
        return STATUS_ERROR;
    }
    
    void* record = (void*)(new char[tableInfo->recordSize]);
    memset(record, 0, tableInfo->recordSize);
    //memset(record, 0, tableInfo->recordSize);
    int offset = 0;
    //process each attribute and combine them into a record
    for (size_t i = 0; i < contentInfo.size(); i++) {
        RemoveSpaces(&contentInfo[i]);
        switch (tableInfo->recordFormat[2 * i]) {
        case INT: {
            int intVal = 0;
            if (!IsDigit(contentInfo[i])) {
                cerr << "Error: wrong input format" << endl;
                return STATUS_ERROR;
            }
            else {
                intVal = atoi(contentInfo[i].c_str());
                memcpy((char*)record + offset, &intVal, sizeof(int));
                offset += sizeof(int);
            }
            break;
        }
        case FLOAT: {
            float floatVal;
            floatVal = atof(contentInfo[i].c_str());
            memcpy((char*)record + offset, &floatVal, sizeof(float));
            offset += sizeof(float);
            break;
        }
        case CHAR: {
            string charVal = contentInfo[i];
            if ((charVal[0] == '\'' && charVal[charVal.size() - 1] == '\'') || (charVal[0] == '"' && charVal[charVal.size() - 1] == '"')) {
                charVal = charVal.substr(1, charVal.size() - 2);
                //if the length of char does not correspond with table record
                if (charVal.size() != tableInfo->recordFormat[2 * i + 1]) {
                    cerr << "Error: wrong input format" << endl;
                    return STATUS_ERROR;
                }
                memcpy((char*)record + offset, charVal.c_str(), charVal.size());
                offset += charVal.size();
            }
            else {
                cerr << "Error: wrong input format" << endl;
                return STATUS_ERROR;
            }
            break;
        }
        default: 
            break;
        }
    }

    //Insert the record
    myAPI->InsertRecord(tableName, record, tableInfo->recordSize);

    return 0;
}

/*
 *  Process delete command.
 *
 *  @return int: the number of rows affected.
 */
int Interpreter::ProcessDeleteCommand()
{
    string tableName = "";          //name of the table
    string requirement = "";
    string tmp = "";
    stringstream commandStream;
    stringstream infoStream;
    vector <string> headInfo;
    int hasCreated = 0;
    bool whereFlag = false;
    tableInfoNode* tableInfo;

    //variables used when processing each requirements
    string attribute;
    string op;
    string val;
    string tmpTableName;

    //decide whether "where" is presented
    int pos = commandString.find("where");
    if (pos != commandString.npos) {
        whereFlag = true;
    }
    //divide demands into words
    commandStream.str(commandString);
    while (getline(commandStream, tmp, ' ')) {
        headInfo.push_back(tmp);
    }
    //eliminate the ";" at the end of the last word
    tmp = headInfo[headInfo.size() - 1];
    tmp = tmp.substr(0, tmp.size() - 1);
    headInfo[headInfo.size() - 1] = tmp;

    //if there is no "where", delete all records
    if (!whereFlag) {
        tableName = headInfo[headInfo.size() - 1];
        myAPI->ClearAllRecords(tableName);          //return an integer
    }
    else {
        //做个虚假的删除，给一个标记如何？如果被标记了就不写回文件，真正的删除事后才做。
        //perform select and get a file
        tableName = headInfo[2];
        tmpTableName = tableName;
        //if table has not been created, return STATUS_ERROR
        if (myCatalogManager->FindTable(tableName) == TABLE_NOT_FOUND) {
            cerr << "Error: table has not been created" << endl;
            return STATUS_ERROR;
        }
        for (int i = 4; i < headInfo.size(); i += 4) {
            //char型的应该还要把两边的引号摘掉哈
            attribute = headInfo[i];
            op = headInfo[i + 1];
            val = headInfo[i + 2];
            //rip off quotation marks
            if (val[0] == '\'' || val[0] == '"')val = val.substr(1, val.size() - 1);
            if (val[val.size() - 1] == '\'' || val[val.size() - 1] == '"')val = val.substr(0, val.size() - 1);

            if (op.compare("=") == 0)       tmpTableName = myAPI->SelectRecord(EQUAL, attribute, val, tmpTableName);
            else if (op.compare("<>") == 0) tmpTableName = myAPI->SelectRecord(UNEQUAL, attribute, val, tmpTableName);
            else if (op.compare("<") == 0)  tmpTableName = myAPI->SelectRecord(LESS_THAN, attribute, val, tmpTableName);
            else if (op.compare(">") == 0)  tmpTableName = myAPI->SelectRecord(GREATER_THAN, attribute, val, tmpTableName);
            else if (op.compare("<=") == 0) tmpTableName = myAPI->SelectRecord(LESS_THAN_OR_EQUAL, attribute, val, tmpTableName);
            else if (op.compare(">=") == 0) tmpTableName = myAPI->SelectRecord(GREATER_THAN_OR_EQUAL, attribute, val, tmpTableName);
            else {
                cerr << "Error: wrong input format" << endl;
                return STATUS_ERROR;
            }
            if (tmpTableName.empty())break; //if the string is empty, an error occurs or no record is fetched
        }

        //if tmpTableName is an empty string, do nothing
        if (tmpTableName.empty())return STATUS_ALLRIGHT;
        //if it is not empty, mark every record in tmpFile as "deleted"
        else {
            myAPI->DeleteRecords(tableName, tmpTableName);
        }
    }
    
    return 0;
}

/**
 *  To be continued...
 */
int Interpreter::ProcessExecfileCommand()
{
    return 0;
}

/**
 *  Translate the string("int", "char" and "float") to the corresponding code(an integer)
 *  
 *  @param string type: one of the three strings.
 *  @return int: the integer code
 */
int Interpreter::GetTypeCode(string type)
{
    if (type.compare("int") == 0) {         //int type
        return INT;
    }
    else if (type.compare("float") == 0) {  //float type
        return FLOAT;
    }
    else{
        string tmp = type.substr(0, 4);
        int charSize = 0;
        if (tmp.compare("char") == 0) {     //char type
            //get "n" in char(n)
            tmp = type.substr(5, type.size() - 6);
            
            if (IsDigit(tmp)) {             //if tmp is a number, return
                charSize = atoi(tmp.c_str());
                if (charSize == 0)return SYNTAX_ERROR;
                else return (3 + charSize);
            }
            else return SYNTAX_ERROR;       //else there must be a syntax error
        }
        else {
            return SYNTAX_TYPE_ERROR;
        }
    }
    return 0;
}

bool Interpreter::IsDigit(string tmp)
{
    for (int i = 0; i < tmp.size(); i++) {
        if (!isdigit(tmp[i]))return false;
    }
    return true;
}

/**
 *  Remove spaces in the string str.
 */
void Interpreter::RemoveSpaces(string* str)
{
    for (int i = 0; i < str->size(); i++) {
        if ((*str)[i] == ' ') {
            (*str).replace(i, 1, "");
        }
    }
}
