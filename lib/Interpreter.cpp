#include "Interpreter.h"
#include <iostream>
#include <sstream>
#include <fstream>
#include <algorithm>
#include <vector>
#include <ctime>
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
    string commandString = "";
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
        //cout << "Command string is: " << commandString << endl;
        if (commandString.compare("quit;") == 0) {
            break;
        }

        //classify command (create table, drop table, create index, drop index
        //                  select, indert, delete, execfile)
        int commandType = ClassifyCommand(commandString);

        //process commands
        switch (commandType) {
        case CREATE_TABLE:
            ProcessCreateTableCommand(commandString);
            break;
        case CREATE_INDEX:
            ProcessCreateIndexCommand(commandString);
            break;
        case DROP_TABLE:
            ProcessDropTableCommand(commandString);
            break;
        case DROP_INDEX:
            ProcessDropIndexCommand(commandString);
            break;
        case SELECT:
            ProcessSelectCommand(commandString);
            break;
        case INSERT:
            ProcessInsertCommand(commandString);
            break;
        case DELETE:
            ProcessDeleteCommand(commandString);
            break;
        case EXEC_FILE:
            ProcessExecfileCommand(commandString);
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
int Interpreter::ClassifyCommand(string commandString)
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
        ErrorDealer(NO_EXISTING_COMMAND);
        return STATUS_ERROR;
    }
    return 0;
}

/**
 *  Process create table command.
 *
 *  @return int: if return STATUS_ALRIGHT, the table has been created successfully.
 *  @            if return STATUS_ERROR, the command failed.
 */
int Interpreter::ProcessCreateTableCommand(string commandString, bool showInfo)
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
    //initialize the tableInfoNode
    
    unsigned char** attrInfo = new unsigned char* [5];
    for (int i = 0; i < 5; i++) {
        attrInfo[i] = new unsigned char[32];
        memset(attrInfo[i], 0, 32);
    }
    int attributeNum = 0;
    vector <string> attributeName;

    //preprocessing: eliminate the ");"
    int pos = commandString.find(");");
    if (pos == commandString.npos) {
        //errorCode = -1
        ErrorDealer(WRONG_FORMAT);
        return STATUS_ERROR;
    }
    else {
        commandString = commandString.substr(0, commandString.size() - 2);
        if (commandString[commandString.size() - 1] == ',') {
            ErrorDealer(WRONG_FORMAT);
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
    string pkName = "";
    vector <pair<string, string>> indexName;
    //get table information
    while (getline(commandStream, content, ',')) {    //divide by line
        contentStream.clear();
        contentStream.str(content);
        tmp = "";
        contentInfo.clear();
        uniqueFlag = false;
        while (getline(contentStream, tmp, ' ')) {    //divide by space
            RemoveSpaces(tmp);
            contentInfo.push_back(tmp);
        }
        if (contentInfo[0].compare("primary") == 0) { //primary key
            string pk = "";
            int frontPos = 0, endPos = 0, pos = 0;
            for (int i = 1; i < contentInfo.size(); i++) {
                pk += contentInfo[i];
            }
            frontPos = pk.find("(");
            endPos = pk.find(")");
            pk = pk.substr((size_t)frontPos + 1, (size_t)endPos - (size_t)frontPos - 1);
            for (int i = 0; i < attributeNum; i++) {
                if (pk == attributeName[i]) {
                    pos = i;
                    break;
                }
            }
            attrInfo[PK][pos] = 1;
            attrInfo[UNIQUE][pos] = 1;
            attrInfo[INDEX][pos] = 1;

            //往indexNames里插入一条
            pkName = GeneratePKName(pk);
            //emmm 
            indexName.push_back(make_pair(attributeName[pos], pkName));
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
                ErrorDealer(TYPE_NOT_EXIST);
                return STATUS_ERROR;
            }
            else {            //update recordUnique, recordSize and recordFormat
                if (uniqueFlag)attrInfo[UNIQUE][attributeNum - 1] = 1;
                else attrInfo[UNIQUE][attributeNum - 1] = 0;
                switch (type) {
                case INT:     //int
                    attrInfo[TYPE][attributeNum - 1] = INT;
                    break;
                case FLOAT:   //float
                    attrInfo[TYPE][attributeNum - 1] = FLOAT;
                    break;
                default:      //char
                    attrInfo[TYPE][attributeNum - 1] = CHAR;
                    attrInfo[LENGTH][attributeNum - 1] = type - CHAR;
                    break;
                }
            }
        }
    }

    //create tables and catalogs
    //get table name and call API module
    tableName = headInfo[headInfo.size() - 1];
    string* attrName = new string[attributeNum];
    for (int i = 0; i < attributeNum; i++) {
        attrName[i] = attributeName[i];
    }
    
    tableInfoNode* tableInfo = new tableInfoNode(tableName, attrName, attributeNum, attrInfo, indexName);

    myCatalogManager->CreateTableCatalog(tableInfo);
    //primary key还需要同时建立一棵B+树
    myAPI->CreateTable(tableName, showInfo);
    
    for (int i = 0; i < attributeNum; i++) {
        if (tableInfo->attrInfo[PK][i]) {
            myAPI->CreateIndex(pkName, tableName, attrName[i]);
        }
    }

    return STATUS_ALLRIGHT;
}

/**
 *  Process drop table command.
 *
 *  @return int: if return STATUS_ALRIGHT, the table has been created successfully.
 *  @            if return STATUS_ERROR, the command failed.
 */
int Interpreter::ProcessDropTableCommand(string commandString, bool showInfo)
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
        ErrorDealer(TABLE_NOT_CREATED);
        return STATUS_ERROR;
    }
    else {
        //delete table file
        if (myAPI->DropTable(tableName, showInfo) == 0) {
            ErrorDealer(DROP_TABLE_FAILED);
            return STATUS_ERROR;
        }
        //drop catalog file
        if (myCatalogManager->DropTableCatalog(tableName) == 0) {
            ErrorDealer(DROP_TABLE_FAILED);
            return STATUS_ERROR;
        }
    }

    return STATUS_ALLRIGHT;
}

/**
 *  To be continued...
 */
int Interpreter::ProcessCreateIndexCommand(string commandString, bool showInfo)
{
    //CREATE INDEX part_of_name ON customer(name(10));
    stringstream commandStream;
    stringstream infoStream;
    stringstream contentStream;
    vector <string> headInfo;
    vector <string> contentInfo;
    string head = "", tmp = "";

    int pos = commandString.find(");");
    if (pos == commandString.npos) {
        ErrorDealer(WRONG_FORMAT);
        return STATUS_ERROR;
    }
    else {
        commandString = commandString.substr(0, commandString.size() - 2);
        if (commandString[commandString.size() - 1] == ',') {
            ErrorDealer(WRONG_FORMAT);
            return STATUS_ERROR;
        }
    }

    commandStream.str(commandString);
    getline(commandStream, head, '(');
    infoStream.str(head);
    while (getline(infoStream, tmp, ' ')) {
        headInfo.push_back(tmp);
    }
    if (headInfo.size() < 5) {
        ErrorDealer(WRONG_FORMAT);
        return STATUS_ERROR;
    }
    string indexName = headInfo[2];
    string tableName = headInfo[4];

    //check whether the table has been created(else we can't do insertion)
    int hasCreated = myCatalogManager->FindTable(tableName);
    if (hasCreated == TABLE_NOT_FOUND) {
        ErrorDealer(TABLE_NOT_CREATED);
        return STATUS_ERROR;
    }

    //get the values to insert
    pos = 0;
    while (getline(commandStream, tmp)) {
        RemoveSpaces(tmp);
        contentInfo.push_back(tmp);
    }
    if (contentInfo.size() > 1) {
        ErrorDealer(WRONG_FORMAT);
        return STATUS_ERROR;
    }
    string attrName = contentInfo[0];

    myAPI->CreateIndex(indexName, tableName, attrName);

    return 0;
}

/**
 *  To be continued...
 */
int Interpreter::ProcessDropIndexCommand(string commandString, bool showInfo)
{
    //DROP INDEX <index_name> ON <table_name>
    stringstream commandStream;
    stringstream infoStream;
    vector <string> headInfo;
    string head = "", tmp = "";

    int pos = commandString.find(";");
    if (pos == commandString.npos) {
        ErrorDealer(WRONG_FORMAT);
        return STATUS_ERROR;
    }
    else {
        commandString = commandString.substr(0, commandString.size() - 1);
        if (commandString[commandString.size() - 1] == ',') {
            ErrorDealer(WRONG_FORMAT);
            return STATUS_ERROR;
        }
    }

    commandStream.str(commandString);
    while (getline(commandStream, head, ' ')) {
        headInfo.push_back(head);
    }
    if (headInfo.size() != 3) {
        ErrorDealer(WRONG_FORMAT);
        return STATUS_ERROR;
    }
    string indexName = headInfo[2];

    //这里做一下index是否存在的错误处理！
    if (!myAPI->HasIndex(indexName)) {
        ErrorDealer(INDEX_NOT_EXIST);
        return STATUS_ERROR;
    }
    
    myAPI->DropIndex(indexName);

    return 0;
}

/*
 *  Process select command.
 *   
 *  @return int: the number of rows selected.
 */
int Interpreter::ProcessSelectCommand(string commandString, bool showInfo)
{
    string requirement = "";
    string tmp = "";
    stringstream commandStream;
    stringstream infoStream;
    vector <string> headInfo;
    int hasCreated = 0;
    bool whereFlag = false;

    //variables used when processing each requirements
    int result = 0;

    //eliminate the ';', if there is no ';', return an error
    if (commandString[commandString.size() - 1] == ';') {
        commandString = commandString.substr(0, commandString.size() - 1);
    }
    else {
        ErrorDealer(WRONG_FORMAT);
        return STATUS_ERROR;
    }

    //decide whether "where" is presented
    int pos = commandString.find("where");
    if (pos != commandString.npos) {
        whereFlag = true;
    }

    //if there are where clauses, use " = " to replace "=" (and so on)
    if (whereFlag) {
        ProcessOperators(commandString);
    }

    //divide demands into words
    commandStream.str(commandString);
    while (getline(commandStream, tmp, ' ')) {
        RemoveSpaces(tmp);
        headInfo.push_back(tmp);
    }
    //if it is not a "select *", report error
    if (headInfo[1].compare("*") != 0) {
        ErrorDealer(WRONG_FORMAT);
        return STATUS_ERROR;
    }

    string tableName = "";
    int N_A = 0;
    string* attribute = nullptr;
    int restrictNum = 0;
    string* restrict = nullptr;
    tableInfoNode* tableInfo = nullptr;
    /*
    int SelectProcess(int N_A, string attribute[], string tableName, 
    int restrictNum, string restricts[], bool show, bool total, vector <char*>* records = nullptr);
    */
    if (!whereFlag) {
        tableName = headInfo.back();
        if (myCatalogManager->FindTable(tableName) == TABLE_NOT_FOUND) {
            ErrorDealer(TABLE_NOT_CREATED);
            return STATUS_ERROR;
        }
        int N_A = myCatalogManager->GetAttrNum(tableName);
        tableInfo = myCatalogManager->GetTableInfo(tableName);
        attribute = tableInfo->attrName;
        result = myAPI->SelectProcess(N_A, attribute, tableName, 0, restrict, true, true, showInfo);
    }
    else {
        for (int i = 1; i < headInfo.size(); i++) {
            if (headInfo[i] == "where") {
                pos = i - 1;
                break;
            }
        }
        tableName = headInfo[pos];
        if (myCatalogManager->FindTable(tableName) == TABLE_NOT_FOUND) {
            ErrorDealer(TABLE_NOT_CREATED);
            return STATUS_ERROR;
        }
        int N_A = myCatalogManager->GetAttrNum(tableName);
        tableInfo = myCatalogManager->GetTableInfo(tableName);
        attribute = tableInfo->attrName;

        if ((headInfo.size() - 1 - pos) % 4 != 0) {
            ErrorDealer(WRONG_FORMAT);
            return STATUS_ERROR;
        }
        restrictNum = (headInfo.size() - pos) / 4;
        restrict = new string[(headInfo.size() - pos) / 4 * 3];
        int restr = 0;
        for (int i = pos + 2; i < headInfo.size(); i += 4) {
            restrict[restr++] = headInfo[i];
            restrict[restr++] = headInfo[(size_t)i + 1];
            restrict[restr++] = headInfo[(size_t)i + 2];
            if (restrict[restr-1][0] == '"' && restrict[restr-1][restrict[restr-1].size() - 1] == '"' ||
                restrict[restr-1][0] == '\'' && restrict[restr-1][restrict[restr-1].size() - 1] == '\'') {
                restrict[restr-1] = restrict[restr-1].substr(1, restrict[restr-1].size() - 2);
            }
        }
        result = myAPI->SelectProcess(N_A, attribute, tableName, restrictNum, restrict, true, false, showInfo);
    }

    return result;
}

/*
 *  Process insert command.
 *
 *  @return int: the number of rows affected.
 */
int Interpreter::ProcessInsertCommand(string commandString, bool showInfo)
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
        ErrorDealer(WRONG_FORMAT);
        return STATUS_ERROR;
    }
    else {
        commandString = commandString.substr(0, commandString.size() - 2);
        if (commandString[commandString.size() - 1] == ',') {
            ErrorDealer(WRONG_FORMAT);
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
        ErrorDealer(WRONG_FORMAT);
        return STATUS_ERROR;
    }
    tableName = headInfo[headInfo.size() - 2];

    //check whether the table has been created(else we can't do insertion)
    hasCreated = myCatalogManager->FindTable(tableName);
    if (hasCreated == TABLE_NOT_FOUND) {
        ErrorDealer(TABLE_NOT_CREATED);
        return STATUS_ERROR;
    }
    else {
        tableInfo = myCatalogManager->GetTableInfo(tableName);
        if (tableInfo == nullptr) {
            ErrorDealer(TABLE_INFO_NOT_FETCHED);
            return STATUS_ERROR;
        }
    }

    //get the values to insert
    pos = 0;
    while (getline(commandStream, tmp, ',')) {
        RemoveSpaces(tmp);
        contentInfo.push_back(tmp);
    }
    //if the number of attributes is not the same as the one specified in catalog manager, report an error
    if (contentInfo.size() != tableInfo->attrNum) {
        ErrorDealer(WRONG_ATTR_NUM);
        return STATUS_ERROR;
    }

    string* attrVal = new string[tableInfo->attrNum];
    int charSize = 0;
    for (int i = 0; i < tableInfo->attrNum; i++) {
        switch (tableInfo->attrInfo[TYPE][i]) {
        case INT:
            if (!IsDigit(contentInfo[i])) {
                ErrorDealer(WRONG_FORMAT);
                return STATUS_ERROR;
            }
            attrVal[i] = contentInfo[i];
            break;
        case FLOAT:
            if(!IsDigit(contentInfo[i])) {
                ErrorDealer(WRONG_FORMAT);
                return STATUS_ERROR;
            }
            attrVal[i] = contentInfo[i];
            break;
        case CHAR:
            charSize = tableInfo->attrInfo[LENGTH][i];
            if (contentInfo[i][0] == '"' && contentInfo[i][contentInfo[i].size()-1] == '"'
                || contentInfo[i][0] == '\'' && contentInfo[i][contentInfo[i].size()-1] == '\'') {
                attrVal[i] = contentInfo[i].substr(1, contentInfo[i].size() - 2);
            }
            else {
                ErrorDealer(WRONG_FORMAT);
                return STATUS_ERROR;
            }
            break;
        }
    }
    //Insert the record
    myAPI->InsertProcess(tableInfo->attrNum, attrVal, tableName, showInfo);

    delete[] attrVal;
    return 0;
}

/*
 *  Process delete command.
 *
 *  @return int: the number of rows affected.
 */
int Interpreter::ProcessDeleteCommand(string commandString, bool showInfo)
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
    string* restrict;
    int restrictNum = 0;
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
    //eliminate the ";" at the end of the last word
    tmp = headInfo[headInfo.size() - 1];
    tmp = tmp.substr(0, tmp.size() - 1);
    headInfo[headInfo.size() - 1] = tmp;

    //if there is no "where", delete all records
    if (!whereFlag) {
        tableName = headInfo[headInfo.size() - 1];
        if (myCatalogManager->FindTable(tableName) == TABLE_NOT_FOUND) {
            ErrorDealer(TABLE_NOT_CREATED);
            return STATUS_ERROR;
        }
        result = myAPI->DeleteProcess(tableName, 0, nullptr, true, showInfo);
        //int DeleteProcess(string tableName, int restrictNum, string restrict[], bool total);
    }
    else {
        //int DeleteProcess(string tableName, int restrictNum, string restrict[], bool total);
        for (int i = 1; i < headInfo.size(); i++) {
            if (headInfo[i] == "where") {
                pos = i - 1;
                break;
            }
        }
        tableName = headInfo[pos];
        if (myCatalogManager->FindTable(tableName) == TABLE_NOT_FOUND) {
            ErrorDealer(TABLE_NOT_CREATED);
            return STATUS_ERROR;
        }
        if ((headInfo.size()- 1 - pos) % 4) {
            ErrorDealer(WRONG_FORMAT);
            return STATUS_ERROR;
        }
        restrictNum = (headInfo.size() - pos) / 4;
        restrict = new string[(headInfo.size() - pos) / 4 * 3];
        int restr = 0;
        for (int i = pos + 2; i < headInfo.size(); i += 4) {
            restrict[restr++] = headInfo[i];
            restrict[restr++] = headInfo[i + 1];
            restrict[restr++] = headInfo[i + 2];
            if (restrict[restr - 1][0] == '"' && restrict[restr - 1][restrict[restr - 1].size() - 1] == '"' ||
                restrict[restr - 1][0] == '\'' && restrict[restr - 1][restrict[restr - 1].size() - 1] == '\'') {
                restrict[restr - 1] = restrict[restr - 1].substr(1, restrict[restr - 1].size() - 2);
            }

        }
        result = myAPI->DeleteProcess(tableName, restrictNum, restrict, false, showInfo);
    }
    
    return 0;
}

/**
 *  To be continued...
 */
int Interpreter::ProcessExecfileCommand(string commandString)
{
    stringstream commandStream;
    string head = "", line = "";
    string fileName = "";

    commandStream.str(commandString);
    while (getline(commandStream, head, ' '));
    fileName = head.substr(0, head.size()-1);

    ifstream in(fileName);
    string singleCommand;
    clock_t startTime = 0, endTime = 0;
    double duration = 0.0;

    if(!in) {
        ErrorDealer(FILE_NOT_EXIST);
        return STATUS_ERROR;
    }
    while (in){
        singleCommand = "";
        while (getline(in, line)) 
        {
            singleCommand += line;
            if (line[line.size()-1] == ';') {
                line = "";
                break;
            }
            line = "";
        }
        if (singleCommand != "") {
            startTime = clock();
            ExecCommand(singleCommand, false);
            endTime = clock();
            duration += endTime - startTime;
        }
    }
    //这里有点问题哈，这也太过直白了
    duration = duration / CLOCKS_PER_SEC;
    cout << "Query OK (" << duration << " sec)";
    cout << endl;

    return STATUS_ALLRIGHT;
}

int Interpreter::ExecCommand(string line, bool showInfo) {
    int commandType = ClassifyCommand(line);

    //process commands
    switch (commandType) {
    case CREATE_TABLE:
        return ProcessCreateTableCommand(line, showInfo);
    case CREATE_INDEX:
        return ProcessCreateIndexCommand(line, showInfo);
    case DROP_TABLE:
        return ProcessDropTableCommand(line, showInfo);
    case DROP_INDEX:
        return ProcessDropIndexCommand(line, showInfo);
    case SELECT:
        return ProcessSelectCommand(line, showInfo);
    case INSERT:
        return ProcessInsertCommand(line, showInfo);
    case DELETE:
        return ProcessDeleteCommand(line, showInfo);
    case EXEC_FILE:
        return ProcessExecfileCommand(line);
    default:
        break;
    }
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
                if (charSize == 0)return -1;
                else return (3 + charSize);
            }
            else return -1;       //else there must be a syntax error
        }
        else {
            return -1;
        }
    }
    return 0;
}

void Interpreter::ProcessOperators(string& commandString) {
    //啊，6写死了。话说这里写死也不会出什么大事吧……
    string* op = new string[6]{ "=", "<>", "<", ">", "<=", ">=" };
    //先消除重复空格
    bool space = false;
    for (int i = 0; i < commandString.size(); i++) {
        if (space == false && commandString[i] == ' ') {
            space = true;
        }
        else if (space == true && commandString[i] == ' ') {
            commandString.replace(i, 1, "");
        }
        else {
            space = false;
        }
    }

    //再进行运算符两边的空格生成
    int pos = 0;
    for (int i = 0; i < 6; i++) {
        pos = commandString.find(op[i]);
        if (pos != commandString.npos) { 
            if (commandString[(size_t)pos - 1] != ' ') {
                commandString.replace(pos, 0, " ");
            }
            pos = commandString.find(op[i]);
            if (commandString[(size_t)pos + 1] != ' ') {
                commandString.replace(pos + op[i].size(), 0, " ");
            }
        }
    }
    delete[] op;
}

bool Interpreter::IsDigit(string tmp)
{
    if (tmp.size() == 1 && isdigit(tmp[0]))return 1;
    return atof(tmp.c_str());
}

/**
 *  Remove spaces in the string str.
 */
void Interpreter::RemoveSpaces(string& str)
{
    for (int i = 0; i < str.size(); i++) {
        if ((str)[i] == ' ') {
            (str).replace(i, 1, "");
        }
    }
}

string Interpreter::GeneratePKName(string attrName) {
    string tmp = "pk_";
    return tmp + attrName;
}

void Interpreter::ErrorDealer(int errorCode) {
    switch (errorCode) {
    case WRONG_FORMAT:
        cerr << "Syntax error: wrong input format" << endl;
        break;
    case TYPE_NOT_EXIST:
        cerr << "Syntax error: variable type does not exist" << endl;
        break;
    case TABLE_NOT_CREATED:
        cerr << "Error: table is not created" << endl;
        break;
    case DROP_TABLE_FAILED:
        cerr << "Error: drop table failed" << endl;
        break;
    case INDEX_NOT_EXIST:
        cerr << "Error: specified index does not exist" << endl;
        break;
    case TABLE_INFO_NOT_FETCHED:
        cerr << "Error: table info is not fetched" << endl;
        break;
    case WRONG_ATTR_NUM:
        cerr << "Syntax error: wrong input attribute number" << endl;
        break;
    case FILE_NOT_EXIST:
        cerr << "Error: the file does not exist" << endl;
        break;
    case NO_EXISTING_COMMAND:
        cerr << "Syntax error: no existing command" << endl;
    }
    cout << endl;
}
