#pragma once

#ifndef INTERPRETER

#include <iostream>
#include <string>
#include "API.h"
#include "CatalogManager.h"
using namespace std;

#define SQL_LINE_BUFSIZE 1024
#define BUFFER_OVERFLOW  1001
#define STATUS_EXIT     0
#define STATUS_ALLRIGHT 1
#define STATUS_ERROR    2

#define CREATE_TABLE    0x11
#define CREATE_INDEX    0x12
#define DROP_TABLE      0x13
#define DROP_INDEX      0x14
#define SELECT          0x15
#define INSERT          0x16
#define DELETE          0x17
#define EXEC_FILE       0x18

#define WRONG_FORMAT           -1
#define TYPE_NOT_EXIST         -2
#define TABLE_NOT_CREATED      -3 
#define DROP_TABLE_FAILED      -4
#define INDEX_NOT_EXIST        -5
#define TABLE_INFO_NOT_FETCHED -6
#define WRONG_ATTR_NUM         -7
#define FILE_NOT_EXIST         -8
#define NO_EXISTING_COMMAND    -9
#define INVALID_ATTR_FOR_INDEX -10
#define INVALID_CHAR_VALUE     -11
#define INVALID_INT_VALUE      -12

class Interpreter {
private:
    API* myAPI;
    CatalogManager* myCatalogManager;
    int ClassifyCommand(string commandString);
    int ProcessCreateTableCommand(string commandString, bool showInfo = true);
    int ProcessDropTableCommand(string commandString, bool showInfo = true);
    int ProcessCreateIndexCommand(string commandString, bool showInfo = true);
    int ProcessDropIndexCommand(string commandString, bool showInfo = true);
    int ProcessSelectCommand(string commandString, bool showInfo = true);
    int ProcessInsertCommand(string commandString, bool showInfo = true);
    int ProcessDeleteCommand(string commandString, bool showInfo = true);
    int ProcessExecfileCommand(string commandString);
    int ExecCommand(string line, bool showInfo = true);
    int GetTypeCode(string type);
    void ErrorDealer(int errorCode);
    bool IsDigit(string tmp);
    void RemoveSpaces(string& str);
    void EraseExtraSpaces(string& commandString);
    void ProcessOperators(string& commandString);
    string GeneratePKName(string attrName);

public:
    Interpreter(CatalogManager* catalogManager, API* api);
    ~Interpreter();
    void InterpretCommand();
};

#endif INTERPRETER