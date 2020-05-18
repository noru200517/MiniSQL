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

#define HAS_CREATED       -1
#define HAS_NOT_CREATED   -2
#define SYNTAX_ERROR      -3 
#define SYNTAX_TYPE_ERROR -4

class Interpreter {
private:
    string commandString;
    API* myAPI;
    CatalogManager* myCatalogManager;
    int ClassifyCommand();
    int ProcessCreateTableCommand();
    int ProcessDropTableCommand();
    int ProcessCreateIndexCommand();
    int ProcessDropIndexCommand();
    int ProcessSelectCommand();
    int ProcessInsertCommand();
    int ProcessDeleteCommand();
    int ProcessExecfileCommand();
    int GetTypeCode(string type);
    bool IsDigit(string tmp);
    void RemoveSpaces(string* str);

public:
    Interpreter(CatalogManager* catalogManager, API* api);
    ~Interpreter();
    void InterpretCommand();
};

#endif INTERPRETER