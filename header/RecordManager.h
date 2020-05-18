#pragma once

#ifndef RECORD_MANAGER

#include <string>
#include "BufferManager.h"
using namespace std;

#define MAX_CHAR_LENGTH   256

#define EQUAL                   0x21
#define UNEQUAL                 0x22
#define LESS_THAN               0x23
#define GREATER_THAN            0x24
#define LESS_THAN_OR_EQUAL      0x25
#define GREATER_THAN_OR_EQUAL   0x26

class RecordManager {
private:
    BufferManager* myBufferManager;
    string GenerateTableName(string tableName);
    string GenerateIndexName(string indexName);

public:
    RecordManager();
    RecordManager(BufferManager* myBM);
    ~RecordManager();

    int CreateTable(string tableName);
    int DropTable(string tableName);

    int CreateIndex(string indexName);
    int DropIndex(string indexName);

    int InsertRecord(blockNode* blocknode, void* record, int recordSize);
    int DeleteRecord(blockNode* blocknode);

    //void* SelectRecord(blockNode* blocknode, tableInfoNode* tableInfo, int op, string attribute, string val);
    void* SelectRecord(blockNode* blocknode, int recordSize, int offset);
    bool RecordMatch(int intVal, int op, string val);
    bool RecordMatch(float floatVal, int op, string val);
    bool RecordMatch(char* charVal, int op, string val, int attrSize);


};

#endif RECORD_MANAGER