#pragma once

#ifndef API
#include "RecordManager.h"
#include "BufferManager.h"
#include "CatalogManager.h"
#include <string>
using namespace std;

class API {
private:
    RecordManager* myRecordManager;
    BufferManager* myBufferManager;
    CatalogManager* myCatalogManager;

public:
    API(CatalogManager* catalogManager, BufferManager* bufferManager, RecordManager* recordManager);
    ~API();

    int CreateTable(string tableName);
    int DropTable(string tableName);

    int InsertRecord(string tableName, void* record, int recordLength);
    string SelectRecord(int op, string attribute, string val, string tableName);

    int ClearAllRecords(string tableName);
    int DeleteRecords(string tableName, string tmpTableName);
    bool CompareRecord(char* recordToCheck, char* buffer, int recordSize);

    int ShowAll(string tableName);
    void PrintHline(vector < pair<string, int> > tableHead);
    void PrintHead(vector < pair<string, int> > tableHead);
    void ShowCell(int intVal, int length);
    void ShowCell(float floatVal, int length);
    void ShowCell(string charVal, int length, int charSize);

    string GenerateTmpFileName(string tableName);
};

#endif API