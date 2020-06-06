#pragma once

#ifndef API_H
#define API_H
#include "RecordManager.h"
#include "BufferManager.h"
#include "CatalogManager.h"
#include <string>

using namespace std;

class IndexManager;
class API {
private:
    IndexManager* myIndexManager;
    RecordManager* myRecordManager;
    BufferManager* myBufferManager;
    CatalogManager* myCatalogManager;

public:
    API(CatalogManager* catalogManager, BufferManager* bufferManager, RecordManager* recordManager, IndexManager* indexManager);
    ~API();

    int CreateTable(string tableName, bool showInfo = true);
    int DropTable(string tableName, bool showInfo = true);
    tableInfoNode* GetTableInfo(string tableName);
    
    int CreateIndex(string indexName, string tableName, string attrName);
    int DropIndex(string indexName);
    bool HasIndex(string tableName, string attrName);
    bool HasIndex(string indexName);
    int SetIndex(string tableName, string attrName, bool hasIndex);

    bool IsDelete(blockNode* blocknode, int recordSize, int offset) const;
    int InsertProcess(int attrNum, string attrVal[], string tableName, bool showInfo = true);
    int InsertOperation(blockNode* blocknode, void* record, int recordSize);
    int InsertIndexRecord(string tableName, string attrName, string attrVal, int recordSize, void* address);

    int DeleteProcess(string tableName, int restrictNum, string restrict[], bool total, bool showInfo = true);
    int SelectProcess(int N_A, string attribute[], string tableName, int restrictNum, 
                      string restricts[], bool show, bool total, bool showInfo = true, vector <char*>* records = nullptr);
    int SelectProcess(int N_A, string attribute[], int tableNum, string tableName[], 
                      int restrictNum, string restricts[], bool show, bool total);
    void* SelectRecord(blockNode* blocknode, int recordSize, int offset) const;
    int FetchRecords(int N_A, string attribute[], string tableName, const vector <char*>& records);

    int GetRecordSize(string tableName);
    int GetAttrType(string tableName, string attrName);
    void* GetAttrValue(string tableName, string attrName, void* recordAddress) const;
};

#endif //API_H