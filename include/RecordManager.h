#pragma once

#ifndef RECORD_MANAGER

#include <string>
#include "BufferManager.h"
#include "CatalogManager.h"
using namespace std;

#define MAX_CHAR_LENGTH   256

//由于新的传入需求，这里也许不是很必要
#define EQUAL                   0x21
#define UNEQUAL                 0x22
#define LESS_THAN               0x23
#define GREATER_THAN            0x24
#define LESS_THAN_OR_EQUAL      0x25
#define GREATER_THAN_OR_EQUAL   0x26

class API;
class RecordManager {
private:
    BufferManager* myBufferManager;
    CatalogManager* myCatalogManager;
    API* myAPI;
    string GenerateTableName(string tableName);
    int FindRecord(blockNode* blocknode, void* record, int recordSize);
    bool CompareRecord(char* recordToCheck, char* buffer, int recordSize);
    void SetDelete(blockNode* blocknode, int pos);
    int DeleteAll(string tableName);
    bool TestUnique(string tableName, string attrName, string attrVal);
    void ProcessRecord(int attrNum, string attrVal[], void* record, tableInfoNode* tableInfo);
    int OpTypeCast(string attrOp);
    void UpdateLRUBlock(blockNode* node);
    bool RecordMatch(int intVal, int op, wstring val);
    bool RecordMatch(float floatVal, int op, wstring val);
    bool RecordMatch(wstring charVal, int op, wstring val);
    void ReleaseRecords(vector<char*> *records);
    int FetchRecords(int N_A, string attribute[], string tableName, bool show, vector <char*>* records = nullptr);
    int FetchRecords(int N_A, string attribute[], int tableNum, string*& tableName, int*& recordSize, 
                     const vector<char*>& recordConcat, bool show);
    vector <char*> ConcatenateRecord(int recordConcatSize, vector <char*>& recordConcat, int recordSize, 
                                     vector <char*>& records);
    void PrintHline(vector < pair<string, int> > tableHead) const;
    void PrintHead(vector<pair<string, int>> tableHead) const;
    void ShowBody(int N_A, vector<string> attrNames, vector<tableInfoNode*>& tableNames,
                  const vector <char*>& records, vector < pair<string, int> >& tableHead, int*& offset) const;
    void ShowCell(int intVal, int length) const;
    void ShowCell(float floatVal, int length) const;
    void ShowCell(wstring charVal, int length) const;
    int CalculateCharSize(wstring& charVal) const;
    //index manager直接和API交互，应该不需要record manager再做处理
    //string GenerateIndexName(string indexName);

public:
    RecordManager();
    RecordManager(BufferManager* myBM, CatalogManager* myCM);
    ~RecordManager();
    void SetAPI(API* api);

    int CreateTable(string tableName);
    int DropTable(string tableName);
    /*
    //index manager直接和API交互，应该不需要record manager再做处理
    int CreateIndex(string indexName);
    int DropIndex(string indexName);
    */
    int InsertProcess(int attrNum, string attrVal[], string tableName);
    int InsertOperation(blockNode* blocknode, void* record, int recordSize);
    int DeleteProcess(string tableName, int restrictNum, string restrict[], bool total);
    bool IsDelete(blockNode* blocknode, int recordSize, int offset) const;

    //void* SelectRecord(blockNode* blocknode, tableInfoNode* tableInfo, int op, string attribute, string val);
    //这两个函数其实可以合并成一个，就是一开始没设计到。
    int SelectProcess(int N_A, string attribute[], string tableName, int restrictNum, string restricts[], bool show, bool total, vector <char*>* records = nullptr);
    int SelectProcess(int N_A, string attribute[], int tableNum, string tableName[], int restrictNum, string restricts[], bool show, bool total);
    void* SelectRecord(blockNode* blocknode, int recordSize, int offset) const;
    int FetchRecords(int N_A, string attribute[], string tableName, const vector <char*>& records);
    void* GetAttrValue(string tableName, string attrName, void* recordAddress) const;
};

#endif //RECORD_MANAGER