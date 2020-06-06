#include "API.h"
#include "IndexManager.h"
#include <iostream>
#include <ctime>
using namespace std;

API::API(CatalogManager* catalogManager, BufferManager* bufferManager, RecordManager* recordManager, IndexManager* indexManager)
{
    myCatalogManager = catalogManager;
    myBufferManager = bufferManager;
    myRecordManager = recordManager;
    myIndexManager = indexManager;
}

API::~API()
{

}

/**
 *  Create a table.
 *
 *  @return int: if return 0, table creation failed.
 *  @            if return 1, table creation succeeded.
 */
int API::CreateTable(string tableName, bool showInfo)
{
    //GetTickCount, 这里做计时打印
    clock_t startTime, endTime;
    double duration = 0.0;
    int resultNum = 0;
    startTime = clock();
    resultNum =  myRecordManager->CreateTable(tableName);
    endTime = clock();
    if (showInfo) {
        duration = (double)(endTime - startTime) / CLOCKS_PER_SEC;
        if (resultNum == 1) {
            cout << "Query OK, 0 rows affected";
            cout << " (" << duration << " sec)" << endl;
            cout << endl;
        }
        else {
            cout << "Query failed" << endl;
            cout << endl;
        }
    }
    return resultNum;
}

/**
 *  Drop a table.
 *
 *  @return int: if return 0, table drop failed.
 *  @            if return 1, table drop succeeded.
 */
int API::DropTable(string tableName, bool showInfo)
{
    clock_t startTime, endTime;
    double duration = 0.0;
    int resultNum = 0;
    startTime = clock();
    resultNum = myRecordManager->DropTable(tableName);
    endTime = clock();
    if (showInfo) {
        duration = (double)(endTime - startTime) / CLOCKS_PER_SEC;
        if (resultNum == 1) {
            cout << "Query OK, 0 rows affected";
            cout << " (" << duration << " sec)" << endl;
            cout << endl;
        }
        else {
            cout << "Query failed" << endl;
            cout << endl;
        }
    }
    return resultNum;
}

tableInfoNode* API::GetTableInfo(string tableName) {
    return myCatalogManager->GetTableInfo(tableName);
}

int API::CreateIndex(string indexName, string tableName, string attrName)
{
    clock_t startTime, endTime;
    double duration = 0.0;
    int resultNum = 0;
    startTime = clock();
    resultNum = myIndexManager->CreateIndexProcess(indexName, tableName, attrName);
    endTime = clock();
    duration = (double)(endTime - startTime) / CLOCKS_PER_SEC;
    if (resultNum == 1) {
        cout << "Query OK, 0 rows affected";
        cout << " (" << duration << " sec)" << endl;
        cout << endl;
    }
    else {
        cout << "Query failed" << endl;
        cout << endl;
    }
    return resultNum;
}

int API::DropIndex(string indexName)
{
    clock_t startTime, endTime;
    double duration = 0.0;
    int resultNum = 0;
    startTime = clock();
    resultNum = myIndexManager->DropIndexProcess(indexName);
    endTime = clock();
    duration = (double)(endTime - startTime) / CLOCKS_PER_SEC;
    if (resultNum == 1) {
        cout << "Query OK, 0 rows affected";
        cout << " (" << duration << " sec)" << endl;
        cout << endl;
    }
    else {
        cout << "Query failed" << endl;
        cout << endl;
    }
    return resultNum;
}

bool API::HasIndex(string tableName, string attrName) {
    return myIndexManager->HasIndex(tableName, attrName);
}

bool API::HasIndex(string indexName) {
    return myIndexManager->HasIndex(indexName);
}

int API::SetIndex(string tableName, string attrName, bool hasIndex) {
    return myCatalogManager->SetIndex(tableName, attrName, hasIndex);
}

bool API::IsDelete(blockNode* blocknode, int recordSize, int offset) const
{
    return myRecordManager->IsDelete(blocknode, recordSize, offset);
}

int API::InsertProcess(int attrNum, string attrVal[], string tableName, bool showInfo)
{
    clock_t startTime, endTime;
    double duration = 0.0;
    int resultNum;
    startTime = clock();
    resultNum = myRecordManager->InsertProcess(attrNum, attrVal, tableName);
    endTime = clock();
    if (showInfo) {
        duration = (double)(endTime - startTime) / CLOCKS_PER_SEC;
        if (resultNum == 1) {
            cout << "Query OK, 1 rows affected";
            cout << " (" << duration << " sec)" << endl;
            cout << endl;
        }
        else {
            cout << "Query failed" << endl;
            cout << endl;
        }
    }
    return resultNum;
}

int API::InsertIndexRecord(string tableName, string attrName, string attrVal, int recordSize, void* address) {
    string indexName = myIndexManager->GetIndexName(tableName, attrName);
    return myIndexManager->InsertRecord(indexName, attrVal, recordSize, address);
}

int API::InsertOperation(blockNode* blocknode, void* record, int recordSize)
{
    myRecordManager->InsertOperation(blocknode, record, recordSize);
    return 0;
}

int API::DeleteProcess(string tableName, int restrictNum, string restrict[], bool total, bool showInfo)
{
    clock_t startTime, endTime;
    double duration = 0.0;
    int resultNum;
    startTime = clock();
    resultNum = myRecordManager->DeleteProcess(tableName, restrictNum, restrict, total);
    endTime = clock();
    if (showInfo) {
        duration = (double)(endTime - startTime) / CLOCKS_PER_SEC;
        if (resultNum > 0) {
            cout << "Query OK, " << resultNum << " rows affected";
            cout << " (" << duration << " sec)" << endl;
            cout << endl;
        }
        else {
            cout << "Query failed" << endl;
            cout << endl;
        }
    }
    return resultNum;
}

int API::SelectProcess(int N_A, string attribute[], string tableName, int restrictNum, 
                       string restricts[], bool show, bool total, bool showInfo, vector<char*>* records)
{
    clock_t startTime, endTime;
    double duration = 0.0;
    int resultNum;

    tableInfoNode* tableInfo = myCatalogManager->GetTableInfo(tableName);
    //这里有点点问题，应该是用restrict中的值做的
    //小心给你返回负值，你还不做错误处理，分分钟崩盘
    //如果我有两个where的restrictions，其中一个有index，那么我应该是想用它得到的东西继续做事情。
    //先处理简单的，只有一个index的情况吧。
    if (!total && restricts[1] != "<>") {
        int pos = myCatalogManager->FindAttrPosition(tableName, restricts[0]);
        if (pos == -1) {
            cerr << "Error: this attribute does not exist" << endl;
            cerr << "Query failed" << endl;
            cerr << endl;
        }
        if (tableInfo->attrInfo[INDEX][pos]) {
            string indexAttr = tableInfo->attrName[pos];
            startTime = clock();
            resultNum = myIndexManager->SelectProcess(N_A, attribute, indexAttr, tableName, restricts, true, false);
            endTime = clock();
        }
        else {
            startTime = clock();
            resultNum = myRecordManager->SelectProcess(N_A, attribute, tableName, restrictNum, restricts, show, total, records);
            endTime = clock();
        }
    }
    else {
        startTime = clock();
        resultNum = myRecordManager->SelectProcess(N_A, attribute, tableName, restrictNum, restricts, show, total, records);
        endTime = clock();
    }
    if (showInfo) {
        duration = (double)(endTime - startTime) / CLOCKS_PER_SEC;
        if (resultNum > 0) {
            cout << "Query OK, " << resultNum << " rows in set";
            cout << " (" << duration << " sec)" << endl;
            cout << endl;
        }
        else {
            cout << "Query failed" << endl;
            cout << endl;
        }
    }
    return resultNum;
}

int API::SelectProcess(int N_A, string attribute[], int tableNum, string tableName[], int restrictNum, string restricts[], bool show, bool total)
{
    myRecordManager->SelectProcess(N_A, attribute, tableNum, tableName, restrictNum, restricts, show, total);
    return 0;
}

void* API::SelectRecord(blockNode* blocknode, int recordSize, int offset) const
{
    return myRecordManager->SelectRecord(blocknode, recordSize, offset);
}

int API::FetchRecords(int N_A, string attribute[], string tableName, const vector <char*>& records) {
    return myRecordManager->FetchRecords(N_A, attribute, tableName, records);
}

int API::GetRecordSize(string tableName)
{
    return myCatalogManager->GetRecordSize(tableName);
}

int API::GetAttrType(string tableName, string attrName)
{
    return myCatalogManager->GetAttrType(tableName, attrName);
}

void* API::GetAttrValue(string tableName, string attrName, void* recordAddress) const
{
    return myRecordManager->GetAttrValue(tableName, attrName, recordAddress);
}

