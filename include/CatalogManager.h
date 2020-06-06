#pragma once
#ifndef CATALOG_MANAGER_H

#include <string>
#include <vector>
#define INT   1
#define FLOAT 2
#define CHAR  3

#define CATALOG_CREATE_FAILURE -1
#define CATALOG_WRITE_FAILURE  -2
#define TABLE_NOT_FOUND        -3
#define STATUS_ERROR           -12

#define MAX_ATTR_NUM          32
#define MAX_FILE_NAME_LENGTH  64
#define MAX_ATTR_NAME_LENGTH  64
#define MAX_INDEX_NAME_LENGTH 32
#define MAX_CATALOG_FILE_NUM  256

#define RECORD_SIZE          4
#define ATTRIBUTE_NUM        4
#define ATTRIBUTE_NAME       2048
#define RECORD_FORMAT        64
#define RECORD_UNIQUE        32
#define PRIMARY_KEY          2048

#define TYPE   0
#define LENGTH 1
#define UNIQUE 2
#define PK     3
#define INDEX  4
using namespace std;

/**
 *  record format definition:
 *  int: 1
 *  float: 2
 *  char: 3
 */
typedef struct table{
    string tableName;               //the name of the file
    string* attrName;               //the names of attribute
    int attrNum;                    //the number of attributes
    int recordSize;                 //the size of a record
    unsigned char **attrInfo;  
    /*
    unsigned char* recordFormat;    //the format of a record
    unsigned char* recordUnique;    //whether the record is unique
    vector <string> primaryKey;     //all primary keys defined on the table
    */
    bool valid;                     //whether the node is in use
    vector <pair<string, string>> indexName;      //the names of index (format: attrName, indexName)

    table() : tableName(""), attrName(nullptr), attrNum(0), recordSize(0), attrInfo(nullptr), valid(false) {};
    table(string mTableName, string* mAttrName, int mAttrNum, unsigned char** mAttrInfo, 
        vector<pair<string, string>> mIndexName = vector<pair<string, string>>()):
        tableName(mTableName),
        attrName(mAttrName),
        attrNum(mAttrNum),
        recordSize(0),
        attrInfo(mAttrInfo),
        valid(true),
        indexName(mIndexName)
    {
        
    }
    table(string mTableName, string* mAttrName, int mAttrNum, int mRecordSize, unsigned char** mAttrInfo) :
        tableName(mTableName),
        attrName(mAttrName),
        indexName(vector <pair<string, string> > (mAttrNum)),
        attrNum(mAttrNum),
        recordSize(mRecordSize),
        attrInfo(mAttrInfo),
        valid(true) {
        for (int i = 0; i < attrNum; i++) {
            indexName[i] = make_pair("", "");
        }
    }
    ~table() {
        //ÕâÓÐµã¸´ÔÓ¡£¡£
    }
}tableInfoNode;

class API;
class CatalogManager {
private:
    int tableNum;
    vector <string> fileNames;
    tableInfoNode* tablePool[MAX_CATALOG_FILE_NUM];
    API* myAPI;
private:
    string GenerateCatalogName(const string tableName);
    tableInfoNode* LoadTableInfo(string tableName);
    void ProcessRecordFormat(int attrNum, tableInfoNode* newNode, unsigned char*& recordFormat, unsigned char* isUnique, unsigned char* isPrimaryKey, unsigned char* hasIndex);
    void ProcessTableInfo(void* buffer, tableInfoNode* newNode);
    int CalculateRecordSize(tableInfoNode* node);
    int GetIndexNum(tableInfoNode* node);
    bool WriteAllCatalogBack();
    bool WriteNodeBack(tableInfoNode* node);
    void ClearNode(tableInfoNode* node);

public:
    CatalogManager();
    ~CatalogManager();

    //for record & buffer manager
    int FindTable(string tableName);
    bool FindAttr(string tableName, string attrName);
    int FindAttrPosition(string tableName, string attrName);
    int GetTableNum();

    //for API 
    void SetAPI(API* api);
    int CreateTableCatalog(tableInfoNode* node);
    int DropTableCatalog(string tableName);
    tableInfoNode* GetTableInfo(string tableName); //GetTableMessage02
    int GetAttrNum(string tableName);              //GetTableMessage01
    int CreateIndex(string tableName, string attrName, string indexName);
    int DropIndex(string tableName, string indexName);
    int HasIndex(string tableName, string attrName);
    int SetIndex(string tableName, string attrName, bool hasIndex);

    //for index manager 
    int GetRecordSize(string tableName);
    int GetAttrType(string tableName, string attrName);
};

#endif //CATALOG_MANAGER_H