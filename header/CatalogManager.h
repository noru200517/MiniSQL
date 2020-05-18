#pragma once
#ifndef CATALOG_MANAGER
#include <string>
#include <vector>
#define INT   1
#define FLOAT 2
#define CHAR  3

#define CATALOG_CREATE_FAILURE -1
#define CATALOG_WRITE_FAILURE  -2
#define TABLE_NOT_FOUND        -3
#define STATUS_ERROR           -12

#define MAX_ATTR_NUM         32
#define MAX_FILE_NAME_LENGTH 64
#define MAX_ATTR_NAME_LENGTH 64
#define MAX_CATALOG_FILE_NUM 256

#define RECORD_SIZE          4
#define ATTRIBUTE_NUM        4
#define ATTRIBUTE_NAME       2048
#define RECORD_FORMAT        64
#define RECORD_UNIQUE        32
#define PRIMARY_KEY          2048

using namespace std;

/**
 *  record format definition:
 *  int: 1
 *  float: 2
 *  char: 3 + one unsigned char, n of char(n)
 *  maximum length: 2 * MAX_RECORD_LENGTH
 */
typedef struct table{
    string tableName;                //the name of the file
    int recordSize;                 //the size of a record
    int attributeNum;               //the number of attributes
    vector <string> attributeName;  //the names of attribute
    unsigned char* recordFormat;    //the format of a record
    unsigned char* recordUnique;    //whether the record is unique
    vector <string> primaryKey;     //all primary keys defined on the table
    bool valid;                     //whether the node is in use

    table() : tableName(""), recordSize(0), attributeNum(0), recordFormat(nullptr), recordUnique(nullptr), valid(false) {};
    ~table() {
        delete recordFormat;
        delete recordUnique;
    }
}tableInfoNode;

typedef struct {
    string indexName;
    string tableName;
    string attributeName;
}indexInfoNode;

class CatalogManager {
private:
    int tableNum;
    vector <string> fileNames;
    tableInfoNode* tablePool[MAX_CATALOG_FILE_NUM];
    string GenerateCatalogName(const string tableName);
    tableInfoNode* LoadTableInfo(string tableName);
    bool WriteAllCatalogBack();
    bool WriteNodeBack(tableInfoNode* node);
    void ClearNode(tableInfoNode* node);

public:
    CatalogManager();
    ~CatalogManager();
    int GetRecordLength(string tableName);
    int CreateTableCatalog(string tableName, int recordSize, int attributeNum, vector<string> attributeName, unsigned char* recordFormat, unsigned char* recordUnique);
    int DropTableCatalog(string tableName, int position);
    tableInfoNode* GetTableInfo(string tableName);
    int FindTable(string tableName);
    int GetTableNum();
    int FindAttrPosition(string attribute, tableInfoNode* tableInfo);
    int FindAttrType(string attribute, int* size, tableInfoNode* tableInfo);
};

#endif CATALOG_MANAGER