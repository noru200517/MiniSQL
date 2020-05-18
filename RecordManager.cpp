#include "RecordManager.h"
#include <iostream>
using namespace std;

RecordManager::RecordManager()
{
       
}

RecordManager::RecordManager(BufferManager* myBM) : myBufferManager(myBM) 
{

}

RecordManager::~RecordManager()
{

}

/**
 *  create a table file.
 *
 *  @param string tableName: name of the table
 *  @return int: number of table effected
 */
int RecordManager::CreateTable(string tableName)
{
    FILE* fileHandle;
    string fullTableName = GenerateTableName(tableName);

    fopen_s(&fileHandle, fullTableName.c_str(), "w+");
    if (fileHandle == nullptr) {
        return 0;
    } 
    fclose(fileHandle);
    return 1;
}

/**
 *  drop a table file.
 *
 *  @param string tableName: name of the table
 *  @return int: number of table effected
 */
int RecordManager::DropTable(string tableName)
{
    string fullTableName = GenerateTableName(tableName);
    fileNode* filenode;
    blockNode* blocknode;
    int position = 0;

    if (remove(fullTableName.c_str())) {
        return 0;
    }
    position = myBufferManager->FindBlock(tableName);
    if (position == FILE_NOT_LOADED) {
        return 1;
    }
    else {
        blocknode = myBufferManager->GetFirstBlock(tableName);
        blocknode->filenode = nullptr;
        blocknode->isDirty = false;
        blocknode->isPinned = false;
        blocknode->offset = 0;
        blocknode->recordAddress = nullptr;
        blocknode->recordNum = 0;
        blocknode->sizeUsed = 0;
        memset(blocknode->deleted, 0, MAX_ATTR_NUM);
        myBufferManager->DropFile(tableName);
        return 1;
    }
}

/**
 *  create an index file.
 *
 *  @param string tableName: name of the table
 *  @return int: number of index effected
 */
int RecordManager::CreateIndex(string indexName)
{
    FILE* fileHandle;
    string fullIndexName = GenerateIndexName(indexName);

    fopen_s(&fileHandle, fullIndexName.c_str(), "w+");
    if (fileHandle == nullptr) {
        return 0;
    }
    fclose(fileHandle);
    return 1;
}

/**
 *  drop an index file.
 *
 *  @param string tableName: name of the table
 *  @return int: number of index effected
 */
int RecordManager::DropIndex(string indexName)
{
    string fullIndexName = GenerateIndexName(indexName);
    if (remove(fullIndexName.c_str())) {
        return 0;
    }
    return 1;
}

int RecordManager::InsertRecord(blockNode* blocknode, void* record, int recordSize)
{
    int sizeUsed = blocknode->sizeUsed;
    void* position;

    //move to the end of the record in the block 
    position = blocknode->recordAddress;
    position = (char*)position + sizeUsed;

    //write the record into the block
    memcpy(position, record, recordSize);

    //update information
    blocknode->sizeUsed += recordSize;
    blocknode->recordNum++;
    blocknode->deleted[blocknode->recordNum] = 0;
    blocknode->isDirty = true;

    return 1;
}

int RecordManager::DeleteRecord(blockNode* blocknode)
{

    return 0;
}

void* RecordManager::SelectRecord(blockNode* blocknode, int recordSize, int offset)
{
    int pos = offset / recordSize;
    void* recordAddress = blocknode->recordAddress;
    void* buffer = (void*)new char[recordSize];

    if (buffer == nullptr) {
        cerr << "Error: memory allocation failed" << endl;
        return nullptr;
    }
    recordAddress = (void*)((char*)recordAddress + offset);
    if (blocknode->deleted[pos] == 0) {
        memcpy(buffer, recordAddress, recordSize);
        return buffer;
    }
    else return nullptr;
}

bool RecordManager::RecordMatch(int intVal, int op, string val)
{
    int value = atoi(val.c_str());

    switch (op) {
    case EQUAL:
        if (intVal == value)return true;
        else return false;
    case UNEQUAL:
        if (intVal != value)return true;
        else return true;
    case LESS_THAN:
        if (intVal < value)return true;
        else return false;
    case GREATER_THAN:
        if (intVal > value)return true;
        else return false;
    case LESS_THAN_OR_EQUAL:
        if (intVal <= value)return true;
        else return false;
    case GREATER_THAN_OR_EQUAL:
        if (intVal >= value)return true;
        else return false;
    }
}

bool RecordManager::RecordMatch(float floatVal, int op, string val)
{
    float value = atof(val.c_str());

    switch (op) {
    case EQUAL:
        if (floatVal == value)return true;
        else return false;
    case UNEQUAL:
        if (floatVal != value)return true;
        else return true;
    case LESS_THAN:
        if (floatVal < value)return true;
        else return false;
    case GREATER_THAN:
        if (floatVal > value)return true;
        else return false;
    case LESS_THAN_OR_EQUAL:
        if (floatVal <= value)return true;
        else return false;
    case GREATER_THAN_OR_EQUAL:
        if (floatVal >= value)return true;
        else return false;
    }
}

bool RecordManager::RecordMatch(char* charVal, int op, string val, int attrSize)
{
    string charval = ((string)charVal).substr(0, attrSize);

    switch (op) {
    case EQUAL:
        if (charval == val)return true;
        else return false;
    case UNEQUAL:
        if (charval != val)return true;
        else return true;
    case LESS_THAN:
        if (charval < val)return true;
        else return false;
    case GREATER_THAN:
        if (charval > val)return true;
        else return false;
    case LESS_THAN_OR_EQUAL:
        if (charval <= val)return true;
        else return false;
    case GREATER_THAN_OR_EQUAL:
        if (charval >= val)return true;
        else return false;
    }
}

string RecordManager::GenerateTableName(string tableName) {
    string tablePrefix = "TABLE_";
    tableName = tablePrefix + tableName;
    return tableName;
}

string RecordManager::GenerateIndexName(string indexName) {
    string indexPrefix = "INDEX_";
    indexName = indexPrefix + indexName;
    return indexName;
}

