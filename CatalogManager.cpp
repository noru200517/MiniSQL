#include "CatalogManager.h"
#include <iostream>
using namespace std;

/**
 *  Generate catalog file's filename (add a prefix CATALOG_FILE_)
 *
 *  @param const string tableName: name of the table
 *  @return: name of the catalog file
 *
 */
string CatalogManager::GenerateCatalogName(const string tableName)
{
    string fullCatalogName = "CATALOG_FILE_" + tableName;
    return fullCatalogName;
}

/**
 *  Load table'information into a tableInfoNode, and insert the node into tablePool
 *
 *  @param string tableName: name of the table
 *  @return: a pointer to the tableInfoNode
 *
 */
tableInfoNode* CatalogManager::LoadTableInfo(string tableName)
{
    string fullCatalogName = GenerateCatalogName(tableName);
    FILE* fileHandle;
    char* buffer = new char[2048];
    tableInfoNode* newNode = new tableInfoNode();
    memset(buffer, 0, 2048);

    if (buffer == nullptr || newNode == nullptr) {
        cerr << "Error: memory allocation failed" << endl;
        return nullptr;
    }

    fopen_s(&fileHandle, fullCatalogName.c_str(), "rb");
    if (fileHandle == nullptr) {
        cerr << "Error: failed to open catalog file" << endl;
        return nullptr;
    }

    //get tableName, which is specified by the parameter tableName
    newNode->tableName = tableName;
    fseek(fileHandle, MAX_FILE_NAME_LENGTH, SEEK_SET); 

    //get record size
    fread(buffer, 4, 1, fileHandle);
    int recordSize = *(int*)buffer;
    newNode->recordSize = recordSize;

    //get attribute number
    fread(buffer, 4, 1, fileHandle);
    int attributeNum = *(int*)buffer;
    newNode->attributeNum = attributeNum;
    memset(buffer, 0, 4);

    //get attribute names
    string name;
    for (int i = 0; i < attributeNum; i++) {
        fread(buffer, 1, 64, fileHandle);
        name = (string)buffer;
        memset(buffer, 0, 64);
        newNode->attributeName.push_back(name);
    }

    //get record format
    unsigned char* recordFormat = new unsigned char[64];
    fread(recordFormat, 1, 64, fileHandle);
    newNode->recordFormat = recordFormat;

    //get record of unique attributes
    unsigned char* recordUnique = new unsigned char[32];
    fread(recordUnique, 1, 32, fileHandle);
    newNode->recordUnique = recordUnique;

    //set valid bit to true
    newNode->valid = true;

    fclose(fileHandle);
    return newNode;
}

/**
 *   When tablePool is full, write all nodes back into files
 *   
 *   @return: bool (true -> successful, false -> failed)
 *
 */
bool CatalogManager::WriteAllCatalogBack()
{
    for (int i = 0; i < MAX_CATALOG_FILE_NUM; i++) {
        if (tablePool[i]->valid == false) {
            ClearNode(tablePool[i]);
        }
        else {
            if (WriteNodeBack(tablePool[i]) == false) {
                return false;
            }
            ClearNode(tablePool[i]);
        }
    }
}

/**
 *  Write a node back into the corresponding catalog file
 *
 *  @param tableInfoNode* node: the node to write back
 *  @return: bool (true -> successful, false -> failed)
 */
bool CatalogManager::WriteNodeBack(tableInfoNode* node)
{
    string fileName = GenerateCatalogName(node->tableName);
    FILE* fileHandle;
    void* buffer = (void*)(new char[2048]);
    memset(buffer, 0, 2048);

    //open the file
    fopen_s(&fileHandle, fileName.c_str(), "wb+");
    if (fileHandle == nullptr) {
        cerr << "Error: failed to open catalog file" << endl;
        return false;
    }
    
    //write tableName
    int tableNameSize = node->tableName.size();
    memcpy(buffer, node->tableName.c_str(), tableNameSize);
    fseek(fileHandle, 0, SEEK_SET);
    fwrite(buffer, 1, tableNameSize, fileHandle);
    memset(buffer, 0, 64);

    //write recordSize
    fseek(fileHandle, MAX_FILE_NAME_LENGTH - tableNameSize, SEEK_CUR);
    memcpy(buffer, &node->recordSize, 4);
    fwrite(buffer, 4, 1, fileHandle);
    memset(buffer, 0, 4);

    //write attributeNum
    memcpy(buffer, &node->attributeNum, 4);
    fwrite(buffer, 4, 1, fileHandle);
    memset(buffer, 0, 4);

    //write attributeName
    int attrSize;
    for (int i = 0; i < node->attributeNum; i++) {
        attrSize = node->attributeName[i].size();
        memcpy(buffer, node->attributeName[i].c_str(), attrSize);
        fwrite(buffer, 1, attrSize, fileHandle);
        memset(buffer, 0, MAX_FILE_NAME_LENGTH);
        fseek(fileHandle, MAX_FILE_NAME_LENGTH - attrSize, SEEK_CUR);
    }

    //write recordFormat
    memcpy(buffer, node->recordFormat, 64);
    fwrite(buffer, 1, 64, fileHandle);
    memset(buffer, 0, 64);

    //write recordUnique
    memcpy(buffer, node->recordUnique, 32);
    fwrite(buffer, 1, 32, fileHandle);
    memset(buffer, 0, 32);

    fclose(fileHandle);

    return true;
}

/**
 *  Clear the tableInfo node, erase all information
 *
 *  @param tableInfoNode* node: the node to clear
 *
 */
void CatalogManager::ClearNode(tableInfoNode* node)
{
    node->attributeName.clear();
    node->primaryKey.clear();
    node->recordFormat = nullptr;
    node->recordUnique = nullptr;
    node->tableName = "";
    node->recordSize = node->attributeNum = 0;
    return;
}

/**
 *  Load information about all tables.
 */
CatalogManager::CatalogManager() : tableNum(0)
{
    FILE* fileHandle;
    int offset = 0;
    string allTable = GenerateCatalogName("ALL_TABLE");
    string tableName;
    char* buffer = new char[MAX_FILE_NAME_LENGTH];
    memset(buffer, 0, MAX_FILE_NAME_LENGTH);

    for (int i = 0; i < MAX_CATALOG_FILE_NUM; i++) {
        tablePool[i] = new tableInfoNode();
    }

    //load names of files into the vector
    fopen_s(&fileHandle, allTable.c_str(), "ab+");
    if (fileHandle == nullptr) {
        cerr << "Error: read catalog info failed" << endl;
        exit(1);
    }
    while (!feof(fileHandle)) {
        fseek(fileHandle, offset, SEEK_SET);
        offset += MAX_FILE_NAME_LENGTH;
        if (fread(buffer, 1, MAX_FILE_NAME_LENGTH, fileHandle) != 0) {
            tableName = (string)buffer;

            /*test
            cout << "Table name is: " << tableName << endl;*/

            fileNames.push_back(tableName);
            memset(buffer, 0, MAX_FILE_NAME_LENGTH);
        }
        else break;
    }
    fclose(fileHandle);
}

/**
 *  Write information about all tables back into the file.
 */
CatalogManager::~CatalogManager()
{
    FILE* fileHandle;
    string allTable = GenerateCatalogName("ALL_TABLE");
    void* buffer = (void*)new char[MAX_FILE_NAME_LENGTH];
    memset(buffer, 0, MAX_FILE_NAME_LENGTH);
    int offset = MAX_FILE_NAME_LENGTH;

    fopen_s(&fileHandle, allTable.c_str(), "w+");
    if (fileHandle == nullptr) {
        cerr << "Error: write catalog info failed" << endl;
        exit(1);
    }
    //write fileNames back
    fseek(fileHandle, 0, SEEK_SET);
    for (int i = 0; i < fileNames.size(); i++) {
        memcpy(buffer, fileNames[i].c_str(), fileNames[i].size());
        fwrite(buffer, 1, fileNames[i].size(), fileHandle);
        fseek(fileHandle, offset, SEEK_SET);
        offset += MAX_FILE_NAME_LENGTH;
    }
    fclose(fileHandle);
    WriteAllCatalogBack();

    for (int i = 0; i < MAX_CATALOG_FILE_NUM; i++) {
        delete tablePool[i];
    }
}

/**
 *  Get the length of one record for the table.
 *
 *  @param string tableName: name of the table
 *  @return int: the length of its record
 *
 */
int CatalogManager::GetRecordLength(string tableName)
{
    int found = FindTable(tableName);
    tableInfoNode* node;

    if (found == TABLE_NOT_FOUND) {
        cerr << "Error: table has not been created" << endl;
        return TABLE_NOT_FOUND;
    }
    else {
        node = GetTableInfo(tableName);
        return node->recordSize;
    }
}

/**
 *  Create a catalog record for the table.
 *
 *  @params: information needed to create a tableInfoNode.
 *  @return int: 0 -> successful, negative -> failed
 *
 */
int CatalogManager::CreateTableCatalog(string tableName, int recordSize, int attributeNum, vector<string> attributeName, unsigned char* recordFormat, unsigned char* recordUnique)
{
    //create a catalog file
    FILE* fileHandle;
    string fullCatalogName = GenerateCatalogName(tableName);
    fopen_s(&fileHandle, fullCatalogName.c_str(), "wb+");
    if (fileHandle == nullptr) {
        cerr << "Error: create catalog file failed" << endl;
        return CATALOG_CREATE_FAILURE;
    }
    fclose(fileHandle);

    //record the name of the file in vector fileNames
    fileNames.push_back(tableName);

    //update information in tablePool
    if (tableNum == MAX_CATALOG_FILE_NUM) {     //buffer full
        if (!WriteAllCatalogBack()) {
            return CATALOG_WRITE_FAILURE;
        }
    }                             //buffer not full
    for (int i = 0; i < MAX_CATALOG_FILE_NUM; i++) {
        if (tablePool[i]->valid == false) {
            tablePool[i]->attributeName = attributeName;
            tablePool[i]->attributeNum = attributeNum;
            tablePool[i]->tableName = tableName;
            tablePool[i]->recordFormat = recordFormat;
            tablePool[i]->recordSize = recordSize;
            tablePool[i]->recordUnique = recordUnique;
            tablePool[i]->valid = true;
            tableNum++;
            break;
        }
    }
    return 0;
}

/**
 *  @return 0: failed
 */
int CatalogManager::DropTableCatalog(string tableName, int position)
{
    //erase file
    string fullCatalogName = GenerateCatalogName(tableName);
    if (remove(fullCatalogName.c_str())) {
        cerr << "Error: failed to remove the catalog file" << endl;
        return 0;
    }

    //erase the record of this file
    vector <string>::iterator iter = fileNames.begin() + position;
    fileNames.erase(iter);

    //mark the file in tablePool(if exists) as invalid, so its position can be taken
    for (int i = 0; i < MAX_CATALOG_FILE_NUM; i++) {
        if (tablePool[i]->tableName.compare(tableName) == 0) {
            tablePool[i]->valid = false;
            break;
        }
    }
    tableNum--;

    return 1;
}

tableInfoNode* CatalogManager::GetTableInfo(string tableName)
{
    int index = -1;
    int firstInvalidPos = -1;
    bool exist = false;
    tableInfoNode* newNode = nullptr;

    //decide whether the table exists
    for (int i = 0; i < fileNames.size(); i++) {
        if (fileNames[i].compare(tableName) == 0)exist = true;
    }
    if (!exist) {
        cerr << "Error: catalog file does not exist" << endl;
        return nullptr;
    }

    //if the tablePool is full, write all records back
    if (tableNum == MAX_CATALOG_FILE_NUM) {
        WriteAllCatalogBack();
    }
    //find the tableNode and the first invalid position
    for (int i = 0; i < MAX_CATALOG_FILE_NUM; i++) {
        if (tablePool[i]->tableName.compare(tableName) == 0) {
            index = i;
        }
        if (tablePool[i]->valid == false) {
            firstInvalidPos = i;
        }
        if (firstInvalidPos != -1 && index != -1)break;
    }

    if (index == -1) {      //the tableNode does not exist
        newNode = LoadTableInfo(tableName);
        tablePool[firstInvalidPos] = newNode;
        return tablePool[firstInvalidPos];
    }
    else {
        return tablePool[index];
    }

    return nullptr;
}

/*
    If the record of the table is found, return its potion in vector fileNames
    If it is not found, return TABLE_NOT_FOUND
*/
int CatalogManager::FindTable(string tableName)
{
    int index = -1;
    for (int i = 0; i < fileNames.size(); i++) {
        if (tableName.compare(fileNames[i]) == 0) {
            index = i;
            break;
        }
    }
    if (index == -1)return TABLE_NOT_FOUND;
    else return index;
}

int CatalogManager::GetTableNum()
{
    return fileNames.size();
}

int CatalogManager::FindAttrPosition(string attribute, tableInfoNode* tableInfo)
{
    int index = -1;
    int position = 0;
    unsigned char* buffer;

    if (tableInfo == nullptr || attribute == "") {
        cerr << "Error: fail to fetch table info" << endl;
        return STATUS_ERROR;
    }

    for (int i = 0; i < tableInfo->attributeNum; i++) {
        if (tableInfo->attributeName[i].compare(attribute) == 0) {
            index = i;
            break;
        }
    }
    //return if the attribute name is not found
    if (index == -1) {
        cerr << "Error: attribute not found" << endl;
        return STATUS_ERROR;
    }
    //calculate the position
    buffer = tableInfo->recordFormat;
    for (int i = 0; i < index; i++) {
        switch (buffer[i * 2]) {
        case INT:
            position += sizeof(int);
            break;
        case FLOAT:
            position += sizeof(float);
            break;
        case CHAR:
            position += (int)buffer[i * 2 + 1];
            break;
        }
    }
    return position;
}

int CatalogManager::FindAttrType(string attribute, int* size, tableInfoNode* tableInfo)
{
    int index = -1;
    int position = 0;
    unsigned char* buffer;

    if (tableInfo == nullptr || attribute == "") {
        cerr << "Error: fail to fetch table info" << endl;
        return STATUS_ERROR;
    }

    for (int i = 0; i < tableInfo->attributeNum; i++) {
        if (tableInfo->attributeName[i].compare(attribute) == 0) {
            index = i;
            break;
        }
    }
    //return if the attribute name is not found
    if (index == -1) {
        cerr << "Error: attribute not found" << endl;
        return STATUS_ERROR;
    }

    buffer = tableInfo->recordFormat;
    if (buffer[2 * index] == CHAR) {
        *size = buffer[2 * index + 1];
    }
    return buffer[2 * index];

    return 0;
}

