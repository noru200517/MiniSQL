#include "CatalogManager.h"
#include "API.h"
#include <iostream>
#include <stdexcept>
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
    tableInfoNode* newNode;
    char* buffer = nullptr;

    try {
        newNode = new tableInfoNode();
        buffer = new char[2048];
        memset(buffer, 0, 2048);
        fopen_s(&fileHandle, fullCatalogName.c_str(), "rb");
        if (fileHandle == nullptr)
            throw fullCatalogName.c_str();

        //get tableName, which is specified by the parameter tableName
        newNode->tableName = tableName;
        fseek(fileHandle, MAX_FILE_NAME_LENGTH, SEEK_SET);

        //get record size
        fread(buffer, sizeof(int), 1, fileHandle);

        int recordSize = *(int*)buffer;
        newNode->recordSize = recordSize;
        memset(buffer, 0, sizeof(int));

        //get attribute number
        fread(buffer, sizeof(int), 1, fileHandle);
        int attributeNum = *(int*)buffer;
        newNode->attrNum = attributeNum;
        memset(buffer, 0, sizeof(int));

        //get attribute names
        string name;
        newNode->attrName = new string[attributeNum];
        for (int i = 0; i < attributeNum; i++) {
            fread(buffer, 1, 64, fileHandle);
            name = (string)buffer;
            memset(buffer, 0, 64);
            (newNode->attrName)[i] = name;
        }

        //get record format
        unsigned char* recordFormat = new unsigned char[64]();
        unsigned char* isUnique = new unsigned char[32]();
        unsigned char* isPrimaryKey = new unsigned char[32]();
        unsigned char* hasIndex = new unsigned char[32]();
        fread(recordFormat, 1, 64, fileHandle);
        fread(isUnique, 1, 32, fileHandle);
        fread(isPrimaryKey, 1, 32, fileHandle);
        fread(hasIndex, 1, 32, fileHandle);

        newNode->attrInfo = new unsigned char* [5];
        for (int i = 0; i < 5; i++) {
            newNode->attrInfo[i] = new unsigned char[32]();
        }
        ProcessRecordFormat(attributeNum, newNode, recordFormat, isUnique, isPrimaryKey, hasIndex);

        //get index names, refill vector<string, string>indexName
        //in-file format: the first bit -> position, the 32 bits followed -> index name
        //vec format: vector<attrName, indexName>
        int indexNum = GetIndexNum(newNode);
        unsigned char pos = 0;
        string attrName = "", indexName = "";
        for (int i = 0; i < indexNum; i++) {
            fread(&pos, 1, 1, fileHandle);
            attrName = newNode->attrName[pos];
            memset(buffer, 0, 32);
            fread(buffer, 1, 32, fileHandle);
            indexName = (string)buffer;
            newNode->indexName.push_back(make_pair(attrName, indexName));
        }

        //set valid bit to true
        newNode->valid = true;

        fclose(fileHandle);
        delete[] buffer;
        return newNode;
    } 
    catch (bad_alloc & e) {
        cerr << e.what() << endl;
        return nullptr;
    }
    catch (char* s) {
        cerr << "Open catalog " << fullCatalogName << "failed" << endl;
        if(buffer) delete[] buffer;
        return nullptr;
    }
}

int CatalogManager::GetIndexNum(tableInfoNode* node) {
    int indexNum = 0;
    for (int i = 0; i < node->attrNum; i++) {
        if (node->attrInfo[INDEX][i]) {
            indexNum++;
        }
    }
    return indexNum;
}

void CatalogManager::ProcessRecordFormat(int attrNum, tableInfoNode* newNode, unsigned char*& recordFormat, unsigned char* isUnique, unsigned char* isPrimaryKey, unsigned char* hasIndex) {
    
    for (int i = 0; i < attrNum; i++) {
        newNode->attrInfo[0][i] = recordFormat[i * 2];
        newNode->attrInfo[1][i] = recordFormat[i * 2 + 1];
        newNode->attrInfo[2][i] = isUnique[i];
        newNode->attrInfo[3][i] = isPrimaryKey[i];
        newNode->attrInfo[4][i] = hasIndex[i];
    }
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
        //if this table has been deleted
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
    string fullCatalogName = GenerateCatalogName(node->tableName);
    FILE* fileHandle;
    void* buffer;

    try {
        buffer = (void*)(new char[2048]);
        memset(buffer, 0, 2048);

        //open the file
        fopen_s(&fileHandle, fullCatalogName.c_str(), "wb+");
        if (fileHandle == nullptr) 
            throw fullCatalogName.c_str();

        //write tableName
        int tableNameSize = node->tableName.size();
        memcpy(buffer, node->tableName.c_str(), tableNameSize);
        fseek(fileHandle, 0, SEEK_SET);
        fwrite(buffer, 1, MAX_FILE_NAME_LENGTH, fileHandle);
        memset(buffer, 0, MAX_FILE_NAME_LENGTH);

        //write recordSize
        fseek(fileHandle, MAX_FILE_NAME_LENGTH, SEEK_SET);
        memcpy(buffer, &node->recordSize, sizeof(int));
        fwrite(buffer, sizeof(int), 1, fileHandle);
        memset(buffer, 0, sizeof(int));

        //write attributeNum
        memcpy(buffer, &node->attrNum, sizeof(int));
        fwrite(buffer, sizeof(int), 1, fileHandle);
        memset(buffer, 0, sizeof(int));

        //write attributeName
        int attrSize = 0;
        for (int i = 0; i < node->attrNum; i++) {
            attrSize = node->attrName[i].size();
            memcpy(buffer, node->attrName[i].c_str(), attrSize);
            fwrite(buffer, 1, attrSize, fileHandle);
            memset(buffer, 0, MAX_ATTR_NAME_LENGTH);
            fseek(fileHandle, MAX_ATTR_NAME_LENGTH - attrSize, SEEK_CUR);
        }

        ProcessTableInfo(buffer, node);
        fwrite(buffer, 1, 160, fileHandle);

        //write indexName
        /*
            format:
            如果这里没有index，不会写，直接跳过
            如果有index，写的格式是1个字节的位置信息（第几个attribute）+ 名字信息
        */
        memset(buffer, 0, 160);
        string indexName = "";
        string attrName = "";
        for (int i = 0; i < node->attrNum; i++) {
            if (node->attrInfo[INDEX][i] == 0) {
                continue;
            }
            else {
                unsigned char pos = i;
                memcpy(buffer, &pos, 1);
                attrName = node->attrName[i];
                for (int i = 0; i < node->indexName.size(); i++) {
                    if (node->indexName[i].first == attrName) {
                        indexName = node->indexName[i].second;
                        break;
                    }
                }
                memcpy((char*)buffer + 1, indexName.c_str(), 32);
                fwrite(buffer, 1, 32 + 1, fileHandle);
                memset(buffer, 0, 32 + 1);
            }
        }

        fclose(fileHandle);
        delete[] buffer;

        return true;
    }
    catch (bad_alloc &e) {
        cerr << e.what() << endl;
        return false;
    }
    catch (char* s) {
        cerr << "Open catalog " << fullCatalogName << "failed" << endl;
        return false;
    }
}

void CatalogManager::ProcessTableInfo(void* buffer, tableInfoNode* newNode) {
    unsigned char *p = (unsigned char*)buffer;
    int attrNum = newNode->attrNum;
    for (int i = 0; i < attrNum; i++) {
        p[i * 2] = newNode->attrInfo[0][i];
        p[i * 2 + 1] = newNode->attrInfo[1][i];
    }
    p = p + 64;
    for (int i = 0; i < attrNum; i++) {
        p[i] = newNode->attrInfo[2][i];
    }
    p = p + 32;
    for (int i = 0; i < attrNum; i++) {
        p[i] = newNode->attrInfo[3][i];
    }
    p = p + 32;
    for (int i = 0; i < attrNum; i++) {
        p[i] = newNode->attrInfo[4][i];
    }
}

/**
 *  Clear the tableInfo node, erase all information
 *
 *  @param tableInfoNode* node: the node to clear
 *
 */
void CatalogManager::ClearNode(tableInfoNode* node)
{

    node->attrName = nullptr;
    node->tableName = "";
    node->recordSize = node->attrNum = 0;
    node->attrInfo = nullptr;
    node->valid = false;
    return;
}

void CatalogManager::SetAPI(API* api){
    myAPI = api;

    //将已有的表放到pool里
    int indexNum = 0;
    for (int i = 0; i < fileNames.size(); i++) {
        delete tablePool[i];
        tablePool[i] = LoadTableInfo(fileNames[i]);
        indexNum = GetIndexNum(tablePool[i]);
        for (int j = 0; j < indexNum; j++) {
            myAPI->CreateIndex(tablePool[i]->indexName[j].second,
                tablePool[i]->tableName,
                tablePool[i]->indexName[j].first);
        }
    }
}

/**
 *  Load information about all tables.
 */
CatalogManager::CatalogManager() : tableNum(0), myAPI(nullptr)
{
    FILE* fileHandle;
    int offset = 0;
    string allTable = GenerateCatalogName("ALL_TABLE");
    string tableName;
    char* buffer;

    try {
        buffer = new char[MAX_FILE_NAME_LENGTH];
        memset(buffer, 0, MAX_FILE_NAME_LENGTH);
        for (int i = 0; i < MAX_CATALOG_FILE_NUM; i++) {
            tablePool[i] = new tableInfoNode();
        }
        fopen_s(&fileHandle, allTable.c_str(), "ab+");
        if (fileHandle == nullptr)
            throw allTable.c_str();

        //load names of files into the vector
        while (!feof(fileHandle)) {
            fseek(fileHandle, offset, SEEK_SET);
            offset += MAX_FILE_NAME_LENGTH;
            if (fread(buffer, 1, MAX_FILE_NAME_LENGTH, fileHandle) != 0) {
                tableName = (string)buffer;
                fileNames.push_back(tableName);
                memset(buffer, 0, MAX_FILE_NAME_LENGTH);
            }
            else break;
        }
        fclose(fileHandle);

        /*
        //将已有的表放到pool里
        int indexNum;
        for (int i = 0; i < fileNames.size(); i++) {
            delete tablePool[i];
            tablePool[i] = LoadTableInfo(fileNames[i]);
            indexNum = GetIndexNum(tablePool[i]);
            for (int j = 0; j < indexNum; j++) {
                myAPI->CreateIndex(tablePool[i]->indexName[j].second,
                    tablePool[i]->tableName,
                    tablePool[i]->indexName[j].first);
            }
        }
        */

    }
    catch (bad_alloc & e) {
        cerr << e.what() << endl;
        exit(1);
    }
    catch (char* s) {
        cerr << "Open catalog " << allTable << "failed" << endl;
        exit(1);
    }
}

/**
 *  Write information about all tables back into the file.
 */
CatalogManager::~CatalogManager()
{
    FILE* fileHandle;
    string allTable = GenerateCatalogName("ALL_TABLE");
    void* buffer;
    int offset;

    try {
        buffer = (void*)(new char[MAX_FILE_NAME_LENGTH]);
        memset(buffer, 0, MAX_FILE_NAME_LENGTH);
        offset = MAX_FILE_NAME_LENGTH;
        fopen_s(&fileHandle, allTable.c_str(), "wb+");
        if (fileHandle == nullptr)
            throw allTable.c_str();

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
    catch (bad_alloc & e) {
        cerr << e.what() << endl;
    }
    catch (char* s) {
        cerr << "Open catalog " << allTable << "failed" << endl;
    }
}

/**
 *  Get the length of one record for the table.
 *
 *  @param string tableName: name of the table
 *  @return int: the length of its record
 *
 */
int CatalogManager::GetRecordSize(string tableName)
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
int CatalogManager::CreateTableCatalog(tableInfoNode* node)
{
    //create a catalog file
    FILE* fileHandle;
    string fullCatalogName = GenerateCatalogName(node->tableName);
    node->recordSize = CalculateRecordSize(node);
    if (FindTable(node->tableName) != TABLE_NOT_FOUND) {
        return STATUS_ERROR;
    }

    try {
        fopen_s(&fileHandle, fullCatalogName.c_str(), "wb+");
        if (fileHandle == nullptr)
            throw fullCatalogName.c_str();
        fclose(fileHandle);

        //record the name of the file in vector fileNames
        fileNames.push_back(node->tableName);

        //update information in tablePool
        if (tableNum == MAX_CATALOG_FILE_NUM) {     //table pool full
            if (!WriteAllCatalogBack()) {
                return CATALOG_WRITE_FAILURE;
            }
        }

        for (int i = 0; i < MAX_CATALOG_FILE_NUM; i++) {
            if (tablePool[i]->valid == false) {
                delete tablePool[i];
                tablePool[i] = node;
                tableNum++;
                break;
            }
        }
        return 1;
    }
    catch (char* s) {
        cerr << "Error: create catalog file failed" << endl;
        return CATALOG_CREATE_FAILURE;
    }
}

int CatalogManager::CalculateRecordSize(tableInfoNode* node) {
    int recordSize = 0;
    int attrType;
    for (int i = 0; i < node->attrNum; i++) {
        attrType = node->attrInfo[0][i];
        switch (attrType) {
        case INT:
            recordSize += sizeof(int);
            break;
        case FLOAT:
            recordSize += sizeof(float);
            break;
        case CHAR:
            recordSize += (node->attrInfo[1][i]) * 2;
            break;
        }
    }
    for (int i = 1; i <= 8192; i = i << 1) {
        if (recordSize > (i >> 1) && recordSize <= i) {
            recordSize = i;
            break;
        }
    }
    return recordSize;
}

/**
 *  @return 0: failed
 */
int CatalogManager::DropTableCatalog(string tableName)
{
    //erase file
    string fullCatalogName = GenerateCatalogName(tableName);
    
    try {
        if (remove(fullCatalogName.c_str()))
            throw fullCatalogName.c_str();

        //erase the record of this file
        int position = FindTable(tableName);
        if (position == TABLE_NOT_FOUND)return 0;
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
    catch (char* s) {
        cerr << "Error: failed to remove the catalog file" << endl;
        return 0;
    }
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
        if (tablePool[i]->tableName.compare(tableName) == 0 && tablePool[i]->valid == true) {
            index = i;
        }
        if (tablePool[i]->valid == false && firstInvalidPos == -1) {
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

bool CatalogManager::FindAttr(string tableName, string attrName) {
    tableInfoNode* tableInfo = GetTableInfo(tableName);
    for (int i = 0; i < tableInfo->attrNum; i++) {
        if (attrName == tableInfo->attrName[i]) {
            return true;
        }
    }
    return false;
}

int CatalogManager::GetTableNum()
{
    return tableNum;
}

int CatalogManager::GetAttrNum(string tableName) {
    if (FindTable(tableName) == TABLE_NOT_FOUND)return 0;
    tableInfoNode* node = GetTableInfo(tableName);
    return node->attrNum;
}

int CatalogManager::GetAttrType(string tableName, string attrName)
{
    int index = -1;
    int position = 0;
    unsigned char* buffer;
    tableInfoNode* node = GetTableInfo(tableName);

    if (node == nullptr) {
        cerr << "Error: fail to fetch table info" << endl;
        return STATUS_ERROR;
    }

    for (int i = 0; i < node->attrNum; i++) {
        if (node->attrName[i].compare(attrName) == 0) {
            index = i;
            break;
        }
    }
    //return if the attribute name is not found
    if (index == -1) {
        cerr << "Error: attribute not found" << endl;
        return STATUS_ERROR;
    }
    return node->attrInfo[0][index];
}

int CatalogManager::FindAttrPosition(string tableName, string attrName) {
    tableInfoNode* node = GetTableInfo(tableName);
    int attrNum = node->attrNum;
    int index = -1;
    for (int i = 0; i < attrNum; i++) {
        if (node->attrName[i] == attrName) {
            index = i;
            break;
        }
    }
    return index;
}

int CatalogManager::CreateIndex(string tableName, string attrName, string indexName) {
    tableInfoNode* tableInfo = GetTableInfo(tableName);
    int pos = FindAttrPosition(tableName, attrName);
    if (pos != -1) {
        tableInfo->attrInfo[4][pos] = 1;
        tableInfo->indexName.push_back(make_pair(attrName, indexName));
        return 1;
    }
    else return TABLE_NOT_FOUND;
}

int CatalogManager::DropIndex(string tableName, string indexName) {
    tableInfoNode* tableInfo = GetTableInfo(tableName);
    string attrName;
    int pos = -1;
    for (int i = 0; i < tableInfo->attrNum; i++) {
        if (tableInfo->indexName[i].second == indexName) {
            pos = i;
            tableInfo->attrInfo[4][i] = 0;
            attrName = tableInfo->attrName[i];
            auto iter = find(tableInfo->indexName.begin(), tableInfo->indexName.end(), make_pair(attrName, indexName));
            tableInfo->indexName.erase(iter);
            break;
        }
    }
    if (pos != -1)return 1;
    else return TABLE_NOT_FOUND;
}

int CatalogManager::HasIndex(string tableName, string attrName) {
    tableInfoNode* tableInfo = GetTableInfo(tableName);
    if (tableInfo == nullptr) {
        return TABLE_NOT_FOUND;
    }
    int pos = FindAttrPosition(tableName, attrName);
    if (pos != -1) {
        if (tableInfo->attrInfo[4][pos] == 1)return 1;
        else return 0;
    }
    else return 0; 
}

int CatalogManager::SetIndex(string tableName, string attrName, bool hasIndex) {
    tableInfoNode* tableInfo = GetTableInfo(tableName);
    if (tableInfo == nullptr) {
        return TABLE_NOT_FOUND;
    }
    int pos = FindAttrPosition(tableName, attrName);
    if (pos != -1) {
        if (hasIndex) tableInfo->attrInfo[INDEX][pos] = 1;
        else tableInfo->attrInfo[INDEX][pos] = 0;
        return 1;
    }
    else return 0;
}