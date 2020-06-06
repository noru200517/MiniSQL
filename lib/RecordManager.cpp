#include "RecordManager.h"
#include "API.h"
#include <iostream>

//#include <Windows.h>
using namespace std;

#pragma warning(disable : 4996)

RecordManager::RecordManager()
{
       
}

RecordManager::RecordManager(BufferManager* myBM, CatalogManager* myCM) : 
    myBufferManager(myBM),
    myCatalogManager(myCM),
    myAPI(nullptr)
{

}

RecordManager::~RecordManager()
{

}

void RecordManager::SetAPI(API* api) {
    myAPI = api;
}

std::wstring s2ws(const std::string& s) {
    std::string curLocale = setlocale(LC_ALL, "");
    _ASSERTE(_CrtCheckMemory());
    const char* _Source = s.c_str();
    size_t _Dsize = mbstowcs(NULL, _Source, 0) + 1;
    wchar_t* _Dest = new wchar_t[_Dsize];
    wmemset(_Dest, 0, _Dsize);
    _ASSERTE(_CrtCheckMemory());
    mbstowcs(_Dest, _Source, _Dsize);
    std::wstring result = _Dest;
    delete[]_Dest;
    setlocale(LC_ALL, curLocale.c_str());
    return result;
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

    fopen_s(&fileHandle, fullTableName.c_str(), "wb+");
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
    blockNode* blocknode, *nextBlockNode;
    int position = 0;

    //DWORD startTime = GetTickCount();

    if (remove(fullTableName.c_str())) {
        return 0;
    }
    position = myBufferManager->FindBlock(tableName);
    if (position == FILE_NOT_LOADED) {
        return 1;
    }
    else {
        blocknode = myBufferManager->GetFirstBlock(tableName);
        while (blocknode)
        {
            int recordSize = myCatalogManager->GetRecordSize(tableName);
            memset(blocknode->deleted, 0, BLOCK_SIZE / recordSize);
            blocknode->isDirty = false;
            blocknode->isPinned = false;
            blocknode->offset = 0;
            blocknode->recordAddress = nullptr;
            blocknode->recordNum = 0;
            blocknode->sizeUsed = 0;
            _ASSERTE(_CrtCheckMemory());
            nextBlockNode = myBufferManager->GetNextBlock(blocknode);
            blocknode->filenode = nullptr;
            blocknode = nextBlockNode;
        }
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
/*
//index manager直接和API交互，应该不需要record manager再做处理
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
*/

/**
 *  drop an index file.
 *
 *  @param string tableName: name of the table
 *  @return int: number of index effected
 */
/*
//index manager直接和API交互，应该不需要record manager再做处理
int RecordManager::DropIndex(string indexName)
{
    string fullIndexName = GenerateIndexName(indexName);
    if (remove(fullIndexName.c_str())) {
        return 0;
    }
    return 1;
}
*/

int RecordManager::InsertOperation(blockNode* blocknode, void* record, int recordSize) {
    int sizeUsed = blocknode->sizeUsed;
    char* position;

    //move to the end of the record in the block  
    position = (char*)(blocknode->recordAddress);
    position = position + sizeUsed;

    //write the record into the block
    memcpy(position, record, recordSize);
    _ASSERTE(_CrtCheckMemory());

    //update information
    blocknode->sizeUsed += recordSize;
    blocknode->recordNum++;
    blocknode->deleted[blocknode->recordNum - 1] = 0;
    _ASSERTE(_CrtCheckMemory());
    blocknode->isDirty = true;

    UpdateLRUBlock(blocknode);

    /*
    pair<int, int> kv = *map[key];
    cache.erase(map[key]);
    cache.push_front(kv);
    // 更新 (key, value) 在 cache 中的位置
    map[key] = cache.begin();
    */

    return 1;
}

//这个名字起得不好，这个函数就是把传进来这些东西改成一个可以写进block的record的格式。
void RecordManager::ProcessRecord(int attrNum, string attrVal[], void* record, tableInfoNode* tableInfo) {
    int attrType = 0;
    int recordSize = tableInfo->recordSize;
    char* p;
    int intVal = 0;
    float floatVal = 0;
    wstring charValString = L"";
    const wchar_t* charVal;
    int offset = 0;

    memset(record, 0, recordSize);
    _ASSERTE(_CrtCheckMemory());
    p = (char*)record;
    for (int i = 0; i < attrNum; i++) {
        attrType = myCatalogManager->GetAttrType(tableInfo->tableName, tableInfo->attrName[i]);
        switch (attrType) {
        case INT:
            intVal = atoi(attrVal[i].c_str());
            memcpy((void*)p, &intVal, sizeof(int));
            _ASSERTE(_CrtCheckMemory());
            p += sizeof(int);
            break;
        case FLOAT:
            floatVal = atof(attrVal[i].c_str());
            memcpy((void*)p, &floatVal, sizeof(float));
            _ASSERTE(_CrtCheckMemory());
            p += sizeof(float);
            break;
        case CHAR:
            charValString = s2ws(attrVal[i]);
            memcpy((void*)p, charValString.c_str(), (size_t)tableInfo->attrInfo[1][i] * 2);
            _ASSERTE(_CrtCheckMemory());
            p += (size_t)tableInfo->attrInfo[1][i] * 2;
            break;
        }
    }
}

bool RecordManager::TestUnique(string tableName, string attrName, string attrVal) {
    blockNode* blocknode = myBufferManager->GetFirstBlock(tableName);
    int recordSize = myCatalogManager->GetRecordSize(tableName);
    int offset = 0;
    void* record;
    void* recordAttrVal;
    string recordVal = "";

    while (blocknode) {
        offset = 0;
        record = SelectRecord(blocknode, recordSize, offset);
        while (record) {
            //这里有问题，你最好考虑重新构造你的select record函数
            //改返回值？改外面的函数结构？反正这样GG
            if (IsDelete(blocknode, recordSize, offset)) {
                offset += recordSize;
                record = SelectRecord(blocknode, recordSize, offset);
                continue;
            }
            offset += recordSize;
            recordAttrVal = GetAttrValue(tableName, attrName, record);
            recordVal = (string)(char*)recordAttrVal;
            delete[] recordAttrVal;
            _ASSERTE(_CrtCheckMemory());
            if (recordVal == attrVal) {
                return true;
            }
            record = SelectRecord(blocknode, recordSize, offset);
        }
        blocknode = myBufferManager->GetNextBlock(blocknode);
    }
    return false;
}

int RecordManager::InsertProcess(int attrNum, string attrVal[], string tableName)
{
    blockNode* blocknode = myBufferManager->GetBlock(tableName);
    blockNode* nextBlockNode;
    tableInfoNode* tableInfo = myCatalogManager->GetTableInfo(tableName);
    int recordSize = myCatalogManager->GetRecordSize(tableName);
    int result = 0;
    char* record =  new char[recordSize]();
    memset(record, 0, recordSize);
    _ASSERTE(_CrtCheckMemory());

    ProcessRecord(attrNum, attrVal, record, tableInfo);
    _ASSERTE(_CrtCheckMemory());
    int pos = 0;
    bool hasRecord = false;
    /*
    for (int i = 0; i < tableInfo->attrNum; i++) {
        if (tableInfo->attrInfo[UNIQUE][i] == 1) {
            hasRecord = TestUnique(tableName, tableInfo->attrName[i], attrVal[i]);
            if (hasRecord) {
                //打印错误信息
                return 0;
            }
        }
    }
    */

    if (blocknode == nullptr) {
        cerr << "Error: buffer allocation failed" << endl;
        return 0;
    }
    //if the block is full && do not have enough space, we also need to create a new block
    else if (blocknode->IsFull() || blocknode->size - blocknode->sizeUsed < recordSize) {
        nextBlockNode = myBufferManager->GetNextBlock(blocknode);
        //if we have reached the end of file
        if (nextBlockNode == nullptr) {
            nextBlockNode = myBufferManager->CreateBlock(tableName, blocknode->offset + BLOCK_SIZE)->blocknode;
            blocknode->filenode->nextFileNode = nextBlockNode->filenode;
            blocknode->filenode->fileReachEnd = false;
            nextBlockNode->filenode->prevFileNode = blocknode->filenode;
            blocknode = nextBlockNode;
        }
        result = InsertOperation(blocknode, record, recordSize);
        _ASSERTE(_CrtCheckMemory());
    }
    //if the block is fetched and there is enough empty space left
    else {
        result = InsertOperation(blocknode, record, recordSize);
        _ASSERTE(_CrtCheckMemory());
    }

    //如果有index，也要在index的树里插上一条
    for (int i = 0; i < attrNum; i++) {
        if (myAPI->HasIndex(tableName, tableInfo->attrName[i])) {
            myAPI->InsertIndexRecord(tableName, tableInfo->attrName[i], attrVal[i], recordSize, record);
        }
    }

    //注意这里回收了record的这块内存，如果你在index里想用的话，必须自己再开一块内存复制。
    delete[] record;
    _ASSERTE(_CrtCheckMemory());
    return result;
}

int RecordManager::DeleteAll(string tableName) {
    blockNode* blocknode = myBufferManager->GetFirstBlock(tableName);
    int cnt = 0;
    while (blocknode) {
        UpdateLRUBlock(blocknode);
        blocknode->isDirty = true;
        for (int i = 0; i < blocknode->recordNum; i++) {
            blocknode->deleted[i] = 1;
            cnt++;
        }
        blocknode = myBufferManager->GetNextBlock(blocknode);
    }
    return cnt;
}

int RecordManager::DeleteProcess(string tableName, int restrictNum, string restricts[], bool total)
{
    int position = myCatalogManager->FindTable(tableName);
    blockNode* blocknode; 
    tableInfoNode* tableInfo;
    vector <void*> records;
    void* buffer;
    unsigned char** attrInfo;
    int offset = 0;
    int recordSize;

    //return if table has not been created
    if (position == TABLE_NOT_FOUND) {
        cerr << "Error: table has not been created" << endl;
        return 0;
    } 

    //get table info and the first block
    tableInfo = myCatalogManager->GetTableInfo(tableName);
    blocknode = myBufferManager->GetFirstBlock(tableName);
    //get recordSize, recordFormat
    recordSize = tableInfo->recordSize;
    attrInfo = tableInfo->attrInfo;

    //if received nullptr, return
    if (blocknode == nullptr) {
        return 0;
    }
    if (tableInfo == nullptr) {
        return 0;
    }
    //if the block is created but is empty, return empty string
    if (blocknode->recordNum == 0) {    //there is no record in the first buffer
        return 0;
    }

    if (total) {
        return DeleteAll(tableName);
    }

    string attrName = "", attrOp = "";
    wstring attrVal = L"";
    void* recordAttrVal;
    int op, attrType = 0;

    attrName = restricts[0];
    attrOp = restricts[1];
    attrVal = s2ws(restricts[2]);
    int intVal = 0;
    float floatVal = 0;
    wstring charVal = L"";
    op = OpTypeCast(attrOp);
    //find the type of attribute
    attrType = myCatalogManager->GetAttrType(tableName, attrName);
    if (attrType == STATUS_ERROR) return 0;
    //find in the record the value of a specific attribute
    while (blocknode) {
        UpdateLRUBlock(blocknode);
        offset = 0;
        for (int i = 0; i < blocknode->recordNum; i++) {
            //get the whole record
            buffer = SelectRecord(blocknode, recordSize, offset);
            offset += recordSize;
            //if record has been deleted
            if (buffer == nullptr)continue;
            //get the value and compare
            recordAttrVal = GetAttrValue(tableName, attrName, buffer);
            switch (attrType) {
            case INT:
                intVal = *(int*)recordAttrVal;
                if (RecordMatch(intVal, op, attrVal)) {
                    records.push_back(buffer);
                }
                break;
            case FLOAT:
                floatVal = *(float*)recordAttrVal;
                if (RecordMatch(floatVal, op, attrVal)) {
                    records.push_back(buffer);
                }
                break;
            case CHAR:
                charVal = (wstring)((wchar_t*)recordAttrVal);
                if (RecordMatch(charVal, op, attrVal)) {
                    records.push_back(buffer);
                }
                break;
            }
        }
        blocknode = myBufferManager->GetNextBlock(blocknode);
    }
    //if no record satisfies the given requirement, break
    if (records.size() == 0) return 0;

    vector <void*>::iterator iter = records.begin();

    for (int i = 3; i < restrictNum * 3; i += 3) {
        attrName = restricts[i];
        attrOp = restricts[i + 1];
        attrVal = s2ws(restricts[i + 2]);
        op = OpTypeCast(attrOp);
        attrType = myCatalogManager->GetAttrType(tableName, attrName);
        for (iter = records.begin(); iter != records.end();) {
            //get the whole record
            buffer = *iter;
            //get the value and compare
            recordAttrVal = GetAttrValue(tableName, attrName, buffer);
            switch (attrType) {
            case INT:
                intVal = *(int*)recordAttrVal;
                if (!RecordMatch(intVal, op, attrVal)) {
                    iter = records.erase(iter);
                    delete[] buffer;
                }
                else {
                    iter++;
                }
                break;
            case FLOAT:
                floatVal = *(float*)recordAttrVal;
                if (!RecordMatch(floatVal, op, attrVal)) {
                    iter = records.erase(iter);
                    delete[] buffer;
                }
                else {
                    iter++;
                }
                break;
            case CHAR:
                charVal = (wstring)((wchar_t*)recordAttrVal);
                if (!RecordMatch(charVal, op, attrVal)) {
                    iter = records.erase(iter);
                    delete[] buffer;
                }
                else {
                    iter++;
                }
                break;
            }
            if (iter == records.end())break;
            else if (iter != records.begin())iter--;
        }
        //if no record satisfies the given requirement, break
        if (records.size() == 0) return 0;
    }

    blocknode = myBufferManager->GetFirstBlock(tableName);
    int recordCnt = 0;
    while (blocknode) {
        offset = 0;
        UpdateLRUBlock(blocknode);
        for (int i = 0; i < blocknode->recordNum; i++) {
            buffer = SelectRecord(blocknode, recordSize, offset);
            offset += recordSize;
            if (buffer == nullptr)continue;
            if (CompareRecord((char*)records[recordCnt], (char*)buffer, recordSize)) {
                SetDelete(blocknode, i);
                recordCnt++;
                if (recordCnt == records.size())return records.size();
            }
            delete[] buffer;
        }
        blocknode = myBufferManager->GetNextBlock(blocknode);
    }

    for (int i = 0; i < records.size(); i++) {
        delete[]records[i];
    }

    return records.size();
}

void RecordManager::SetDelete(blockNode* blocknode, int pos) {
    blocknode->deleted[pos] = 1;
}

bool RecordManager::CompareRecord(char* recordToCheck, char* buffer, int recordSize)
{
    for (int i = 0; i < recordSize; i++) {
        if (recordToCheck[i] != buffer[i])return false;
    }
    return true;
}

bool RecordManager::IsDelete(blockNode* blocknode, int recordSize, int offset) const {
    if (blocknode->deleted[offset / recordSize] == 1)return true;
    else return false;
}

//删除的情况下也会返回空指针，所以务必事先检查删除，否则就GG
//返回的是一个复制的record，而不是原先的record。这是为了select的时候不会出错
//因此，注意使用完之后必须要释放内存，否则会导致泄漏
void* RecordManager::SelectRecord(blockNode* blocknode, int recordSize, int offset) const {
    int pos = offset / recordSize;
    void* recordAddress = blocknode->recordAddress;
    void* buffer = (void*)new char[recordSize]();

    if (pos >= blocknode->recordNum) {
        delete[] buffer;
        return nullptr;
    }
    /*
    if (buffer == nullptr) {
        cerr << "Error: memory allocation failed" << endl;
        return nullptr;
    }
    */
    recordAddress = (void*)((char*)recordAddress + offset);
    if (blocknode->deleted[pos] == 0) {
        memcpy(buffer, recordAddress, recordSize);
        return buffer;
    }
    else {
        delete[] buffer;
        return nullptr;
    }
}

int RecordManager::SelectProcess(int N_A, string attribute[], string tableName, int restrictNum, string restricts[], bool show, bool total, vector <char*>* records)
{
    int position = myCatalogManager->FindTable(tableName);
    blockNode* blocknode;
    tableInfoNode* tableInfo;
    char* buffer;
    unsigned char** attrInfo;
    int offset = 0;
    int recordSize;

    //return if table has not been created
    if (position == TABLE_NOT_FOUND) {
        cerr << "Error: table has not been created" << endl;
        return 0;
    }

    //get table info and the first block
    tableInfo = myCatalogManager->GetTableInfo(tableName);
    blocknode = myBufferManager->GetFirstBlock(tableName);
    //get recordSize, recordFormat
    recordSize = tableInfo->recordSize;
    attrInfo = tableInfo->attrInfo;

    //if received nullptr, return
    if (blocknode == nullptr) {
        return 0;
    }
    if (tableInfo == nullptr) {
        return 0;
    }
    //if the block is created but is empty, return empty string
    if (blocknode->recordNum == 0) {    //there is no record in the first buffer
        return 0;
    }
    
    if (total) {
        return FetchRecords(N_A, attribute, tableName, show, records);
    }

    if (records == nullptr) {
        records = new vector<char*>();
    }

    string attrName = "", attrOp = "";
    wstring attrVal = L"";
    void* recordAttrVal;
    int op, attrType = 0;
    attrName = restricts[0];
    attrOp = restricts[1];  
    attrVal = s2ws(restricts[2]);
    int intVal = 0;
    float floatVal = 0;
    wstring charVal = L"";
    op = OpTypeCast(attrOp);
    //find the type of attribute
    attrType = myCatalogManager->GetAttrType(tableName, attrName);
    if (attrType == STATUS_ERROR) return 0;
    //find in the record the value of a specific attribute
    while (blocknode) { 
        offset = 0;
        UpdateLRUBlock(blocknode);
        for (int i = 0; i < blocknode->recordNum; i++) {
            //get the whole record
            buffer = (char*)SelectRecord(blocknode, recordSize, offset);
            offset += recordSize;
            //if record has been deleted
            if (buffer == nullptr)continue;
            //get the value and compare
            recordAttrVal = GetAttrValue(tableName, attrName, buffer);
            switch (attrType) {
            case INT:
                intVal = *(int*)recordAttrVal;
                if (RecordMatch(intVal, op, attrVal)) {
                    records->push_back(buffer);
                }
                break;
            case FLOAT:
                floatVal = *(float*)recordAttrVal;
                if (RecordMatch(floatVal, op, attrVal)) {
                    records->push_back(buffer);
                }
                break;
            case CHAR:
                charVal = (wstring)((wchar_t*)recordAttrVal);
                if (RecordMatch(charVal, op, attrVal)) {
                    records->push_back(buffer);
                }
                break;
            }
        }
        blocknode = myBufferManager->GetNextBlock(blocknode);
    }
     //if no record satisfies the given requirement, break
    if (records->size() == 0) return 0;

    vector <char*>::iterator iter = records->begin();

    for (int i = 3; i < restrictNum * 3; i += 3) {
        attrName = restricts[i];
        attrOp = restricts[i + 1];
        attrVal = s2ws(restricts[i + 2]);
        op = OpTypeCast(attrOp);
        attrType = myCatalogManager->GetAttrType(tableName, attrName);
        for (iter = records->begin(); iter != records->end(); ) {
            //get the whole record
            buffer = *iter;
            //get the value and compare
            recordAttrVal = GetAttrValue(tableName, attrName, buffer);
            switch (attrType) {
            case INT:
                intVal = *(int*)recordAttrVal;
                if (!RecordMatch(intVal, op, attrVal)) {
                    iter = records->erase(iter);
                    delete[] buffer;
                }
                else {
                    ++iter;
                }
                break;
            case FLOAT:
                floatVal = *(float*)recordAttrVal;
                if (!RecordMatch(floatVal, op, attrVal)) {
                    iter = records->erase(iter);
                    delete[] buffer;
                }
                else {
                    ++iter;
                }
                break;
            case CHAR:
                charVal = (wstring)((wchar_t*)recordAttrVal);
                if (!RecordMatch(charVal, op, attrVal)) {
                    iter = records->erase(iter);
                    delete[] buffer;
                }
                else {
                    ++iter;
                }
                break;
            }
        }
        //if no record satisfies the given requirement, break
        if (records->size() == 0) return 0;
    }

    if (show) {
        int size = records->size();
        FetchRecords(N_A, attribute, tableName, *records);
        ReleaseRecords(records);
        //delete records;
        return size;
    }
    else return records->size();
}

int RecordManager::SelectProcess(int N_A, string attribute[], int tableNum, string tableName[], int restrictNum, string restricts[], bool show, bool total) {
    
    typedef vector<char*>* vecPtr;
    vecPtr* records = new vecPtr[tableNum];
    _ASSERTE(_CrtCheckMemory());
    int thisRestrictNum = 0;
    string* thisRestricts = new string[restrictNum * 3];
    _ASSERTE(_CrtCheckMemory());
    vector <string> thisAttribute;
    string attrName;
    bool totalFlag;

    for (int i = 0; i < tableNum; i++) {

        thisRestrictNum = 0;
        totalFlag = true;
        delete [] thisRestricts;
        _ASSERTE(_CrtCheckMemory());
        thisRestricts = new string[restrictNum * 3];
        _ASSERTE(_CrtCheckMemory());
        records[i] = new vector<char*>();
        for (int j = 0; j < restrictNum; j += 3) {
            attrName = restricts[j];
            if (myCatalogManager->FindAttr(tableName[i], attrName)) {
                totalFlag = false;
                thisRestrictNum++;
                thisRestricts[j] = restricts[j];
                thisRestricts[j + 1] = restricts[j + 1];
                thisRestricts[j + 2] = restricts[j + 2];
            }
        }
        SelectProcess(N_A, attribute, tableName[i], thisRestrictNum, thisRestricts, false, totalFlag, records[i]);
    }

    vector <char*> recordConcat = *records[0];
    //这里用vector该有多好啊！
    int attrNum = 0;
    int recordConcatSize = myCatalogManager->GetRecordSize(tableName[0]);
    int* recordSize = new int[tableNum];
    recordSize[0] = recordConcatSize;

    //直接用selectRecord得到的是block块里的地址，这个不能释放，释放了再去取就成了野指针了。
    //但是之后生成的那些复合record是应该把内存释放的，不然积累的时间一长，N次操作后肯定内存慢慢泄露完了。
    //问题是这里咋弄……少量测试用并不需要太在意这块内存（何况我32G），但是实际上必须管理！
    for (int i = 1; i < tableNum; i++) {
        recordSize[i] = myCatalogManager->GetRecordSize(tableName[i]);
        recordConcat = ConcatenateRecord(recordConcatSize, recordConcat, recordSize[i], *records[i]);
        recordConcatSize += recordSize[i];
    }
    FetchRecords(N_A, attribute, tableNum, tableName, recordSize, recordConcat, show);

    return (int)recordConcat.size();
}

vector <char*> RecordManager::ConcatenateRecord(int recordConcatSize, vector <char*>& recordConcat, int recordSize, vector <char*>& records) {
    vector <char*> newRecord;
    char* buffer;
    int newRecordSize = recordConcatSize + recordSize;
    for (int i = 0; i < recordConcat.size(); i++) {
        for (int j = 0; j < records.size(); j++) {
            buffer = new char[newRecordSize];
            memset(buffer, 0, newRecordSize);
            _ASSERTE(_CrtCheckMemory());
            memcpy(buffer, recordConcat[i], recordConcatSize);
            _ASSERTE(_CrtCheckMemory());
            memcpy((char*)buffer + recordConcatSize, records[j], recordSize);
            _ASSERTE(_CrtCheckMemory());
            newRecord.push_back(buffer);
        }
    }
    /*
    for (int i = 0; i < recordConcat.size(); i++) {
        delete[] recordConcat[i];
    }
    */
    return newRecord;
}

int RecordManager::FetchRecords(int N_A, string attribute[], int tableNum, string*& tableName, 
                                int*& recordSize, const vector<char*>& recordConcat, bool show){
    //利用这一堆传入的table生成表头啊！
    //variables used to get records
    tableInfoNode* tableInfo;

    //variables used to show records
    vector < pair<string, int> > tableHead;
    vector <tableInfoNode*> tableNames;
    vector <string> attrNames;
    int charSize = 0;
    int lastAttrNum = 0, totalAttrNum = 0;
    int* offset = new int[32]();
    _ASSERTE(_CrtCheckMemory());
    int curOffset = 0;

    for (int i = 0; i < tableNum; i++) {
        tableInfo = myCatalogManager->GetTableInfo(tableName[i]);
        totalAttrNum += tableInfo->attrNum;
    }
    bool* marked = new bool[totalAttrNum]();
    _ASSERTE(_CrtCheckMemory());

    if (recordConcat.size() == 0)return 0; 
    //get table info and the first block
    
    for (int i = 0; i < tableNum; i++) {
        tableInfo = myCatalogManager->GetTableInfo(tableName[i]);
        if (tableInfo == nullptr) {
            return STATUS_ERROR;
        }
        for (int j = 0; j < N_A; j++) {
            for (int k = 0; k < tableInfo->attrNum; k++) {
                if (tableInfo->attrName[k] != attribute[j] || marked[lastAttrNum + k])continue;
                charSize = tableInfo->attrInfo[1][k];
                if (charSize * 2 > tableInfo->attrName[k].size() * 2 && tableInfo->attrInfo[0][k] == CHAR) {
                    tableHead.push_back(make_pair(tableInfo->attrName[k], charSize * 2 + 2));
                }
                else {
                    tableHead.push_back(make_pair(tableInfo->attrName[k], 12));
                }
                tableNames.push_back(tableInfo);
                offset[j] = curOffset;
                marked[lastAttrNum + k] = true;
                break;
            }
        }
        lastAttrNum = tableInfo->attrNum;
        curOffset += tableInfo->recordSize;
    }
    PrintHline(tableHead);
    PrintHead(tableHead);
    PrintHline(tableHead);
    /*
    void RecordManager::ShowBody(int N_A, vector<string> attrNames, vector<tableInfoNode*>& tableNames,
                       const vector <char*>& records, vector < pair<string, int> >& tableHead, int*& offset) const
    */
    for (int i = 0; i < N_A; i++) {
        attrNames.push_back(attribute[i]);
    }
    ShowBody(N_A, attrNames, tableNames, recordConcat, tableHead, offset);
    PrintHline(tableHead);

    int s = (int)recordConcat.size();
    //return the number of records fetched
    return s;
}


int RecordManager::FetchRecords(int N_A, string attribute[], string tableName, bool show, vector <char*>* records) {
    //variables used to get records
    tableInfoNode* tableInfo;
    blockNode* blocknode;
    int recordSize = 0;
    int offset = 0;
    char* buffer;
    bool nullFlag = false;

    if (records == nullptr) {
        records = new vector<char*> ();
        nullFlag = true;
    }

    //get table info and the first block
    tableInfo = myCatalogManager->GetTableInfo(tableName);
    if (tableInfo == nullptr) {
        return STATUS_ERROR;
    }
    blocknode = myBufferManager->GetFirstBlock(tableName);
    //get recordSize, recordFormat
    recordSize = tableInfo->recordSize;

    //get records and put them into the vector "records"
    while (blocknode) {
        offset = 0;
        UpdateLRUBlock(blocknode);
        for (int i = 0; i < blocknode->sizeUsed; i += tableInfo->recordSize) {
            buffer = (char*)SelectRecord(blocknode, recordSize, offset);
            if (IsDelete(blocknode, recordSize, offset)) {
                offset += recordSize;
                continue;
            }
            //if record has been deleted
            offset += recordSize;
            if (buffer == nullptr)continue;
            records->push_back(buffer);
        }
        blocknode = myBufferManager->GetNextBlock(blocknode);
    }

    if (show) {
        int res = FetchRecords(N_A, attribute, tableName, *records);
        ReleaseRecords(records);
        if (nullFlag) delete records;
        return res;
    }
    else {
        ReleaseRecords(records);
    }
}

void RecordManager::ReleaseRecords(vector<char*>* records) {
    for (int i = 0; i < records->size(); i++) {
        delete[] (*records)[i];
    }
}

/**
 *  Show all records in a table.
 *
 *  @param string tableName: the table to insert a record into.
 *
 *  @return int: if return STATUS_ERROR, insertion failed. Else return the number of rows in the table.
 */
int RecordManager::FetchRecords(int N_A, string attribute[], string tableName, const vector <char*>& records)
{ 
    //variables used to get records
    tableInfoNode* tableInfo;

    //variables used to show records
    vector < pair<string, int> > tableHead;
    vector <string> attrName;
    vector<tableInfoNode*> tableNames;
    
    int attrCnt = 0;
    int charSize = 0;

    //get table info and the first block
    tableInfo = myCatalogManager->GetTableInfo(tableName);
    if (tableInfo == nullptr) {
        return STATUS_ERROR;
    }

    if (records.size() == 0)return 0;

    //explain and show records
    for (int i = 0; i < N_A; i++) {
        tableNames.push_back(tableInfo);
        for (int j = 0; j < tableInfo->attrNum; j++) {
            if (tableInfo->attrName[j] != attribute[i]) continue;
            charSize = tableInfo->attrInfo[1][j];
            if (charSize * 2 > tableInfo->attrName[j].size() * 2 && tableInfo->attrInfo[0][j] == CHAR) {
                tableHead.push_back(make_pair(tableInfo->attrName[j], charSize * 2 + 2));
            }
            else {
                tableHead.push_back(make_pair(tableInfo->attrName[j], 12));
            }
            attrName.push_back(tableInfo->attrName[j]);
        }
    }
        
    PrintHline(tableHead);
    PrintHead(tableHead);
    PrintHline(tableHead);
    //这里做一个函数比较好吧
    /*
    void RecordManager::ShowBody(int N_A, vector<string> attrNames, vector<tableInfoNode*>& tableNames,
                       const vector <char*>& records, vector < pair<string, int> >& tableHead, int*& offset) const
    */
    int* offset = new int[32]();
    _ASSERTE(_CrtCheckMemory());
    ShowBody(N_A, attrName, tableNames, records, tableHead, offset);
    PrintHline(tableHead);

    int s = records.size();
    //return the number of records fetched
    delete[] offset;
    return s;
}

//Print the hline when showing the result of select query
void RecordManager::PrintHline(vector < pair<string, int> > tableHead) const
{
    cout << "+";
    for (int i = 0; i < tableHead.size(); i++) {
        for (int j = 0; j < tableHead[i].second; j++) {
            cout << "-";
        }
        cout << "+";
    }
    cout << endl;
}

//Print the head of the table
void RecordManager::PrintHead(vector<pair<string, int>> tableHead) const
{
    cout << "|";
    int frontSpace = 0;
    int endSpace = 0;

    for (int i = 0; i < tableHead.size(); i++) {
        frontSpace = (tableHead[i].second - tableHead[i].first.size()) / 2;
        endSpace = tableHead[i].second - tableHead[i].first.size() - frontSpace;
        for (int j = 0; j < frontSpace; j++) {
            cout << " ";
        }
        cout << tableHead[i].first;
        for (int j = 0; j < endSpace; j++) {
            cout << " ";
        }
        cout << "|";
    }
    cout << endl;
}

void RecordManager::ShowBody(int N_A, vector<string> attrNames, vector<tableInfoNode*>& tableNames,
                             const vector <char*>& records, vector < pair<string, int> >& tableHead, int*& offset) const {
    void* attrVal;
    int intVal = 0;
    float floatVal = 0;
    wstring charVal = L"";
    int pos = 0;
    tableInfoNode* tableInfo;

    for (int i = 0; i < records.size(); i++) {          //for each record
        cout << "|";
        for (int j = 0; j < N_A; j++) {
            tableInfo = tableNames[j];
            attrVal = GetAttrValue(tableInfo->tableName, attrNames[j], records[i] + offset[j]);
            pos = myCatalogManager->FindAttrPosition(tableInfo->tableName, attrNames[j]);
            switch (tableInfo->attrInfo[0][pos]) {
            case INT:
                intVal = *(int*)attrVal;
                ShowCell(intVal, tableHead[j].second);
                break;
            case FLOAT:
                floatVal = *(float*)attrVal;
                ShowCell(floatVal, tableHead[j].second);
                break;
            case CHAR:
                charVal = (wstring)((wchar_t*)attrVal);
                ShowCell(charVal, tableHead[j].second);
                break;
            }
        }
        cout << endl;
    }
}

//Show one cell in the table
void RecordManager::ShowCell(int intVal, int length) const
{
    int endSpace = 1;
    int frontSpace = 0;
    string value;

    value = to_string(intVal);
    frontSpace = length - value.size() - 1;
    for (int j = 0; j < frontSpace; j++) {
        cout << " ";
    }
    cout << intVal; 
    for (int j = 0; j < endSpace; j++) {
        cout << " ";
    }
    cout << "|";
}

//Show one cell in the table
void RecordManager::ShowCell(float floatVal, int length) const
{
    int endSpace = 1;
    int frontSpace = 0;
    string value;

    value = to_string(floatVal);
    value = value.substr(0, value.find('.') + 3);
    frontSpace = length - value.size() - 1;
    for (int j = 0; j < frontSpace; j++) {
        cout << " ";
    }
    cout << value;
    for (int j = 0; j < endSpace; j++) {
        cout << " ";
    }
    cout << "|";
}

int RecordManager::CalculateCharSize(wstring& charVal) const {
    int size = charVal.size();
    int charSize = 0;
    char* p = (char*)charVal.c_str();
    for (int i = 0; i < size; i++) {
        if (*(p+1) == 0)charSize += 1;
        else charSize += 2;
        p += 2;
    }
    return charSize;
}

//Show one cell in the table
void RecordManager::ShowCell(wstring charVal, int length) const
{
    int endSpace = 1;
    int frontSpace = 0;
    int charSize = CalculateCharSize(charVal);
    //charVal = charVal.substr(0, charSize);

    wcout.imbue(std::locale("chs"));
    frontSpace = length - charSize - 1;
    for (int j = 0; j < frontSpace; j++) {
        cout << " ";
    }
    wcout << charVal;
    for (int j = 0; j < endSpace; j++) {
        cout << " ";
    }
    cout << "|";
}

void* RecordManager::GetAttrValue(string tableName, string attrName, void* recordAddress) const {
    tableInfoNode* info = myCatalogManager->GetTableInfo(tableName);
    int index = 0;
    for (int i = 0; i < info->attrNum; i++) {
        if (info->attrName[i] == attrName) {
            index = i;
            break;
        }
    }
    int attrType = myCatalogManager->GetAttrType(tableName, info->attrName[index]);
    int position = 0;
    for (int i = 0; i < index; i++) {
        if (info->attrInfo[0][i] == INT)   position += sizeof(int);
        if (info->attrInfo[0][i] == FLOAT) position += sizeof(float);
        if (info->attrInfo[0][i] == CHAR)  position += info->attrInfo[1][i] * 2;
    }

    void* buffer = new char[MAX_CHAR_LENGTH * 2];
    memset(buffer, 0, MAX_CHAR_LENGTH * 2);
    
    switch (attrType) {
    case INT:
        memcpy(buffer, (char*)recordAddress + position, sizeof(int));
        break;
    case FLOAT:
        memcpy(buffer, (char*)recordAddress + position, sizeof(float));
        break;
    case CHAR:
        memcpy(buffer, (char*)recordAddress + position, (size_t)info->attrInfo[1][index] * 2);
        break;
    }
    return buffer;
}

int RecordManager::OpTypeCast(string attrOp) {
    string attrOps[6] = {
        "=", "<>", ">", "<", ">=", "<="
    };
    int ops[6] = {
        EQUAL, UNEQUAL, GREATER_THAN, LESS_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL
    };
    for (int i = 0; i < 6; i++) {
        if (attrOp == attrOps[i]) {
            return ops[i];
        }
    }
}

bool RecordManager::RecordMatch(int intVal, int op, wstring val)
{
    int value = _wtoi(val.c_str());

    switch (op) {
    case EQUAL:
        if (intVal == value)return true;
        else return false;
    case UNEQUAL:
        if (intVal != value)return true;
        else return false;
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
    default:
        return false;
        break;
    }
}

bool RecordManager::RecordMatch(float floatVal, int op, wstring val)
{
    float value = _wtof(val.c_str());

    switch (op) {
    case EQUAL:
        if (floatVal == value)return true;
        else return false;
    case UNEQUAL:
        if (floatVal != value)return true;
        else return false;
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
    default:
        return false;
        break;
    }
}

bool RecordManager::RecordMatch(wstring charVal, int op, wstring val)
{
    switch (op) {
    case EQUAL:
        if (charVal == val)return true;
        else return false;
    case UNEQUAL:
        if (charVal != val)return true;
        else return false;
    case LESS_THAN:
        if (charVal < val)return true;
        else return false;
    case GREATER_THAN:
        if (charVal > val)return true;
        else return false;
    case LESS_THAN_OR_EQUAL:
        if (charVal <= val)return true;
        else return false;
    case GREATER_THAN_OR_EQUAL:
        if (charVal >= val)return true;
        else return false;
    default:
        return false;
        break;
    }
}

void RecordManager::UpdateLRUBlock(blockNode* node){
    int key = myBufferManager->GetKey(node);
    pair<int, blockNode*> kv = myBufferManager->GetBlockInPool(key);
    myBufferManager->EraseOldNode(key);
    myBufferManager->PutNodeFront(kv);
    myBufferManager->UpdateHashKey(key);
}

string RecordManager::GenerateTableName(string tableName) {
    string tablePrefix = "TABLE_";
    tableName = tablePrefix + tableName;
    return tableName;
}
