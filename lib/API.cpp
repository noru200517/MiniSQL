#include "API.h"
#include <iostream>
using namespace std;

API::API(CatalogManager* catalogManager, BufferManager* bufferManager, RecordManager* recordManager)
{
    myCatalogManager = catalogManager;
    myBufferManager = bufferManager;
    myRecordManager = recordManager;
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
int API::CreateTable(string tableName)
{
    return myRecordManager->CreateTable(tableName);
}

/**
 *  Drop a table.
 *
 *  @return int: if return 0, table drop failed.
 *  @            if return 1, table drop succeeded.
 */
int API::DropTable(string tableName)
{
    return myRecordManager->DropTable(tableName);
}

/**
 *  Insert records into a table.
 *
 *  @param string tableName: the table to insert a record into.
 *  @param void* record: a pointer to the record.
 *  @param int recordLength: the length of the record.
 *  @return int: if return 0, insertion failed. If return 1, insertion succeeded.
 */
int API::InsertRecord(string tableName, void* record, int recordLength)
{
    blockNode* blocknode = myBufferManager->GetBlock(tableName);
    blockNode* nextBlockNode;
    int result = 0;

    if (blocknode == nullptr) {
        cerr << "Error: buffer allocation failed" << endl;
        return 0;
    }
    //if the block is full && do not have enough space, we also need to create a new block
    else if (blocknode->IsFull() || blocknode->size - blocknode->sizeUsed < recordLength) {
        nextBlockNode = myBufferManager->GetNextBlock(tableName, blocknode);
        //if we have reached the end of file
        if (nextBlockNode == nullptr) {
            nextBlockNode = myBufferManager->CreateBlock(tableName, blocknode->offset + BLOCK_SIZE)->blocknode;
        }
        result = myRecordManager->InsertRecord(blocknode, record, recordLength);
    }
    //if the block is fetched and there is enough empty space left
    else {
        result = myRecordManager->InsertRecord(blocknode, record, recordLength);
    }

    return result;
}

/**
 *  Select records from a table.
 *
 *  @param int op: the operator(=, <>, <, <=, >, >=)
 *  @param string attribute: the name of the attribute.
 *  @param string val: the value of this attribute specified in the query.
 *  e.g. select * from student where [   sno      =    "00001" ]
 *                                    attribute   op     val
 *  @param string tableName: name of the table to select records from.
 *
 *  @return string: the name of the temporary table to store the selected records.
 */
string API::SelectRecord(int op, string attribute, string val, string tableName)
{
    int position = myCatalogManager->FindTable(tableName);
    blockNode* blocknode;
    fileNode* filenode;
    tableInfoNode* tableInfo;
    vector <void*> records;
    void* buffer;
    unsigned char* recordFormat;
    int offset = 0;
    int recordSize;

    int attrPos = 0;
    int attrType = 0;
    int attrSize = 0;
    int intVal = 0;
    float floatVal = 0.0f;
    char* charVal = new char[MAX_CHAR_LENGTH];

    //return if table has not been created
    if (position == TABLE_NOT_FOUND) {
        cerr << "Error: table has not been created" << endl;
        return (string)"";
    }

    //get table info and the first block
    tableInfo = myCatalogManager->GetTableInfo(tableName);
    blocknode = myBufferManager->GetFirstBlock(tableName);
    //get recordSize, recordFormat
    recordSize = tableInfo->recordSize;
    recordFormat = tableInfo->recordFormat;

    //if received nullptr, return
    if (blocknode == nullptr) {
        return (string)"";
    }
    if (tableInfo == nullptr) {
        return (string)"";
    }

    //if the block is created but is empty, return empty string
    filenode = blocknode->filenode;
    if (blocknode->recordNum == 0) {    //there is no record in the first buffer
        return (string)"";
    }
    else {
        //find in the record the value of a specific attribute
        attrPos = myCatalogManager->FindAttrPosition(attribute, tableInfo);
        if (attrPos == STATUS_ERROR) return (string)"";
        //find the type of attribute
        attrType = myCatalogManager->FindAttrType(attribute, &attrSize, tableInfo);
        if (attrType == STATUS_ERROR) return (string)"";

        while (blocknode) {
            offset = 0;
            for(int i=0; i<blocknode->recordNum; i++) {
                //get the whole record
                buffer = myRecordManager->SelectRecord(blocknode, recordSize, offset);
                offset += recordSize;
                //if record has been deleted
                if (buffer == nullptr)continue;     
                //get the value and compare
                switch (attrType) {
                case INT:
                    memcpy(&intVal, (char*)buffer + attrPos, sizeof(int));
                    if (myRecordManager->RecordMatch(intVal, op, val)) {
                        records.push_back(buffer);
                    }
                    break;
                case FLOAT:
                    memcpy(&floatVal, (char*)buffer + attrPos, sizeof(float));
                    if (myRecordManager->RecordMatch(floatVal, op, val)) {
                        records.push_back(buffer);
                    }
                    break;
                case CHAR:
                    //charVal初始化哈
                    memcpy(charVal, (char*)buffer + attrPos, attrSize);
                    if (myRecordManager->RecordMatch(charVal, op, val, attrSize)) {
                        records.push_back(buffer);
                    }
                    break;
                }
            }
            blocknode = myBufferManager->GetNextBlock(tableName, blocknode);
        }
    }
    
    //if no record satisfies the given requirement, break
    if (records.size() == 0)return (string)"";

    //drop previous intermediate tables
    string tmpFileName = GenerateTmpFileName(tableName);
    position = myCatalogManager->FindTable(tmpFileName);
    if(position != TABLE_NOT_FOUND){
        myRecordManager->DropTable(tmpFileName);
        myCatalogManager->DropTableCatalog(tmpFileName, position);
        myBufferManager->DropFile(tmpFileName);
    }

    //generate a tmpFile to store intermediate results
    if (myRecordManager->CreateTable(tmpFileName) == 0) {
        cerr << "Error: create table failed" << endl;
        return (string)"";
    }
    if (myCatalogManager->CreateTableCatalog(tmpFileName, recordSize, tableInfo->attributeNum, tableInfo->attributeName, recordFormat, tableInfo->recordUnique) != 0) {
        return (string)"";
    }
    blocknode = myBufferManager->GetFirstBlock(tmpFileName);
    if (blocknode == nullptr) {
        return (string)"";
    }

    //write into the block of tmpFile
    for (int i = 0; i < records.size(); i++) {
        if (blocknode->IsFull() || blocknode->size - blocknode->sizeUsed < recordSize) {
            blocknode = myBufferManager->GetNextBlock(tmpFileName, blocknode);
        }
        else {
            myRecordManager->InsertRecord(blocknode, records[i], recordSize);
        }
    }
    return tmpFileName;
}

/**
 *  Show all records in a table.
 *
 *  @param string tableName: the table to insert a record into.
 *
 *  @return int: if return STATUS_ERROR, insertion failed. Else return the number of rows in the table.
 */
int API::ShowAll(string tableName)
{
    //variables used to get records
    tableInfoNode* tableInfo;
    blockNode* blocknode;
    vector <void*> records;
    int recordSize = 0;
    int offset = 0;
    void* buffer;

    //variables used to show records
    vector < pair<string, int> > tableHead;
    unsigned char* recordFormat;
    int cellLength;
    string charVal;
    int intVal = 0;
    float floatVal = 0.0f;
    int pos = 0, charSize = 0;

    //get table info and the first block
    tableInfo = myCatalogManager->GetTableInfo(tableName);
    if (tableInfo == nullptr) {
        return STATUS_ERROR;
    }
    blocknode = myBufferManager->GetFirstBlock(tableName);
    //get recordSize, recordFormat
    recordSize = tableInfo->recordSize;
    recordFormat = tableInfo->recordFormat;

    //get records and put them into the vector "records"
    while (blocknode) {
        for (int i = 0; i < blocknode->sizeUsed; i += tableInfo->recordSize) {
            buffer = myRecordManager->SelectRecord(blocknode, recordSize, offset);
            //if record has been deleted
            offset += recordSize;
            if (buffer == nullptr)continue;     
            records.push_back(buffer);
        }
        if (blocknode->filenode->fileReachEnd)break;
        else blocknode = myBufferManager->GetNextBlock(tableName, blocknode);
    }
    
    //explain and show records
    //这里应该用format里面记录的字长的哈
    buffer = (void*)new char[recordSize];
    memset(buffer, 0, recordSize);
    for (int i = 0; i < tableInfo->attributeNum; i++) {
        charSize = (int)recordFormat[pos * 2 + 1];
        if (charSize * 2 > tableInfo->attributeName[i].size() * 2 && recordFormat[pos * 2] == CHAR) {
            tableHead.push_back(make_pair(tableInfo->attributeName[i], charSize * 2));
        }
        else {
            tableHead.push_back(make_pair(tableInfo->attributeName[i], tableInfo->attributeName[i].size() * 2));
        }
        pos++;
    }
    PrintHline(tableHead);
    PrintHead(tableHead);
    PrintHline(tableHead);
    for (int i = 0; i < records.size(); i++) {          //for each record
        cout << "|";
        pos = 0;
        offset = 0;
        for (int j = 0; j < tableInfo->attributeNum; j++) { //for each attribute
            //cout << records.size() << endl;
            switch (recordFormat[pos * 2]) {
            case INT:
                memcpy(buffer, (char*)records[i] + offset, sizeof(int));
                intVal = *(int*)buffer;
                ShowCell(intVal, tableHead[j].second);
                offset += sizeof(int);
                break;
            case FLOAT:
                memcpy(buffer, (char*)records[i] + offset, sizeof(float));
                floatVal = *(float*)buffer;
                ShowCell(floatVal, tableHead[j].second);
                offset += sizeof(float);
                break;
            case CHAR:
                charSize = (int)recordFormat[pos * 2 + 1];
                memcpy(buffer, (char*)records[i] + offset, charSize);
                charVal = (string)((char*)buffer);
                ShowCell(charVal, tableHead[j].second, charSize);
                offset += charSize;
                break;
            }
            memset(buffer, 0, recordSize);
            //cout << records.size() << endl;
            pos++;
        }   
        cout << endl;
    }
    PrintHline(tableHead);

    int s = records.size();
    //return the number of records fetched
    return s;
}

/**
 *  Delete all records in the table.
 *
 *  @param string tableName: the table to insert a record into.
 * 
 *  @return int: if return 0, insertion failed. Else (这里应该return个啥啊。）
 */
int API::ClearAllRecords(string tableName)
{
    tableInfoNode* tableInfo;
    blockNode* blocknode;
    int position;

    //Drop the old table
    position = myCatalogManager->FindTable(tableName);
    if (position != TABLE_NOT_FOUND) {
        myRecordManager->DropTable(tableName);
        myRecordManager->CreateTable(tableName);
    }

    //clear records in buffer
    position = myBufferManager->FindBlock(tableName);
    if (position == FILE_NOT_LOADED) {
        return 0;
    }
    else {
        blocknode = myBufferManager->GetFirstBlock(tableName);
        while (blocknode) {
            memset(blocknode->recordAddress, 0, blocknode->size);
            memset(blocknode->deleted, 0, MAX_ATTR_NUM);
            blocknode->isDirty = false;
            blocknode->isPinned = false;
            blocknode->recordNum = 0;
            blocknode->sizeUsed = 0;
            blocknode = myBufferManager->GetNextBlock(tableName, blocknode);
        }
        return 0;
    }
}

/**
 *  Delete all records in the table.
 *
 *  @param string tableName: the table to delete a record from.
 *  @param string tmpTableName: the table used to store the records that should be deleted
 *
 *  @return int: if return 0, deletion failed. If return 1, deletion succeeded.
 */
int API::DeleteRecords(string tableName, string tmpTableName)
{
    blockNode* tmpBlock;
    blockNode* deleteBlock;
    tableInfoNode* tableInfo;
    int recordSize;
    char* buffer;
    char* recordToCheck;
    void* recordAddress;
    void* tmpAddress;
    int pos = 0;
    int tmpOffset = 0;
    int deleteOffset = 0;

    //if the tables do not exist, return 0 (no record deleted)
    if (myCatalogManager->FindTable(tableName) == TABLE_NOT_FOUND) {
        return 0;
    }
    if (myCatalogManager->FindTable(tmpTableName) == TABLE_NOT_FOUND) {
        return 0;
    }
    recordSize = myCatalogManager->GetRecordLength(tableName);
    buffer = new char[recordSize];
    memset(buffer, 0, recordSize);
    recordToCheck = new char[recordSize];
    memset(recordToCheck, 0, recordSize);

    tmpBlock = myBufferManager->GetFirstBlock(tmpTableName);
    deleteBlock = myBufferManager->GetFirstBlock(tableName);

    tmpAddress = tmpBlock->recordAddress;
    memcpy(buffer, tmpAddress, recordSize);
    recordAddress = deleteBlock->recordAddress;
    memcpy(recordToCheck, recordAddress, recordSize);

    while (1) {
        if (deleteBlock->deleted[pos] == 1) {   //if the record has already been deleted, neglect it
            pos++;
            deleteOffset += recordSize;
            recordAddress = (char*)recordAddress + recordSize;
        }  
        else {                                  //if the record is not deleted
            if (CompareRecord(recordToCheck, buffer, recordSize)) {
                deleteBlock->deleted[pos] = 1;
                tmpAddress = (char*)tmpAddress + recordSize;
                tmpOffset += recordSize;
                if (tmpOffset >= tmpBlock->sizeUsed) {
                    tmpBlock = myBufferManager->GetNextBlock(tmpTableName, tmpBlock);
                    tmpOffset = 0;
                    if (tmpBlock == nullptr)break;
                    tmpAddress = tmpBlock->recordAddress;
                }
            }
            recordAddress = (char*)recordAddress + recordSize;
            pos++;
            deleteOffset += recordSize;
            if (deleteOffset >= deleteBlock->sizeUsed) {
                deleteBlock = myBufferManager->GetNextBlock(tableName, tmpBlock);
                recordAddress = deleteBlock->recordAddress;
                deleteOffset = 0;
                pos = 0;
            }
        }
    }

    //delete the tmp file
    pos = myCatalogManager->FindTable(tmpTableName);
    if (pos != TABLE_NOT_FOUND) {
        myRecordManager->DropTable(tmpTableName);
        myCatalogManager->DropTableCatalog(tmpTableName, pos);
    }

    return 1;
}

/*
 *  Compare two records to see whether they are the same.
 *
 *  @param char* recordToCheck: record to check
 *  @param char* buffer: another record
 *  @param int recordSize: size of the record
 *
 *  @return bool: if the two records are the same, return true; else return false.
 */
bool API::CompareRecord(char* recordToCheck, char* buffer, int recordSize)
{
    for (int i = 0; i < recordSize; i++) {
        if (recordToCheck[i] != buffer[i])return false;
    }
    return true;
}

//Print the hline when showing the result of select query
void API::PrintHline(vector < pair<string, int> > tableHead)
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
void API::PrintHead(vector<pair<string, int>> tableHead)
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

//Show one cell in the table
void API::ShowCell(int intVal, int length)
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
void API::ShowCell(float floatVal, int length)
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

//Show one cell in the table
void API::ShowCell(string charVal, int length, int charSize)
{
    int endSpace = 1;
    int frontSpace = 0;
    charVal = charVal.substr(0, charSize);

    frontSpace = length - charVal.size() - 1;
    for (int j = 0; j < frontSpace; j++) {
        cout << " ";
    } 
    cout << charVal;
    for (int j = 0; j < endSpace; j++) {
        cout << " ";
    }
    cout << "|";
}

string API::GenerateTmpFileName(string tableName)
{
    string prefix = "TEMP_FILE";
    return prefix;
}
