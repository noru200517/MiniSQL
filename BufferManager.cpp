#include "BufferManager.h"
#include <algorithm>
#include <iostream>
using namespace std;

BufferManager::BufferManager(CatalogManager* catalogManager) : myBlockQueue(bqueue()), fileNum(0)
{
    myCatalogManager = catalogManager;
    for (int i = 0; i < MAX_FILE_NUM; i++) {
        filePool[i] = new fileNode();
    }
}

BufferManager::~BufferManager()
{
    WriteAllBlocksBack();
    for (int i = 0; i < MAX_FILE_NUM; i++) {
        delete filePool[i];
    }
}

/**
 *  Read a block from the file.
 *
 *  @param string fileName: name of the file.
 *  @param int *size: the number of bytes successsfully read
 *  @param long offset: offset in the file where the block starts
 *
 *  @return char*: the address of the new block
 *
 */
char* BufferManager::ReadFile(string fileName, int* size, bool* reachEnd, long offset)
{
    FILE* fileHandle, *fp;
    char* block = new char[BLOCK_SIZE]; //allocate a new block for the file
    string fullFileName = GenerateFileName(fileName);
    memset(block, 0, BLOCK_SIZE);

    //open the file
    fopen_s(&fileHandle, fullFileName.c_str(), "rb+");
    if (fileHandle == nullptr) {
        cerr << "Error in buffer manager: fail to read the file." << endl;
        return nullptr;
    }
    else {
        //move the file pointer to position specified by offset
        //return 0: succeeded. else: failed.
        if (fseek(fileHandle, offset, SEEK_SET) != 0) {    
            //cerr << "Error in buffer manager: fail to read the file." << endl;
            fclose(fileHandle);
            return nullptr;
        }
        else{
            *size = fread(block, 1, BLOCK_SIZE, fileHandle);
            if (ferror(fileHandle)) {
                cerr << "Error in buffer manager: fail to read the file." << endl;
                fclose(fileHandle);
                return nullptr;
            }
            else if (feof(fileHandle)) {
                *reachEnd = true;
            }
        }
    }
    fclose(fileHandle);

    /*test
    cout << ((char*)block)[0] + '0' << endl;
    cout << ((char*)block)[1] + '0' << endl;
    cout << ((char*)block)[2] + '0' << endl;
    cout << ((char*)block)[3] + '0' << endl;
    */

    return block;
}

/*
    还没测完！还没测完！还没测完！
    必须测完这个模块哈。（指GetBlock）
*/

/**
 *  Get a block of the file.
 *  
 *  @param const string fileName: name of the file
 *  @param long offset: offset of the block from the begining of the file
 *
 *  @return blockNode: the blockNode containing information about the block
 *
 */
blockNode* BufferManager::GetBlock(const string fileName)
{
    int blockIndex = myBlockQueue.head;
    bool found = false;

    fileNode* filenode = nullptr;
    fileNode* nextFileNode = nullptr;
    blockNode* blocknode = nullptr;

    //decide whether the table exists
    if (myCatalogManager->FindTable(fileName) == TABLE_NOT_FOUND) {
        cerr << "Error: table does not exists" << endl;
        return nullptr;
    }

    //traver the filePool to see whether there are blocks of the file
    for (int i = 0; i < MAX_FILE_NUM; i++) {
        if (filePool[i]->fileName.compare(fileName) == 0 && filePool[i]->valid == true) {
            filenode = filePool[i];
            break;
        }
    }

    if (filenode == nullptr) {  //the file is not in filePool: create a new block.
        filenode = CreateBlock(fileName, 0);
        //find an available node in filePool, replace it with new node
        int index = -1;
        if (fileNum != MAX_FILE_NUM) {
            for (int i = 0; i < MAX_FILE_NUM; i++) {
                if (filePool[i]->valid == false) {
                    index = i;
                    break;
                }
            }
        }
        else {
            index = 0;  //default 0. Currently LRU is only applied to blocks.
        }
        filePool[index] = filenode;
        fileNum++;
        return filenode->blocknode;
    }
    else {                      //there is such a file. Find the last block.
        while (filenode->nextFileNode) {
            filenode = filenode->nextFileNode;
        }
        blocknode = filenode->blocknode;
        //if the last block is not full, return this block
        if (!blocknode->IsFull()) {
            return blocknode;
        }
        //else create a block and add it to the end of the linked list.
        else {
            if (filenode->fileReachEnd == true)return nullptr;
            else {
                nextFileNode = CreateBlock(fileName, blocknode->offset + BLOCK_SIZE);
                filenode->nextFileNode = nextFileNode;
                nextFileNode->prevFileNode = filenode;
                return nextFileNode->blocknode;
            }
        }
    }
}


blockNode* BufferManager::GetFirstBlock(string fileName) {
    int blockIndex = myBlockQueue.head;
    bool found = false;

    fileNode* filenode = nullptr;
    fileNode* nextFileNode = nullptr;
    blockNode* blocknode = nullptr;

    //decide whether the table exists
    if (myCatalogManager->FindTable(fileName) == TABLE_NOT_FOUND) {
        cerr << "Error: table does not exists" << endl;
        return nullptr;
    }

    //traverse the filePool to see whether there are blocks of the file
    for (int i = 0; i < MAX_FILE_NUM; i++) {
        if (filePool[i]->fileName.compare(fileName) == 0 && filePool[i]->valid == true) {
            filenode = filePool[i];
            break;
        }
    }
    if (filenode == nullptr) {  //the file is not in filePool: create a new block.
        filenode = CreateBlock(fileName, 0);
        //find an available node in filePool, replace it with new node
        int index = -1;
        if (fileNum != MAX_FILE_NUM) {
            for (int i = 0; i < MAX_FILE_NUM; i++) {
                if (filePool[i]->valid == false) {
                    index = i;
                    break;
                }
            }
        }
        else {
            index = 0;  //default 0. Currently LRU is only applied to blocks.
        }
        filePool[index] = filenode;
        fileNum++;
        return filenode->blocknode;
    }
    else {
        return filenode->blocknode;
    }
}

blockNode* BufferManager::GetNextBlock(string fileName, blockNode* blockNode) {
    fileNode* filenode = blockNode->filenode;
    fileNode* nextFileNode = nullptr;

    //if we have reached the end of the file, return a null pointer.
    if (blockNode->filenode->fileReachEnd == true)return nullptr;

    if (filenode->nextFileNode) {   //if this is not the last block
        return filenode->nextFileNode->blocknode;
    }
    else {                          //create a new block, add to the linked list and return
        nextFileNode = CreateBlock(fileName, blockNode->offset + BLOCK_SIZE);
        filenode->nextFileNode = nextFileNode;
        nextFileNode->prevFileNode = filenode;
        return nextFileNode->blocknode;
    }
}

/*
 *   Create an empty new block, return a pointer pointing to it.
 */
fileNode* BufferManager::CreateBlock(string fileName, int fileOffset)
{
    //create a fileNode first
    fileNode* newNode = new fileNode(fileName);
    blockNode* newBlockNode = nullptr;
    void* recordAddress = (void*)new char[BLOCK_SIZE];
    memset(recordAddress, 0, BLOCK_SIZE);
    newNode->valid = true;
    
    //get record size
    int recordSize = myCatalogManager->GetRecordLength(fileName);
    int totalSize = 0;

    //get a block's space
    if (!myBlockQueue.IsFull()) {   //not full: get a new block
        myBlockQueue.tail = (myBlockQueue.tail + 1) % myBlockQueue.capacity;
        newBlockNode = myBlockQueue.queue[myBlockQueue.tail];
    }
    else {                          //full: write the LRU block back
        int position = WriteLRUBlockBack();
        ClearBlockNode(myBlockQueue.queue[position]);
        newBlockNode = myBlockQueue.queue[position];
    }
    newBlockNode->recordAddress = recordAddress;
    newBlockNode->fileName = fileName;
    newBlockNode->deleted = new char[MAX_ATTR_NUM];
    memset(newBlockNode->deleted, 0, MAX_ATTR_NUM);
    newBlockNode->filenode = newNode;
    newBlockNode->offset = fileOffset;
    newNode->blocknode = newBlockNode;

    //read file into the block
    totalSize = 0;
    bool reachEnd = false;

    //等下，这下面怎么回事。。。
    //if (newNode->fileReachEnd)return newNode;
    recordAddress = ReadFile(fileName, &totalSize, &reachEnd, fileOffset);
    if (reachEnd == true) {
        newNode->fileReachEnd = true;
        newNode->blocknode->sizeUsed = totalSize;
    }
    newBlockNode->recordNum = totalSize / recordSize;
    newNode->blocknode->recordAddress = recordAddress;
    return newNode;
}

int BufferManager::FindBlock(string fileName)
{
    for (int i = 0; i < MAX_FILE_NUM; i++) {
        if (filePool[i]->fileName.compare(fileName) == 0 && filePool[i]->valid == true) {
            return i;
        }
    }
    return FILE_NOT_LOADED;
}

int BufferManager::DropFile(string fileName)
{
    for (int i = 0; i < MAX_FILE_NUM; i++) {
        if (filePool[i]->fileName.compare(fileName) == 0) {
            filePool[i]->valid = false;
        }
    }
    return 0;
}

/**
 *  Write all exiting blocks back when exiting the database.
 *
 *  @return bool: whether the operation succeeded.
 *
 */
bool BufferManager::WriteAllBlocksBack() {
    fileNode* filenode;
    int totalFile = myCatalogManager->GetTableNum();

    for (int i = 0; i < MAX_FILE_NUM; i++) {
        if (filePool[i]->valid == true) {
            filenode = filePool[i];
            if (filenode) {
                if (WriteBlocksBack(filenode->blocknode) != 0) {
                    return false;
                }
            }
        }
    }
    return true;
}

int BufferManager::WriteLRUBlockBack()
{
    int position = myBlockQueue.head;
    int preHead = myBlockQueue.head;
    int step = 0;

    //find the LRU block that is not pinned
    while (position != myBlockQueue.tail) {
        if (myBlockQueue.queue[position]->isPinned == false) break;
        myBlockQueue.head = (myBlockQueue.head + 1) % myBlockQueue.capacity;
        position = myBlockQueue.head;
        step++;
    }
    //if every block is pinned
    if (position == myBlockQueue.tail) {
        myBlockQueue.head = preHead;
        return ALL_PINNED;
    }
    //write the block back
    else{
        myBlockQueue.tail = (myBlockQueue.tail + step) % myBlockQueue.capacity;
        if (WriteBlocksBack(myBlockQueue.queue[position]) != 0) {
            return STATUS_ERROR;
        }
        return position;
    }
}

int BufferManager::WriteBlocksBack(blockNode* node)
{
    FILE* fileHandle;
    string fullFileName = GenerateFileName(node->fileName);
    int offset = node->offset;
    int recordSize = myCatalogManager->GetRecordLength(node->fileName);
    void* buffer = (void*)new char[BLOCK_SIZE];

    //check whether the table has been created
    if (myCatalogManager->FindTable(node->fileName) == TABLE_NOT_FOUND) {
        cerr << "Error: table has not been created" << endl;
        return TABLE_NOT_FOUND;
    }
    //try opening the file
    fopen_s(&fileHandle, fullFileName.c_str(), "w+");
    if (fileHandle == nullptr) {
        cerr << "Error: failed to open table file" << endl;
        return TABLE_NOT_OPEN;
    }

    
    fseek(fileHandle, node->offset, SEEK_SET);
    while (node) {
        memset(buffer, 0, BLOCK_SIZE);
        memcpy(buffer, node->recordAddress, node->sizeUsed);
        for (int i = 0; i < node->recordNum; i++) {
            if (node->deleted[i] == 0) {
                fwrite(buffer, 1, recordSize, fileHandle);
            }
            buffer = (char*)buffer + recordSize;
        }
        node = GetNextBlock(node->fileName, node);
    }

    fclose(fileHandle);

    return 0;
}

void BufferManager::ClearBlockNode(blockNode* node)
{
    node->fileName = "";
    node->filenode = nullptr;
    node->isDirty = false;
    node->isPinned = false;
    node->offset = 0;
    delete node->recordAddress;
    node->recordAddress = nullptr;
    node->size = MAX_BLOCK_NUM;
    node->sizeUsed = 0;
}

string BufferManager::GenerateFileName(string fileName)
{
    string prefix = "TABLE_";
    return prefix + fileName;
}

void BufferManager::SetPinned(blockNode* node)
{
    node->isPinned = true;
}

void BufferManager::ClearPinned(blockNode* node) 
{
    node->isPinned = false;
}

void BufferManager::SetDirty(blockNode* node)
{
    node->isDirty = true;
}
