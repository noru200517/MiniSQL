#include "BufferManager.h"
#include <algorithm>
#include <iostream>
using namespace std;

BufferManager::BufferManager(CatalogManager* catalogManager) : 
    fileNum(0), 
    capacity(MAX_BLOCK_NUM),
    size(0)
{
    myCatalogManager = catalogManager;
}

BufferManager::~BufferManager()
{
    WriteAllBlocksBack();
    for (int i = 0; i < filePool.size(); i++) {
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
    _ASSERTE(_CrtCheckMemory());

    //open the file
    fopen_s(&fileHandle, fullFileName.c_str(), "rb");
    if (fileHandle == nullptr) {
        cerr << "Error in buffer manager: fail to read the file." << endl;
        return nullptr;
    }
    else {
        //move the file pointer to position specified by offset
        //return 0: succeeded. else: failed.
        fseek(fileHandle, 0, SEEK_END);
        int end = ftell(fileHandle);
        if (offset >= end) {
            fclose(fileHandle);
            *size = 0;
            *reachEnd = true;
            delete[] block;
            return nullptr;
        }
        else{
            fseek(fileHandle, offset, SEEK_SET);
            *size = fread(block, 1, BLOCK_SIZE, fileHandle);
            if (ferror(fileHandle)) {
                cerr << "Error in buffer manager: fail to read the file." << endl;
                fclose(fileHandle);
                return nullptr;
            }
            else if (feof(fileHandle) || end == offset + BLOCK_SIZE) {
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
    bool found = false;

    fileNode* filenode = nullptr;
    fileNode* nextFileNode = nullptr;
    blockNode* blocknode = nullptr, *nextnode = nullptr;

    //decide whether the table exists
    if (myCatalogManager->FindTable(fileName) == TABLE_NOT_FOUND) {
        cerr << "Error: table does not exists" << endl;
        return nullptr;
    }

    //traverse the filePool to see whether there are blocks of the file
    for (int i = 0; i < filePool.size(); i++) {
        if (filePool[i]->fileName.compare(fileName) == 0 && filePool[i]->valid == true) {
            filenode = filePool[i];
            break;
        }
    }

    if (filenode == nullptr) {  //the file is not in filePool: create a new block.
        filenode = CreateBlock(fileName, 0);
        //这样返回的是第一个block啊，注意一下，这个函数是想要返回最末尾的那个block
        //find an available node in filePool, replace it with new node
        filenode->isHead = true;
        filenode->valid = true;
        filePool.push_back(filenode);
        fileNum++;
        nextnode = GetNextBlock(filenode->blocknode);
        blocknode = filenode->blocknode;
        while (nextnode) {
            nextnode = GetNextBlock(blocknode);
            if (nextnode != nullptr)blocknode = nextnode;
            else break;
        }
        return blocknode;
    }
    else {                      //there is such a file. Find the last block.
        while (filenode->nextFileNode) {
            filenode = filenode->nextFileNode;
        }
        blocknode = filenode->blocknode;
        //if the last block is not full, return this block
        return blocknode;
        /*
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
        */
    }
}

blockNode* BufferManager::GetFirstBlock(string fileName) {
    
    bool found = false;

    fileNode* filenode = nullptr;
    fileNode* nextFileNode = nullptr;
    blockNode* blocknode = nullptr;
    int recordSize = myCatalogManager->GetRecordSize(fileName);

    //decide whether the table exists
    if (myCatalogManager->FindTable(fileName) == TABLE_NOT_FOUND) {
        cerr << "Error: table does not exists" << endl;
        return nullptr;
    }

    int pos = 0;
    //traverse the filePool to see whether there are blocks of the file
    for (int i = 0; i < filePool.size(); i++) {
        if (filePool[i]->fileName.compare(fileName) == 0 && filePool[i]->valid == true) {
            filenode = filePool[i];
            pos = i;
            break;
        }
    }
    if (filenode == nullptr) {  //the file is not in filePool: create a new block.
        filenode = CreateBlock(fileName, 0);
        filenode->isHead = true;
        //find an available node in filePool, replace it with new node
        filePool.push_back(filenode);
        fileNum++;
        return filenode->blocknode;
    }
    else {
        blockNode* newBlock;
        //if the first block exists
        if (filenode->blocknode->offset == 0)return filenode->blocknode;
        else {
            if (!IsFull()) {    //buffer is not full
                newBlock = GetEmptyBlock();
                InsertFileNode(fileName, nullptr, newBlock, 0, pos);
                //因为是新找到的block，所以要推进池子，再更新map
                size++;
                PutNodeFront(make_pair(size, newBlock));
                UpdateHashKey(size);
            }
            else {              //buffer is full, LRU write back
                int key = 0;    //note: this key will be "returned" as key of the block
                newBlock = WriteLRUBlockBack(key);
                InsertFileNode(fileName, nullptr, newBlock, 0, pos);
                //直接把得到的key作为新结点的键值，推到池子头部，更新map
                PutNodeFront(make_pair(key, newBlock));
                UpdateHashKey(key);
            }
        }
        return newBlock;
    }
}

//在某个文件链表中插入一项，并且向buffer中添加对应的block，做blockPool以及hashmap的更新
//该函数仅当blockPool未满时被调用
void BufferManager::InsertFileNode(string fileName, fileNode* prevNode, blockNode* newBlock, int offset, int pos) {

    fileNode* newNode;
    int recordSize = myCatalogManager->GetRecordSize(fileName);

    /*//文件结点成员变量参考
    file(string filename):
    fileName(filename),
    prevFileNode(nullptr),
    nextFileNode(nullptr),
    blocknode(nullptr),
    fileReachEnd(false),
    valid(false),
    isHead(false) {};
    */

    if (prevNode == nullptr) {
        fileNode* firstNode = filePool[pos];
        filePool[pos] = new fileNode(fileName);
        newNode = filePool[pos];
        newNode->nextFileNode = firstNode;
        firstNode->prevFileNode = filePool[pos];
        newNode->blocknode = newBlock;
        newNode->valid = true;
        newNode->isHead = true;
        firstNode->isHead = false;

        //recordAddress(nullptr), filenode(nullptr), deleted(nullptr)
        bool reachEnd = false;
        int size = 0;
        newBlock->fileName = fileName;
        newBlock->offset = offset;
        newBlock->recordAddress = ReadFile(fileName, &size, &reachEnd, 0);
        if (newBlock->recordAddress == nullptr) {
            newBlock->recordAddress = (void*)new char[BLOCK_SIZE];
        }
        newBlock->sizeUsed = size;
        newBlock->recordNum = size / recordSize;
        newBlock->filenode = filePool[pos];
        newBlock->deleted = new char[BLOCK_SIZE / recordSize]();

    }
    else {
        newNode = new fileNode(fileName);
        newNode->nextFileNode = prevNode->nextFileNode;
        newNode->prevFileNode = prevNode;
        prevNode->nextFileNode = newNode;
        newNode->blocknode = newBlock;
        newNode->valid = true;
        newNode->isHead = false;

        //recordAddress(nullptr), filenode(nullptr), deleted(nullptr)
        bool reachEnd = false;
        int size = 0;
        newBlock->fileName = fileName;
        newBlock->offset = offset;
        newBlock->recordAddress = ReadFile(fileName, &size, &reachEnd, offset);
        if (newBlock->recordAddress == nullptr) {
            newBlock->recordAddress = (void*)new char[BLOCK_SIZE];
        }
        newBlock->sizeUsed = size;
        newBlock->recordNum = size / recordSize;
        newBlock->filenode = newNode;
        newBlock->deleted = new char[BLOCK_SIZE / recordSize]();

    }
}

blockNode* BufferManager::GetNextBlock(blockNode* blocknode) {
    fileNode* filenode = blocknode->filenode;
    fileNode* nextFileNode = nullptr;
    string fileName = blocknode->fileName;

    //if we have reached the end of the file, return a null pointer.
    if (blocknode->filenode->fileReachEnd == true)return nullptr;

    if (filenode->nextFileNode) {   //if this is not the last block
        //检查它是不是刚好下一个block，否则要把那个block换上来，插入链表
        nextFileNode = filenode->nextFileNode;
        //如果它刚好是下一个block(两个block的offset是block块的大小)
        if (nextFileNode->blocknode->offset - filenode->blocknode->offset == BLOCK_SIZE) {
            return filenode->nextFileNode->blocknode;
        }
        //如果它不是(两个块的大小有间隔)，把那块文件读出来，插入filePool对应的位置
        //不用在这里更新LRU，尽管这似乎是更好的操作方式，待改正
        //返回这块block
        else {
            blockNode* newBlock;
            if (!IsFull()) {    //buffer is not full
                newBlock = GetEmptyBlock();
                InsertFileNode(fileName, filenode, newBlock, filenode->blocknode->offset + BLOCK_SIZE, 0);
                //因为是新找到的block，所以要推进池子，再更新map
                size++;
                PutNodeFront(make_pair(size, newBlock));
                UpdateHashKey(size);
            }
            else {              //buffer is full, LRU write back
                int key = 0;    //note: this key will be "returned" as key of the block
                newBlock = WriteLRUBlockBack(key);
                InsertFileNode(fileName, filenode, newBlock, filenode->blocknode->offset + BLOCK_SIZE, 0);
                //直接把得到的key作为新结点的键值，推到池子头部，更新map
                PutNodeFront(make_pair(key, newBlock));
                UpdateHashKey(key);
            }
            return newBlock;
        }
    }
    else {                          //last block. create a new block, add to the linked list and return
        nextFileNode = CreateBlock(fileName, blocknode->offset + BLOCK_SIZE);
        filenode->nextFileNode = nextFileNode;
        nextFileNode->prevFileNode = filenode;
        return nextFileNode->blocknode;
    }
}

blockNode* BufferManager::GetEmptyBlock() {
    return new blockNode();
    //开内存啦！
}

/*
 *  Create an empty new block, return a pointer pointing to it.
 *  Update: I think it is not an empty block, but a block with contents in file
 */
fileNode* BufferManager::CreateBlock(string fileName, int fileOffset)
{
    //create a fileNode first
    fileNode* newNode = new fileNode(fileName);
    blockNode* newBlockNode = nullptr;
    void* recordAddress;
    newNode->valid = true;
    
    //get record size
    int recordSize = myCatalogManager->GetRecordSize(fileName);
    int totalSize = 0;
    
    //get a block's space
    if (!IsFull()) {   //not full: get a new block
        newBlockNode = GetEmptyBlock();
        //size: the number of blocks in the blockPool
        size++;
        //add another node into the blockPool(front: recently used)
        blockPool.push_front(make_pair(size, newBlockNode));
        //I make size its key.
        //When the pool is not full, each time size is definitely unique.
        //ONE THING TO NOTICE: WITH <DROP TABLE> WE CAN RELEASE SOME BLOCKS.
        //IN THAT CASE, IMPLEMENTATION OF <GetEmptyBlock> SHOULD CHANGE, 
        //AND SIZE IS NOT A GOOD KEY CHOICE.
        map[size] = blockPool.begin();
    }
    else {                          //full: write the LRU block back
        int key = 0;
        newBlockNode = WriteLRUBlockBack(key);
        blockPool.push_front(make_pair(key, newBlockNode));
        map[key] = blockPool.begin();
    }
    newBlockNode->fileName = fileName;
    newBlockNode->deleted = new char[BLOCK_SIZE / recordSize];
    memset(newBlockNode->deleted, 0, (BLOCK_SIZE / recordSize));
    _ASSERTE(_CrtCheckMemory());
    newBlockNode->filenode = newNode;
    newBlockNode->offset = fileOffset;
    newNode->blocknode = newBlockNode;

    //read file into the block
    totalSize = 0;
    bool reachEnd = false;

    //等下，这下面怎么回事。。。
    //if (newNode->fileReachEnd)return newNode;
    recordAddress = ReadFile(fileName, &totalSize, &reachEnd, fileOffset);
    if (recordAddress == nullptr) {
        recordAddress = new char[BLOCK_SIZE]();
        memset(recordAddress, 0, BLOCK_SIZE);
        _ASSERTE(_CrtCheckMemory());
    }
    if (reachEnd == true) {
        newNode->fileReachEnd = true;
    }
    newNode->blocknode->sizeUsed = totalSize;
    newBlockNode->recordNum = totalSize / recordSize;
    newNode->blocknode->recordAddress = recordAddress;
    return newNode;
}

int BufferManager::FindBlock(string fileName)
{
    for (int i = 0; i < filePool.size(); i++) {
        if (filePool[i]->fileName.compare(fileName) == 0 && filePool[i]->valid == true) {
            return i;
        }
    }
    return FILE_NOT_LOADED;
}

int BufferManager::DropFile(string fileName)
{
    for (int i = 0; i < filePool.size(); i++) {
        if (filePool[i]->fileName.compare(fileName) == 0) {
            filePool[i]->valid = false;
        }
    }
    //为什么不顺便释放掉人家所有的block呢，会省空间的
    //！！！！！！！！！！！！！！！！！！！！！！！！
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

    for (int i = 0; i < filePool.size(); i++) {
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

//这个函数问题大了
blockNode* BufferManager::WriteLRUBlockBack(int &key)
{
    pair<int, blockNode*> lastPair;
    blockNode* lastBlock;
    fileNode* lastFileNode, *tmpNode, * prevNode, *nextNode;
    list<pair<int, blockNode*>>::iterator iter;
    /*
    //this part gets done after PINNED is implemented
    for (iter = blockPool.end(); iter != blockPool.begin(); iter--) {

    }
    */

    //首先：把这块的内容写回文件
    lastPair = blockPool.back();
    int lastKey = lastPair.first;
    key = lastKey;
    lastBlock = map[lastKey]->second;
    WriteBlockBack(lastBlock);

    //其次：把链表中的这个结点删去
    //filePool: do a deleteNode operation
    lastFileNode = lastBlock->filenode;
    nextNode = lastFileNode->nextFileNode;
    prevNode = lastFileNode->prevFileNode;
    if (prevNode) {
        prevNode->nextFileNode = nextNode;
    }
    if (nextNode) {
        nextNode->prevFileNode = prevNode;
    }

    //再次：仅清空这个block，不回收它
    ClearBlockNode(lastBlock);
    //再次：从hashmap和blockPool中擦除这个block，保留它的key并通过参数返回
    //该清空后的block的地址则通过返回值返回
    map.erase(lastKey);     //map是哈希表
    blockPool.pop_back();   //blockPool是真正的block池

    if (!lastFileNode->isHead)delete lastFileNode;
    else if (lastFileNode->isHead && nextNode == nullptr) {
        lastFileNode->valid = false;
    }
    else {
        int pos = 0;
        for (int i = 0; i < filePool.size(); i++) {
            if (filePool[i]->fileName.compare(lastFileNode->fileName) == 0 && filePool[i]->valid == true) {
                pos = i;
                break;
            }
        }
        filePool[pos] = nextNode;
        nextNode->isHead = true;
        delete lastFileNode;
    }
    
    /*
    auto lastPair = cache.back();
    int lastKey = lastPair.first;
    map.erase(lastKey);
    cache.pop_back();
    */
    return lastBlock;
}

int BufferManager::WriteBlockBack(blockNode* node) {
    FILE* fileHandle;
    string fullFileName = GenerateFileName(node->fileName);
    int offset = node->offset;
    int recordSize = myCatalogManager->GetRecordSize(node->fileName);
    char* buffer = new char[BLOCK_SIZE];
    char* p = buffer;
    _ASSERTE(_CrtCheckMemory());

    //check whether the table has been created
    if (myCatalogManager->FindTable(node->fileName) == TABLE_NOT_FOUND) {
        cerr << "Error: table has not been created" << endl;
        return TABLE_NOT_FOUND;
    }
    //try opening the file
    fopen_s(&fileHandle, fullFileName.c_str(), "rb+");
    if (fileHandle == nullptr) {
        cerr << "Error: failed to open table file" << endl;
        return TABLE_NOT_OPEN;
    }
    fseek(fileHandle, node->offset, SEEK_SET);

    memset(buffer, 0, BLOCK_SIZE);
    _ASSERTE(_CrtCheckMemory());
    memcpy(buffer, node->recordAddress, node->sizeUsed);
    _ASSERTE(_CrtCheckMemory());
    for (int i = 0; i < node->recordNum; i++) {
        if (node->deleted[i] == 0) {
            fwrite(p, 1, recordSize, fileHandle);
        }
        p = p + recordSize;
    }

    delete[] buffer;
    _ASSERTE(_CrtCheckMemory());
    fclose(fileHandle);

    return 0;
}

int BufferManager::WriteBlocksBack(blockNode* node)
{
    FILE* fileHandle;
    string fullFileName = GenerateFileName(node->fileName);
    int offset = node->offset;
    int recordSize = myCatalogManager->GetRecordSize(node->fileName);
    char* buffer = new char[BLOCK_SIZE];
    char* p = buffer;

    //check whether the table has been created
    if (myCatalogManager->FindTable(node->fileName) == TABLE_NOT_FOUND) {
        cerr << "Error: table has not been created" << endl;
        return TABLE_NOT_FOUND;
    }
    //try opening the file
    fopen_s(&fileHandle, fullFileName.c_str(), "wb+");
    if (fileHandle == nullptr) {
        cerr << "Error: failed to open table file" << endl;
        return TABLE_NOT_OPEN;
    }

    fseek(fileHandle, node->offset, SEEK_SET);
    while (node) {
        if (node->isDirty == false) {
            node = GetNextBlock(node);
            continue;
        }
        memset(buffer, 0, BLOCK_SIZE);
        memcpy(buffer, node->recordAddress, node->sizeUsed);
        _ASSERTE(_CrtCheckMemory());
        p = buffer;
        for (int i = 0; i < node->recordNum; i++) {
            if (node->deleted[i] == 0) {
                fwrite(p, 1, recordSize, fileHandle);
            }
            else {
                char* emptyBuf = new char[recordSize]();
                memset(emptyBuf, 0, recordSize);
                fwrite(emptyBuf, 1, recordSize, fileHandle);
                fseek(fileHandle, -1 * recordSize, SEEK_CUR);
                delete[] emptyBuf;
            }
            p = p + recordSize;
        }
        node = GetNextBlock(node);
    }

    delete[] buffer;
    fclose(fileHandle);

    return 0;
}

inline void BufferManager::ClearBlockNode(blockNode* node)
{
    node->fileName = "";
    node->filenode = nullptr;
    node->isDirty = false;
    node->isPinned = false;
    node->offset = 0;
    if(node->recordAddress) delete[] node->recordAddress;
    node->recordAddress = nullptr;
    node->size = BLOCK_SIZE;
    node->sizeUsed = 0;
    delete[] node->deleted;
    //shared_ptr<int> A;
}

inline string BufferManager::GenerateFileName(string fileName)
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

inline void BufferManager::SetDirty(blockNode* node)
{
    node->isDirty = true;
}

bool BufferManager::IsFull() 
{
    return (capacity == size);
}

int BufferManager::GetKey(blockNode* node) 
{
    list <pair<int, blockNode*>>::iterator iter;
    for (iter = blockPool.begin(); iter != blockPool.end(); iter++) {
        if (iter->second == node) {
            return iter->first;
        }
    }
    return 0;
}

pair<int, blockNode*> BufferManager::GetBlockInPool(int key) 
{
    return *map[key];
}

void BufferManager::EraseOldNode(int key) 
{
    blockPool.erase(map[key]);
}

void BufferManager::PutNodeFront(pair<int, blockNode*> kv) 
{
    blockPool.push_front(kv); 
}

void BufferManager::UpdateHashKey(int key) 
{
    map[key] = blockPool.begin();
}