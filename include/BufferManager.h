#pragma once

/**
    Date: 2020/05/13
    Author: Cindy Liu
    All Rights Reserved.
*/

#ifndef BUFFER_MANAGER

#include <unordered_map>
#include <list>
#include "BufferDataStructure.h"
#include "CatalogManager.h"

#define TABLE_NOT_OPEN -10
#define ALL_PINNED     -11
#define STATUS_ERROR   -12
#define FILE_NOT_LOADED -13

class BufferManager {
private:
    CatalogManager* myCatalogManager;
    vector <fileNode*> filePool;
    list <pair<int, blockNode*>> blockPool;
    std::unordered_map<int, list<pair<int, blockNode*>>::iterator> map;
    int fileNum;
    int capacity;
    int size;
    
private:
    char* ReadFile(string fileName, int* size, bool* reachEnd, long offset);
    bool IsFull();
    bool WriteAllBlocksBack();
    blockNode* WriteLRUBlockBack(int& key);
    int WriteBlockBack(blockNode* node);
    int WriteBlocksBack(blockNode* node);
    void ClearBlockNode(blockNode* node);
    void InsertFileNode(string fileName, fileNode* prevNode, blockNode* newBlock, int offset, int pos);
    string GenerateFileName(string fileName);

public:
    BufferManager(CatalogManager* catalogManager);
    ~BufferManager();
    int GetKey(blockNode* node);
    pair<int, blockNode*> GetBlockInPool(int key);
    void EraseOldNode(int key);
    void PutNodeFront(pair<int, blockNode*> kv);
    void UpdateHashKey(int key);

    blockNode* GetBlock(const string fileName);
    blockNode* GetFirstBlock(string fileName);
    blockNode* GetNextBlock(blockNode* blocknode);
    blockNode* GetEmptyBlock();
    fileNode* CreateBlock(string fileName, int fileOffset);
    int FindBlock(string fileName);
    int DropFile(string fileName);
    void SetPinned(blockNode* myBlockNode);
    void ClearPinned(blockNode* myBlockNode);
    void SetDirty(blockNode* myBlockNode);
};

#endif //BUFFER_MANAGER
