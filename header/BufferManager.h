#pragma once

/**
    Date: 2020/05/13
    Author: Cindy Liu
    All Rights Reserved.
*/

#ifndef BUFFER_MANAGER

#include "BufferDataStructure.h"
#include "CatalogManager.h"

#define TABLE_NOT_OPEN -10
#define ALL_PINNED     -11
#define STATUS_ERROR   -12
#define FILE_NOT_LOADED -13

class BufferManager {
private:
    blockQueue myBlockQueue;
    fileNode* filePool[MAX_FILE_NUM];
    int fileNum;
    CatalogManager* myCatalogManager;
    char* ReadFile(string fileName, int* size, bool* reachEnd, long offset);
    bool WriteAllBlocksBack();
    int WriteLRUBlockBack();
    int WriteBlocksBack(blockNode* node);
    void ClearBlockNode(blockNode* node);
    string GenerateFileName(string fileName);

public:
    BufferManager(CatalogManager* catalogManager);
    ~BufferManager();
    blockNode* GetBlock(const string fileName);
    blockNode* GetFirstBlock(string fileName);
    blockNode* GetNextBlock(string fileName, blockNode* blockNode);
    fileNode* CreateBlock(string fileName, int fileOffset);
    int FindBlock(string fileName);
    int DropFile(string fileName);
    void SetPinned(blockNode* myBlockNode);
    void ClearPinned(blockNode* myBlockNode);
    void SetDirty(blockNode* myBlockNode);
};

#endif BUFFER_MANAGER
