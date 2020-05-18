#pragma once

#include <string>
using namespace std;

#define MAX_BLOCK_NUM 256    //the maximum number of blocks
#define BLOCK_SIZE    8192   //the size of a block (8K)
#define MAX_FILE_NUM  32

typedef struct block blockNode;

/*
 *  fileNode: pointer representing a file.
 */
typedef struct file {
    string fileName;             //name of the file
    struct file* prevFileNode;   //pointer to the previous fileNode of the file
    struct file* nextFileNode;   //pointer to the next fileNode of the file
    blockNode* blocknode;        //the block pointed by this fileNode
    bool fileReachEnd;           //whether the whole file has been written in buffer
    bool valid;                  //valid or not

    file() : fileName(""), prevFileNode(nullptr), nextFileNode(nullptr), blocknode(nullptr), fileReachEnd(false), valid(false) {};
    file(string filename): fileName(filename), prevFileNode(nullptr), nextFileNode(nullptr), blocknode(nullptr), fileReachEnd(false), valid(false) {};
    ~file() {
        if (prevFileNode != nullptr) delete prevFileNode;
        if (nextFileNode != nullptr) delete nextFileNode;
        if (blocknode != nullptr) delete blocknode;
    }
}fileNode;

typedef struct fqueue {
    int head;
    int tail;
    int size;                 //store the current number of blocks in the buffer
    int capacity;             //the total number of blocks that can be put in the buffer
    fileNode* fileQueue[MAX_FILE_NUM] = { nullptr };

    fqueue() : head(0), tail(-1), size(0), capacity(MAX_FILE_NUM) {
        for (int i = 0; i < MAX_FILE_NUM; i++) {
            fileQueue[i] = new fileNode();
        }
    }
    ~fqueue() {
        for (int i = 0; i < MAX_FILE_NUM; i++) {
            delete fileQueue[i];
        }
    }
}fileQueue;

/*
 *  blockNode: pointer to the block's content.
 */
struct block{
    string fileName;         //name of the file
    bool isPinned;           //whether the block can be moved out of buffer
    bool isDirty;            //whether the content of the block has been modified
    long offset;             //the begining of this data block in the file
    int recordNum;
    size_t size;             //the total data size in the block
    size_t sizeUsed;         //the size used
    void* recordAddress;     //the address of the content of the block
    char* deleted;
    fileNode* filenode;      //related fileNode
   
    block() : fileName(""), isPinned(false), isDirty(false), offset(0), recordNum(0),
              size(BLOCK_SIZE), sizeUsed(0), recordAddress(nullptr), filenode(nullptr), deleted(nullptr){};
    block(string filename) : fileName(filename), isPinned(false), isDirty(false), offset(0), recordNum(0),
                             size(BLOCK_SIZE), sizeUsed(0), recordAddress(nullptr), filenode(nullptr), deleted(nullptr) {};
    ~block() {
        /*
        if (recordAddress != nullptr)delete recordAddress;
        if (deleted != nullptr)delete deleted;
        if (filenode != nullptr)delete filenode;
        */
    }
    bool IsFull() {
        return (size == sizeUsed);
    }
};

/**
 *  blockQueue: a circular queue of blockNodes.
 *  The head is the least recently used(LRU) block.
*/
typedef struct bqueue {
    int head;
    int tail;
    int size;                //store the current number of blocks in the buffer
    int capacity;            //the total number of blocks that can be put in the buffer
    blockNode* queue[MAX_BLOCK_NUM] = { nullptr };
   
    bqueue() : head(0), tail(-1), size(0), capacity(MAX_BLOCK_NUM) {
        for (int i = 0; i < capacity; i++) {
            queue[i] = new blockNode();
        }
    };
    ~bqueue() {
        /*
        for (int i = 0; i < capacity; i++) {
            delete queue[i];
        }
        */
    }
    bool IsEmpty() const{
        if (size == 0)return true;
        else return false;
    }
    bool IsFull() const{
        if (size == capacity) {
            return true;
        }
        else return false;
    }
}blockQueue;