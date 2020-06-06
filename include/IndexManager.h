#pragma once


#ifndef INDEX_MANAGER_H
#define INDEX_MANAGER_H

#include <string>
#include <vector>
#include "BufferManager.h"
#include "Bplus_Tree.h"

using namespace std;

typedef struct inode{
	string indexName;
	string tableName;
	string attrName;
	inode(string myIndexName, string myTableName, string myAttrName):
		indexName(myIndexName),
		tableName(myTableName),
		attrName(myAttrName)
	{}
}indexInfo;

class API;
class IndexManager {
	
	enum {

	};

private:
	vector <indexInfo>  indexes;
	int					indexNum;
	BufferManager*		myBufferManager;
	API*                myAPI;
	map <string, BplusTree*> myBplusTrees; //指向对应B+树的根结点
private:
	string TypeCastToString(void* val, int type);
	int OpTypeCast(string attrOp);
	vector <char*> SelectGreaterThan(BplusTree* myTree, string val);
	vector <char*> SelectLessThan(BplusTree* myTree, string val);
	vector <char*> SelectGreaterThanOrEqual(BplusTree* myTree, string val);
	vector <char*> SelectLessThanOrEqual(BplusTree* myTree, string val);

public:
	IndexManager(BufferManager* BM, API* api): 
		indexNum(0),
		myBufferManager(BM),
		myAPI(api)
	{
	
	}
	void SetAPI(API* api) {
		myAPI = api;
	}
	int GetIndexMessage(string indexName);
	bool HasIndex(string tableName, string attrName);
	bool HasIndex(string indexName);
	string GetIndexName(string tableName, string attrName);
	int CreateIndexProcess(string indexName, string tableName, string attribute);
	int DropIndexProcess(string indexName);
	int InsertRecord(string indexName, string attrVal, int recordSize, void* address);
	int DeleteRecord(string indexName, string attrVal);
	int SelectProcess(int N_A, string attribute[], string indexAttr, string tableName, string restricts[], bool show, bool total);
};

#endif
