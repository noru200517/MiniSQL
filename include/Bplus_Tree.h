#pragma once

#ifndef B_PLUS_TREE_H
#define B_PLUS_TREE_H
#include "RecordManager.h"
#include <algorithm>
#include <map>
#define B_PLUS_TREE_SIZE 128         //这个参数可以外面给，就能保证一次一个block，暂时扔一个128

typedef struct node TreeNode;
struct node {
	vector<string> vals;		     //如果不是叶结点，这个结构用来存放“叶结点中的最小值们”
	vector<TreeNode*> children;      //如果不是叶结点，存放指向叶结点的指针
	TreeNode* parent;                //记录父结点，满了要分裂的时候用
	TreeNode* next;                  //只有叶结点才有，用来指向下一个叶子
	bool isLeaf;                     //是否是叶结点
	map <string, void*> recordMap;   //如果是叶结点，这个结构用来将值和存放的记录对应
	                                 //插入的时候只要给map一个值对，就可以达到每个结点的有序
	vector<bool> isDeleted;          //删除域，已删除给1，未删除给0

	node(): parent(nullptr), next(nullptr), isLeaf(true) {}
	node(int m): parent(nullptr), next(nullptr), isLeaf(true){

	}
	node(string mVal, int m): parent(nullptr), next(nullptr), isLeaf(true){
		
	}
};

class BplusTree {
private:
	int m;		//叉数
	int type;   
	string indexName;
	TreeNode* root;
private:
	TreeNode* SplitLeafNode(TreeNode* node);
	TreeNode* SplitInternalNode(TreeNode* node);
	void UpdateMinLeafVal(TreeNode* parent, TreeNode* child);
	void UpdateMinInternalVal(TreeNode* parent, TreeNode* child);

public:
	BplusTree(string mIndexName, int mType) :
		m(B_PLUS_TREE_SIZE), 
		type(mType), 
		indexName(mIndexName),
		root(new TreeNode(B_PLUS_TREE_SIZE))
	{
		//root = CreateTree();
	}
	int InsertIntoNode(void* address, string val);
	pair<string, void*> FindFirstRecord();
	pair<string, void*> FindRecord(string val);
	TreeNode* FindLeafNode(string val);
	TreeNode* GetRootNode();
	int DeleteFromNode(void* address, string val);
};

//建树
BplusTree* CreateBplusTree(string tableName, string indexName, int type);
//drop树，释放空间
int DestroyBplusTree(BplusTree* myTree);
void DeleteTreeNode(TreeNode* node);
#endif