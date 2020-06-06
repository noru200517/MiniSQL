#pragma once

#ifndef B_PLUS_TREE_H
#define B_PLUS_TREE_H
#include "RecordManager.h"
#include <algorithm>
#include <map>
#define B_PLUS_TREE_SIZE 128         //���������������������ܱ�֤һ��һ��block����ʱ��һ��128

typedef struct node TreeNode;
struct node {
	vector<string> vals;		     //�������Ҷ��㣬����ṹ������š�Ҷ����е���Сֵ�ǡ�
	vector<TreeNode*> children;      //�������Ҷ��㣬���ָ��Ҷ����ָ��
	TreeNode* parent;                //��¼����㣬����Ҫ���ѵ�ʱ����
	TreeNode* next;                  //ֻ��Ҷ�����У�����ָ����һ��Ҷ��
	bool isLeaf;                     //�Ƿ���Ҷ���
	map <string, void*> recordMap;   //�����Ҷ��㣬����ṹ������ֵ�ʹ�ŵļ�¼��Ӧ
	                                 //�����ʱ��ֻҪ��mapһ��ֵ�ԣ��Ϳ��Դﵽÿ����������
	vector<bool> isDeleted;          //ɾ������ɾ����1��δɾ����0

	node(): parent(nullptr), next(nullptr), isLeaf(true) {}
	node(int m): parent(nullptr), next(nullptr), isLeaf(true){

	}
	node(string mVal, int m): parent(nullptr), next(nullptr), isLeaf(true){
		
	}
};

class BplusTree {
private:
	int m;		//����
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

//����
BplusTree* CreateBplusTree(string tableName, string indexName, int type);
//drop�����ͷſռ�
int DestroyBplusTree(BplusTree* myTree);
void DeleteTreeNode(TreeNode* node);
#endif