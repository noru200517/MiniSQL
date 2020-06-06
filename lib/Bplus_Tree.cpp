#include "Bplus_Tree.h"
#include <iostream>
//Ӧ����һ��ʮ����
//ÿһ���Ҿ���4K��block��

BplusTree* CreateBplusTree(string tableName, string indexName, int type)
{
	BplusTree* bTree = new BplusTree(indexName, type);
	return bTree;
}

int DestroyBplusTree(BplusTree* myTree) {
	auto firstRecordPair = myTree->FindFirstRecord();
	TreeNode* node = myTree->FindLeafNode(firstRecordPair.first);

	//�Ȱ����н��ļ�¼��ַɾ������Щ��ַ���Ƿ�buffer�¿��ģ�ɾ���ǰ�ȫ�ġ�
	string val = firstRecordPair.first;
	auto pos = node->recordMap.begin();
	while (node) {
		pos = node->recordMap.begin();
		while (pos != node->recordMap.end()) {
			delete[] pos->second;
			pos++;
		}
		node = node->next;
	}
	//�ٰ����е������ɾ����
	DeleteTreeNode(myTree->GetRootNode());
	//�������ֵ�ܲ��Ͻ�����˵�ɡ�
	return 1;
}

//�ݹ�ɾ��
void DeleteTreeNode(TreeNode* node) {
	if (node->isLeaf) {
		delete node;
		return;
	}
	else {
		auto iter = node->children.begin();
		for (; iter != node->children.end(); iter++) {
			DeleteTreeNode(*iter);
		}
		delete node;
	}
}

//��������һ��ȫ��ָ��Ľ����������α����¿��أ�
//Ӧ����ָ�������һ��ſ�����棬����Ч�ʲŸ߰�����
//���⣬emptyNode��Ҫ����buffer�������Ŷ��
int BplusTree::InsertIntoNode(void* address, string val)
{
	/*
	//����ο�
	vector<string> vals;		  //�������Ҷ��㣬����ṹ������š�Ҷ����е���Сֵ��
	vector<TreeNode*> children;   //�������Ҷ��㣬���ָ��Ҷ����ָ��
	bool isLeaf;                  //�Ƿ���Ҷ���
	map <string, void*> recordMap;   //�����Ҷ��㣬����ṹ������ֵ�ʹ�ŵļ�¼��Ӧ
	                              //�����ʱ��ֻҪ��mapһ��ֵ�ԣ��Ϳ��Դﵽÿ����������

	/*
	if (root->recordMap.empty()) {
		std::cout << "empty" << endl;
	}
	*/

	//ֻ��һ�����ڵ㣬�Ҹ��ڵ�δ�������룬Ȼ��ֱ�ӷ���
	if (root->isLeaf == true && root->recordMap.empty() || 
		root->isLeaf == true && root->recordMap.size() < m) {
		root->recordMap[val] = address;
		return 1;
	}
	//ֻ��һ�����ڵ㣬�Ҹ��ڵ����������ѽ��
	if (root->isLeaf == true && root->recordMap.size() == m) {
		root = SplitLeafNode(root);
		TreeNode* childPtr;
		if (val > root->vals[0]) {
			childPtr = root->children[1];
		}
		else {
			childPtr = root->children[0];
		}
		childPtr->recordMap[val] = address;
		UpdateMinLeafVal(root, childPtr);
		return 1;
	}
	//��ֻ��һ�����ڵ㣺�ҵ���Ӧ��Ҷ��㣬�������������ѣ����δ���������
	TreeNode* p = root;
	TreeNode* childPtr;
	while (p->isLeaf == false) {
		childPtr = p->children[0];
		int pos = 1;
		for (; pos < p->vals.size(); pos++) {
			childPtr = p->children[pos];
			if (p->vals[pos] > val) {
				break;
			}
		}
		if (pos != p->vals.size()) {
			childPtr = p->children[(size_t)pos - 1];
		}
		p = childPtr;
	}
	//�˴��Ѿ��ҵ���Ӧ�ò����Ҷ�ӽ��
	if (p->recordMap.size() < m) { //���û���������뼴��
		p->recordMap[val] = address;
		UpdateMinLeafVal(p->parent, p); 
	}
	else {                         //�ܲ��ң����ˣ�����֮
		TreeNode* parent = p->parent;
		TreeNode* tmp = p;
		SplitLeafNode(p);
		int pos = 0;
		for (; pos < parent->children.size(); pos++) {
			if (parent->children[pos] == p) {
				break;
			}
		}
		if (parent->vals[(size_t)pos + 1] < val) {
			p = parent->children[(size_t)pos + 1];
			p->recordMap[val] = address;
		}
		else {
			p->recordMap[val] = address;
		}
		if (parent->children.size() <= m) {       //parentû����
			//���������⣬����µĲ����²���Ľ�㡭��
			//�����ƺ�Ҳ���á��������ȥ��ʱ���Ѿ�Ū���µ��ˡ���
			//p���Ǹ������ѵĽ�㣬Ҫô���p��Ҫô�����һ����㣬��һ�����һ����p����
			UpdateMinLeafVal(parent, tmp);
			return 1;
		}
		else {
			//����Ҫдһ��whileѭ����B+������
			TreeNode* newParent = parent;
			TreeNode* lastParent = parent;
			while (parent) {
				if (parent->children.size() > m) {
					newParent = SplitInternalNode(parent);
					UpdateMinInternalVal(newParent, parent);
				}
				else {
					newParent = parent->parent;
				}
				lastParent = parent;
				parent = newParent;
			}
			if (lastParent && lastParent->parent == nullptr) {
				root = lastParent;
			}
		}
	}
	return 0;
}

//���¸��ڵ��vals���飬��˵����Ӧ���ø�list
//parent�Ǹ��ڵ㣬child�Ǹոձ�������record���Ǹ����
void BplusTree::UpdateMinLeafVal(TreeNode* parent, TreeNode* child) {
	//�����С��Ԫ��
	auto firstRec = child->recordMap.begin();
	int pos = 0;
	for (int i = 0; i < parent->children.size(); i++) {
		if (parent->children[i] == child) {
			pos = i;
			break;
		}
	}
	parent->vals[pos] = firstRec->first;
}

//��child��һ��internal����ʱ��
void BplusTree::UpdateMinInternalVal(TreeNode* parent, TreeNode* child) {
	string firstRec = child->vals[0];
	int pos = 0;
	for (int i = 0; i < parent->children.size(); i++) {
		if (parent->children[i] == child) {
			pos = i;
			break;
		}
	}
	parent->vals[pos] = firstRec;
}

//����һ�������ڵ㡱�������㲻һ�����ã���������child���Ƿ��Ѻ��������㡣
//���ڵ�ķ��Ѻ�Ҷ�ӽ�㲻һ������΢�ѵ㹦��������������
TreeNode* BplusTree::SplitLeafNode(TreeNode* node) {
	
	TreeNode* child1 = node;
	TreeNode* child2 = new TreeNode();
	TreeNode* virtualParent;
	auto firstRec = node->recordMap.begin();
	bool hasParent = true;

	if (node->parent) {
		virtualParent = node->parent;
	}
	else {
		hasParent = false;
		virtualParent = new TreeNode();
		virtualParent->vals.push_back(firstRec->first);
	}

	if (virtualParent->children.size() != 0) {
		auto iter = find(virtualParent->children.begin(), virtualParent->children.end(), child1);
		iter++;
		virtualParent->children.insert(iter, child2);
		child2->parent = virtualParent;
	}
	else {
		virtualParent->children.push_back(child1);
		virtualParent->children.push_back(child2);
		child1->parent = virtualParent;
		child2->parent = virtualParent;
	}
	
	for (int i = 0; i <= m / 2; i++) {
		firstRec++;
	}
	for (; firstRec != node->recordMap.end(); ) {
		child2->recordMap[firstRec->first] = firstRec->second;
		firstRec = child1->recordMap.erase(firstRec);
	}

	//����ҲҪ����һ��Ŷ
	//������½����뵽���ڵ��children����
	firstRec = child1->recordMap.begin();
	auto newFirstRec = child2->recordMap.begin();
	if (hasParent) {
		auto iter = find(virtualParent->vals.begin(), virtualParent->vals.end(), firstRec->first);
		iter++;
		virtualParent->vals.insert(iter, newFirstRec->first);
	}
	else {
		virtualParent->vals.push_back(newFirstRec->first);
	}

	child1->isLeaf = true;
	child2->isLeaf = true;
	child1->next = child2;
	virtualParent->isLeaf = false;

	return virtualParent;
}

TreeNode* BplusTree::SplitInternalNode(TreeNode* node) {

	TreeNode* child1 = node;
	TreeNode* child2 = new TreeNode();
	TreeNode* virtualParent;
	bool hasParent = true;

	auto firstChild = node->children.begin();
	if (node->parent) {
		virtualParent = node->parent;
		auto iter = find(virtualParent->children.begin(), virtualParent->children.end(), child1);
		iter++;
		virtualParent->children.insert(iter, child2);
		child2->parent = virtualParent;
	}
	else {
		hasParent = false;
		virtualParent = new TreeNode();
		virtualParent->vals.push_back(node->vals[0]);
		virtualParent->children.push_back(child1);
		virtualParent->children.push_back(child2);
		child1->parent = virtualParent;
		child2->parent = virtualParent;
	}

	child1->isLeaf = false;
	child2->isLeaf = false;
	virtualParent->isLeaf = false;

	//Ҷ�ӽ��Ĳ����͵ײ�������һ��Ҫһ���ģ���B+��������ٿ�һ�������ɣ�
	int pos = 0;
	for (; pos <= m / 2; pos++) {
		firstChild++;
	}
	for (; firstChild != node->children.end(); ) {
		child2->children.push_back(*firstChild);
		(*firstChild)->parent = child2;
		firstChild = child1->children.erase(firstChild);

		auto iter = find(child1->vals.begin(), child1->vals.end(), child1->vals[pos]);
		child2->vals.push_back(child1->vals[pos]);
		child1->vals.erase(iter);
		
	}

	//���¸��ڵ��vals
	string firstRec = child1->vals[0];
	string newFirstRec = child2->vals[0];
	if (hasParent) {
		auto iter = find(virtualParent->vals.begin(), virtualParent->vals.end(), firstRec);
		iter++;
		virtualParent->vals.insert(iter, newFirstRec);
	}
	else {
		virtualParent->vals.push_back(newFirstRec);
	}

	return virtualParent;
}

pair<string, void*> BplusTree::FindRecord(string val) {
	TreeNode* childPtr = FindLeafNode(val);
	auto iter = childPtr->recordMap.find(val);
	return make_pair(iter->first, iter->second);
}

pair<string, void*> BplusTree::FindFirstRecord() {
	TreeNode* p = root;
	//����ע�����ﷵ����һ���գ������ã������
	//ϣ��������������ͱ����
	if (p->isLeaf) {
		return *p->recordMap.begin();
	}

	TreeNode* childPtr = p->children[0];
	while (childPtr->isLeaf == false) {
		p = childPtr;
		childPtr = p->children[0];
	}
	return make_pair(p->vals[0], childPtr->recordMap[p->vals[0]]);
}

TreeNode* BplusTree::FindLeafNode(string val) {
	TreeNode* p = root;
	TreeNode* childPtr;
	//���ҵ���Ӧ��LeafNode
	while (p->isLeaf == false) {
		childPtr = p->children[0];
		int pos = 1;
		for (; pos < p->vals.size(); pos++) {
			childPtr = p->children[pos];
			if (p->vals[pos] > val) {
				break;
			}
		}
		if (pos != p->vals.size()) {
			childPtr = p->children[(size_t)pos - 1];
		}
		p = childPtr;
	}
	return p;
}

TreeNode* BplusTree::GetRootNode() {
	return root;
}

int BplusTree::DeleteFromNode(void* address, string val)
{
	//lazy delete��һ�㣬��Ȼ��Ҫִ�н��ϲ�����ͦ�˵ģ�
	//�ǣ�Ҫ��Ҫ��һ��delete�򰡣�
	TreeNode* childPtr = FindLeafNode(val);

	return 0;
}