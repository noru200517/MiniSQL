#include "Bplus_Tree.h"
#include <iostream>
//应该是一个十叉树
//每一个我就做4K的block吧

BplusTree* CreateBplusTree(string tableName, string indexName, int type)
{
	BplusTree* bTree = new BplusTree(indexName, type);
	return bTree;
}

int DestroyBplusTree(BplusTree* myTree) {
	auto firstRecordPair = myTree->FindFirstRecord();
	TreeNode* node = myTree->FindLeafNode(firstRecordPair.first);

	//先把所有结点的记录地址删掉。这些地址都是非buffer新开的，删掉是安全的。
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
	//再把所有的树结点删掉。
	DeleteTreeNode(myTree->GetRootNode());
	//这个返回值很不严谨，再说吧。
	return 1;
}

//递归删除
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

//不，别。做一个全是指针的结点它不香嘛？何必重新开呢？
//应该是指针像虫子一样趴在上面，这样效率才高啊！！
//另外，emptyNode需要考虑buffer满的情况哦。
int BplusTree::InsertIntoNode(void* address, string val)
{
	/*
	//定义参考
	vector<string> vals;		  //如果不是叶结点，这个结构用来存放“叶结点中的最小值”
	vector<TreeNode*> children;   //如果不是叶结点，存放指向叶结点的指针
	bool isLeaf;                  //是否是叶结点
	map <string, void*> recordMap;   //如果是叶结点，这个结构用来将值和存放的记录对应
	                              //插入的时候只要给map一个值对，就可以达到每个结点的有序

	/*
	if (root->recordMap.empty()) {
		std::cout << "empty" << endl;
	}
	*/

	//只有一个根节点，且根节点未满：插入，然后直接返回
	if (root->isLeaf == true && root->recordMap.empty() || 
		root->isLeaf == true && root->recordMap.size() < m) {
		root->recordMap[val] = address;
		return 1;
	}
	//只有一个根节点，且根节点已满：分裂结点
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
	//不只有一个根节点：找到对应的叶结点，如果已满，则分裂；如果未满，则插入
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
	//此处已经找到了应该插入的叶子结点
	if (p->recordMap.size() < m) { //如果没有满，插入即可
		p->recordMap[val] = address;
		UpdateMinLeafVal(p->parent, p); 
	}
	else {                         //很不幸，满了，分裂之
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
		if (parent->children.size() <= m) {       //parent没有满
			//这里有问题，你更新的不是新插入的结点……
			//而且似乎也不用……？插进去的时候已经弄成新的了……
			//p是那个待分裂的结点，要么插进p，要么插进另一个结点，另一个结点一定在p后面
			UpdateMinLeafVal(parent, tmp);
			return 1;
		}
		else {
			//这里要写一个while循环，B+树升层
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

//更新父节点的vals数组，话说这里应该用个list
//parent是父节点，child是刚刚被插入了record的那个结点
void BplusTree::UpdateMinLeafVal(TreeNode* parent, TreeNode* child) {
	//获得最小的元素
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

//当child是一个internal结点的时候
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

//返回一个“父节点”，这个结点不一定有用，它的两个child就是分裂后的两个结点。
//父节点的分裂和叶子结点不一样，稍微费点功夫做两个函数吧
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

	//下面也要做这一段哦
	//将这个新结点加入到父节点的children表里
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

	//叶子结点的叉数和底层数量不一定要一样的，在B+树里可以再开一个参数吧？
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

	//更新父节点的vals
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
	//万万注意这里返回了一个空，别乱用，做检查
	//希望这种特殊情况就别出现
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
	//先找到对应的LeafNode
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
	//lazy delete好一点，不然还要执行结点合并，还挺伤的？
	//那，要不要做一个delete域啊？
	TreeNode* childPtr = FindLeafNode(val);

	return 0;
}