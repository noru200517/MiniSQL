#include "IndexManager.h"
#include "API.h"
#include <iostream>

#pragma warning(disable : 4996)

int IndexManager::GetIndexMessage(string IndexName)
{
	return 0;
}

bool IndexManager::HasIndex(string tableName, string attrName) {
	bool hasIndex = false;
	for (int i = 0; i < indexes.size(); i++) {
		if (indexes[i].tableName == tableName && indexes[i].attrName == attrName) {
			hasIndex = true;
			break;
		}
	}
	return hasIndex;
}

bool IndexManager::HasIndex(string indexName) {
	bool hasIndex = false;
	for (int i = 0; i < indexes.size(); i++) {
		if (indexes[i].indexName == indexName) {
			hasIndex = true;
			break;
		}
	}
	return hasIndex;
}

//没找到的时候返回一个空串
string IndexManager::GetIndexName(string tableName, string attrName) {
	int pos = 0;
	for (; pos < indexes.size(); pos++) {
		if (indexes[pos].tableName == tableName && indexes[pos].attrName == attrName) {
			return indexes[pos].indexName;
		}
	}
	return "";
}

std::string ws2s(const std::wstring& ws)
{
	std::string curLocale = setlocale(LC_ALL, "");        // curLocale = "C";
	setlocale(LC_ALL, "chs");
	const wchar_t* _Source = ws.c_str();
	size_t _Dsize = 2 * ws.size() + 1;
	char* _Dest = new char[_Dsize];
	memset(_Dest, 0, _Dsize);
	wcstombs(_Dest, _Source, _Dsize);
	std::string result = _Dest;
	delete[]_Dest;
	setlocale(LC_ALL, curLocale.c_str());
	return result;
}

int IndexManager::CreateIndexProcess(string myIndexName, string tableName, string attrName)
{
	TreeNode* root = new TreeNode();
	blockNode* block = myBufferManager->GetFirstBlock(tableName);
	blockNode* treeBlock = myBufferManager->GetEmptyBlock();
	void* record;
	int recordSize = myAPI->GetRecordSize(tableName);
	int offset = 0;

	int attrType = myAPI->GetAttrType(tableName, attrName);
	//首先建一棵空树
	BplusTree* myTree = CreateBplusTree(tableName, myIndexName, attrType);
	//把空树推到Pool里
	myBplusTrees[myIndexName] = myTree;
	//在index_name的池子里推一条记录
	indexInfo newInfo(myIndexName, tableName, attrName);
	indexes.push_back(newInfo);

	//然后把tableInfo那里给set了
	myAPI->SetIndex(tableName, attrName, true);

	string stringVal = "";
	void* val;
	while (block) {
		offset = 0;
		//无论删除与否都返回当条记录，下做删除检验。仅当走完块以后会做返回空值。
		record = myAPI->SelectRecord(block, recordSize, offset);
		while (record) {
			if (myAPI->IsDelete(block, recordSize, offset)) {
				offset += recordSize;
				record = myAPI->SelectRecord(block, recordSize, offset);
				continue;
			}
			val = myAPI->GetAttrValue(tableName, attrName, record);
			stringVal = TypeCastToString(val, attrType);
			myTree->InsertIntoNode(record, stringVal);
			offset += recordSize;
			record = myAPI->SelectRecord(block, recordSize, offset);
		}
		block = myBufferManager->GetNextBlock(block);
	}

	return 1;
}

string IndexManager::TypeCastToString(void* val, int type) {
	//GetAttrVal是不是内存泄漏了啊
	//是，给我删了
	int intVal = 0;
	int floatVal = 0.0f;
	wstring wcharVal = L"";
	string charVal = "";
	/*
	是不是存wstring比较好？这样中文也可以……但是中文本来也建不了index吧！
	或者这里来一个wstring to string然后再存这样……
	*/

	switch (type) {
	case INT:
		intVal = *(int*)val;
		delete[] val;
		return to_string(intVal);
	case FLOAT:
		floatVal = *(float*)val;
		delete[] val;
		return to_string(floatVal);
	case CHAR:
		wcharVal = (wstring)((wchar_t*)val);
		charVal = ws2s(wcharVal);
		delete[] val;
		return charVal;
	}
}

int IndexManager::OpTypeCast(string attrOp) {
	string attrOps[6] = {
		"=", "<>", ">", "<", ">=", "<="
	};
	int ops[6] = {
		EQUAL, UNEQUAL, GREATER_THAN, LESS_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL
	};
	for (int i = 0; i < 6; i++) {
		if (attrOp == attrOps[i]) {
			return ops[i];
		}
	}
}

int IndexManager::DropIndexProcess(string indexName)
{
	//先把catalogManager里面的index给设置成0
	//其实主键的index应该是不能设置成0的，我暂时先不做这个处理了
	string attrName;
	string tableName;
	for (int i = 0; i < indexes.size(); i++) {
		if (indexes[i].indexName == indexName) {
			attrName = indexes[i].attrName;
			tableName = indexes[i].tableName;
			break;
		}
	}
	myAPI->SetIndex(tableName, attrName, false);

	//然后把这棵树（应该是一定有这棵B+树的）里面的所有record空间释放掉, 把每个结点删掉
	auto iter = myBplusTrees.find(indexName);
	BplusTree* myTree = iter->second; 
	DestroyBplusTree(myTree);

	//把这个index的记录从indexes的记录里干掉
	auto idx = indexes.begin();
	int pos = 0;
	for (pos = 0; pos < indexes.size(); pos++) {
		if (indexes[pos].indexName == indexName) {
			break;
		}
		idx++;
	}
	if(pos == indexes.size()){}
	else {
		indexes.erase(idx);
	}

	//最后把这棵树的记录从池子里清掉, 然后干掉这棵树的指针
	myBplusTrees.erase(indexName);
	delete myTree;

	return 1;
}

int IndexManager::InsertRecord(string indexName, string attrVal, int recordSize, void* address)
{
	//map找不到是不是返回and的啊
	//这里在做之前应该也会做一重检查
	auto pos = myBplusTrees.find(indexName);
	//record开辟的内存不会在这里回收。如果drop index销毁这棵树，或者quit，这两个时候执行销毁操作。
	void* record = (void*)new char[recordSize]();
	memset(record, 0, recordSize);
	memcpy(record, address, recordSize);

	BplusTree* myTree = nullptr;
	if (pos != myBplusTrees.end()) {
		myTree = pos->second;
		myTree->InsertIntoNode(record, attrVal);
		return 1;
	}
	return 0;
}

int IndexManager::DeleteRecord(string indexName, string attrVal)
{
	return 0;
}

/*
	N_A: 这里不需要考虑
	attribute: 有index的属性
	tableName: 表名
	N_S: restrict的数量
	restricts: 具体存放restrict用的string数组
	show: 是否展示内容，默认应赋值true
	total: 是否展示全部内容，如果total直接走recordManager，不会走index(效率区别不大)
*/
int IndexManager::SelectProcess(int N_A, string attribute[], string indexAttr, string tableName, 
	                            string restricts[], bool show, bool total)
{
	//先找到池子中的这个树
	string indexName = "";
	for (int i = 0; i < indexes.size(); i++) {
		if (indexes[i].tableName == tableName && indexes[i].attrName == indexAttr) {
			indexName = indexes[i].indexName;
			break;
		}
	}
	BplusTree* myTree = myBplusTrees[indexName];
	//pair<string, void*> FindRecord(string val);

	//加工一下restricts
	//目前还只是加工一个where，有and还搞不定的
	int op = OpTypeCast(restricts[1]);
	vector<char*> records;
	//tableInfoNode* tableInfo = myAPI->GetTableInfo(tableName);
	if (op == EQUAL) {
		auto recordPair = myTree->FindRecord(restricts[2]);
		records.push_back((char*)recordPair.second);
		return myAPI->FetchRecords(N_A, attribute, tableName, records);
	}
	else {
		if (op == GREATER_THAN) {
			//这里应该是做了一个copy ctor，个人感觉不算很有效率吧……因为这个拖累了index的速度的话还是要考虑指针
			records = SelectGreaterThan(myTree, restricts[2]);
		}
		else if (op == LESS_THAN) {
			records = SelectLessThan(myTree, restricts[2]);
		}
		else if (op == GREATER_THAN_OR_EQUAL) {
			records = SelectGreaterThanOrEqual(myTree, restricts[2]);
		}
		else if (op == LESS_THAN_OR_EQUAL) {
			records = SelectLessThanOrEqual(myTree, restricts[2]);
		}
		else {
			//<>不在这里做，index的加速很有限，也挺难搞。
			return 0;
		}
		//这里我长脑子了，用的是引用，应该还能快一点。
		return myAPI->FetchRecords(N_A, attribute, tableName, records);
	}

	return 0;
}

vector <char*> IndexManager::SelectGreaterThan(BplusTree* myTree, string val) {
	//首先得找到那个记录
	auto recordPair = myTree->FindRecord(val);
	//那个记录所在的叶子结点
	TreeNode* node = myTree->FindLeafNode(val);
	
	vector <char*> records;
	auto iter = node->recordMap.find(recordPair.first);
	iter++; //因为是大于，第一条是不要的
	while (node) {
		while (iter != node->recordMap.end()) {
			records.push_back((char*)iter->second);
			iter++;
		}
		node = node->next;
		if (node) {
			iter = node->recordMap.begin();
		}
	}
	//这样返回是一个复制吧……也行……也算行？其实效率有点低，但我真是被内存泄漏吓怕了
	return records;
}

vector <char*> IndexManager::SelectLessThan(BplusTree* myTree, string val) {
	//找到第一条记录
	auto recordPair = myTree->FindFirstRecord();
	//找到这个记录对应的TreeNode
	TreeNode* node = myTree->FindLeafNode(recordPair.first);
	vector <char*> records;
	auto iter = node->recordMap.find(recordPair.first);
	while (node) {
		while (iter->first < val && iter != node->recordMap.end()) {
			records.push_back((char*)iter->second);
			iter++;
		}
		if (iter->first >= val) {
			break;
		}
		node = node->next;
		if (node) {
			iter = node->recordMap.begin();
		}
	}
	return records;
}

vector <char*> IndexManager::SelectGreaterThanOrEqual(BplusTree* myTree, string val) {
	//首先得找到那个记录
	auto recordPair = myTree->FindRecord(val);
	//那个记录所在的叶子结点
	TreeNode* node = myTree->FindLeafNode(val);

	vector <char*> records;
	auto iter = node->recordMap.find(recordPair.first);
	while (node) {
		while (iter != node->recordMap.end()) {
			records.push_back((char*)iter->second);
			iter++;
		}
		node = node->next;
		if (node) {
			iter = node->recordMap.begin();
		}
	}
	return records;
}

vector <char*> IndexManager::SelectLessThanOrEqual(BplusTree* myTree, string val) {
	//找到第一条记录
	auto recordPair = myTree->FindFirstRecord();
	//找到这个记录对应的TreeNode
	TreeNode* node = myTree->FindLeafNode(recordPair.first);
	vector <char*> records;
	auto iter = node->recordMap.find(recordPair.first);
	while (node) {
		while (iter->first <= val && iter != node->recordMap.end()) {
			records.push_back((char*)iter->second);
			iter++;
		}
		if (iter->first > val) {
			break;
		}
		node = node->next;
		if (node) {
			iter = node->recordMap.begin();
		}
	}
	return records;
}
