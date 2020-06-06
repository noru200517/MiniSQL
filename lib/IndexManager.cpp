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

//û�ҵ���ʱ�򷵻�һ���մ�
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
	//���Ƚ�һ�ÿ���
	BplusTree* myTree = CreateBplusTree(tableName, myIndexName, attrType);
	//�ѿ����Ƶ�Pool��
	myBplusTrees[myIndexName] = myTree;
	//��index_name�ĳ�������һ����¼
	indexInfo newInfo(myIndexName, tableName, attrName);
	indexes.push_back(newInfo);

	//Ȼ���tableInfo�����set��
	myAPI->SetIndex(tableName, attrName, true);

	string stringVal = "";
	void* val;
	while (block) {
		offset = 0;
		//����ɾ����񶼷��ص�����¼������ɾ�����顣����������Ժ�������ؿ�ֵ��
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
	//GetAttrVal�ǲ����ڴ�й©�˰�
	//�ǣ�����ɾ��
	int intVal = 0;
	int floatVal = 0.0f;
	wstring wcharVal = L"";
	string charVal = "";
	/*
	�ǲ��Ǵ�wstring�ȽϺã���������Ҳ���ԡ����������ı���Ҳ������index�ɣ�
	����������һ��wstring to stringȻ���ٴ���������
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
	//�Ȱ�catalogManager�����index�����ó�0
	//��ʵ������indexӦ���ǲ������ó�0�ģ�����ʱ�Ȳ������������
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

	//Ȼ����������Ӧ����һ�������B+���ģ����������record�ռ��ͷŵ�, ��ÿ�����ɾ��
	auto iter = myBplusTrees.find(indexName);
	BplusTree* myTree = iter->second; 
	DestroyBplusTree(myTree);

	//�����index�ļ�¼��indexes�ļ�¼��ɵ�
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

	//����������ļ�¼�ӳ��������, Ȼ��ɵ��������ָ��
	myBplusTrees.erase(indexName);
	delete myTree;

	return 1;
}

int IndexManager::InsertRecord(string indexName, string attrVal, int recordSize, void* address)
{
	//map�Ҳ����ǲ��Ƿ���and�İ�
	//��������֮ǰӦ��Ҳ����һ�ؼ��
	auto pos = myBplusTrees.find(indexName);
	//record���ٵ��ڴ治����������ա����drop index���������������quit��������ʱ��ִ�����ٲ�����
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
	N_A: ���ﲻ��Ҫ����
	attribute: ��index������
	tableName: ����
	N_S: restrict������
	restricts: ������restrict�õ�string����
	show: �Ƿ�չʾ���ݣ�Ĭ��Ӧ��ֵtrue
	total: �Ƿ�չʾȫ�����ݣ����totalֱ����recordManager��������index(Ч�����𲻴�)
*/
int IndexManager::SelectProcess(int N_A, string attribute[], string indexAttr, string tableName, 
	                            string restricts[], bool show, bool total)
{
	//���ҵ������е������
	string indexName = "";
	for (int i = 0; i < indexes.size(); i++) {
		if (indexes[i].tableName == tableName && indexes[i].attrName == indexAttr) {
			indexName = indexes[i].indexName;
			break;
		}
	}
	BplusTree* myTree = myBplusTrees[indexName];
	//pair<string, void*> FindRecord(string val);

	//�ӹ�һ��restricts
	//Ŀǰ��ֻ�Ǽӹ�һ��where����and���㲻����
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
			//����Ӧ��������һ��copy ctor�����˸о��������Ч�ʰɡ�����Ϊ���������index���ٶȵĻ�����Ҫ����ָ��
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
			//<>������������index�ļ��ٺ����ޣ�Ҳͦ�Ѹ㡣
			return 0;
		}
		//�����ҳ������ˣ��õ������ã�Ӧ�û��ܿ�һ�㡣
		return myAPI->FetchRecords(N_A, attribute, tableName, records);
	}

	return 0;
}

vector <char*> IndexManager::SelectGreaterThan(BplusTree* myTree, string val) {
	//���ȵ��ҵ��Ǹ���¼
	auto recordPair = myTree->FindRecord(val);
	//�Ǹ���¼���ڵ�Ҷ�ӽ��
	TreeNode* node = myTree->FindLeafNode(val);
	
	vector <char*> records;
	auto iter = node->recordMap.find(recordPair.first);
	iter++; //��Ϊ�Ǵ��ڣ���һ���ǲ�Ҫ��
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
	//����������һ�����ưɡ���Ҳ�С���Ҳ���У���ʵЧ���е�ͣ��������Ǳ��ڴ�й©������
	return records;
}

vector <char*> IndexManager::SelectLessThan(BplusTree* myTree, string val) {
	//�ҵ���һ����¼
	auto recordPair = myTree->FindFirstRecord();
	//�ҵ������¼��Ӧ��TreeNode
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
	//���ȵ��ҵ��Ǹ���¼
	auto recordPair = myTree->FindRecord(val);
	//�Ǹ���¼���ڵ�Ҷ�ӽ��
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
	//�ҵ���һ����¼
	auto recordPair = myTree->FindFirstRecord();
	//�ҵ������¼��Ӧ��TreeNode
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
