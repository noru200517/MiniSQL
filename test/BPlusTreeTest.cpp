#include <iostream>
#include <string>
#include <map>
#include "Bplus_Tree.h"
using namespace std;

int main(void) {
	BplusTree* myTree = CreateBplusTree("student2", "index_id", 3);
	void* recordAddress;
	map <int, string> recordMap;

	if (recordMap.empty()) {
		cout << "empty" << endl;
	}
	recordMap[1] = "aaa";
	cout << recordMap.size() << endl;

	vector<string>name;
	int base = 1080100000;
	for (int i = 0; i < 10000; i++) {
		name.push_back(to_string(base + i));
	}

	for (int i = 0; i < 10000; i++) {
		recordAddress = (void*)new char[32]();
		myTree->InsertIntoNode(recordAddress, name[i]);
	}

	myTree->FindRecord("1080100015");
	
	return 0;
}