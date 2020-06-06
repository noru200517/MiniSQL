#include <iostream>
#include <crtdbg.h>
#include "BufferManager.h"
#include "CatalogManager.h"
#include "RecordManager.h"
#include "IndexManager.h"
#include "Interpreter.h"
#include "API.h"

#define CRTDBG_MAP_ALLOC  

/**
 *  This is the test program for record manager.
*/
int main(void) {
    using namespace std;
    CatalogManager* myCatalogManager = new CatalogManager();
    BufferManager* myBufferManager = new BufferManager(myCatalogManager);
    RecordManager* myRecordManager = new RecordManager(myBufferManager, myCatalogManager);
    IndexManager* myIndexManager = new IndexManager(myBufferManager, nullptr);
    API* myAPI = new API(myCatalogManager, myBufferManager, myRecordManager, myIndexManager);
    myIndexManager->SetAPI(myAPI);
    myCatalogManager->SetAPI(myAPI);
    myRecordManager->SetAPI(myAPI);
    Interpreter* myInterpreter = new Interpreter(myCatalogManager, myAPI);

    string Name = "student";
    string* attr = new string[3]{ "sid", "sno", "sage" };
    int attrNum = 3;
    unsigned char info[5][32] = {
        {3, 1, 1},
        {10, 0, 0},
        {1, 0, 0},
        {1, 0, 0},
        {0, 0, 0}
    };
    unsigned char** attrInfo = new unsigned char* [5];
    for (int i = 0; i < 5; i++) {
        attrInfo[i] = new unsigned char[32];
        memset(attrInfo[i], 0, 32);
        for (int j = 0; j < 3; j++) {
            attrInfo[i][j] = info[i][j];
        }
    }
    tableInfoNode* infoNode = new tableInfoNode(Name, attr, attrNum, attrInfo);
    //myCatalogManager->CreateTableCatalog(infoNode);

    string Name2 = "student2";
    string* attr2 = new string[4]{ "sid2", "sno2", "sage2", "sgender" };
    int attrNum2 = 4;
    unsigned char info2[5][32] = {
        {3, 1, 1, 3},
        {10, 0, 0, 1},
        {1, 0, 0, 0},
        {1, 0, 0, 0},
        {1, 0, 0, 0}
    };
    unsigned char** attrInfo2 = new unsigned char* [5];
    for (int i = 0; i < 5; i++) {
        attrInfo2[i] = new unsigned char[32];
        memset(attrInfo2[i], 0, 32);
        for (int j = 0; j < 4; j++) {
            attrInfo2[i][j] = info2[i][j];
        }
    }
    tableInfoNode* infoNode2 = new tableInfoNode(Name2, attr2, attrNum2, attrInfo2);
    //myCatalogManager->CreateTableCatalog(infoNode2);

//#define CATALOG_MANAGER_TEST
#ifdef CATALOG_MANAGER_TEST
    //Catalog Manager测试建表和表的删除

    //Catalog Manager测试获得表的数据
    infoNode = nullptr;
    infoNode = myCatalogManager->GetTableInfo(Name);
    cout << "tableName: " << infoNode->tableName << endl;
    cout << "attrName[0]: " << infoNode->attrName[0] << endl;
    cout << "attrNum: " << infoNode->attrNum << endl;
    
    for (int i = 0; i < 5; i++) {
        for (int j = 0; j < attrNum; j++) {
            cout << "attrInfo[" << i << "][" << j << "]: " << (int)infoNode->attrInfo[i][j] << " ";
        }
        cout << endl;
    }

    cout << "recordSize: " << infoNode->recordSize << endl;
    
    attrNum = 0;
    attrNum = myCatalogManager->GetAttrNum(Name);
    cout << "attrNum gotten: " << attrNum << endl;

    //CatalogManager 测试create Index, drop index
    myCatalogManager->CreateIndex(Name, "sno", "sno_index");
    for (int i = 0; i < 5; i++) {
        for (int j = 0; j < attrNum; j++) {
            cout << "attrInfo[" << i << "][" << j << "]: " << (int)infoNode->attrInfo[i][j] << " ";
        }
        cout << endl;
    }
    cout << endl;
    if (myCatalogManager->hasIndex(Name, "sno") == 1) {
        cout << "has index sno_index" << endl;
    }
    cout << endl;
    myCatalogManager->DeleteIndex(Name, "sno_index");
    for (int i = 0; i < 5; i++) {
        for (int j = 0; j < attrNum; j++) {
            cout << "attrInfo[" << i << "][" << j << "]: " << (int)infoNode->attrInfo[i][j] << " ";
        }
        cout << endl;
    }

    //测试 GetRecordSize, GetAttrType
    int size = myCatalogManager->GetRecordSize(Name);
    cout << "record size = " << size << endl;
    int type = myCatalogManager->GetAttrType(Name, "sid");
    cout << "attribute type = " << type << endl;
#endif

#define RECORD_MANAGER_TEST
#ifdef RECORD_MANAGER_TEST
    //测试create table, drop table
    //myRecordManager->CreateTable(Name);
    //myRecordManager->CreateTable(Name2);

    //测试insertProcess
    string* attrVal  = new string[3]{ "一二三四56七八九十", "1", "3" };
    string* attrVal2 = new string[3]{ "0000000002", "2", "4" };
    string* attrVal3 = new string[3]{ "0000000003", "4", "5" };

    //myRecordManager->InsertProcess(3, attrVal, Name);
    //myRecordManager->InsertProcess(3, attrVal2, Name);
    //myRecordManager->InsertProcess(3, attrVal3, Name);

    string* attrVal4 = new string[4]{ "学号暂时还没有想好呢", "1", "4", "m" };
    string* attrVal5 = new string[4]{ "学号仍然还没有想好呢", "4", "5", "f" };
    string* attrVal6 = new string[4]{ "学号竟然还没有想好呢", "8", "10", "m" };

    //myRecordManager->InsertProcess(4, attrVal4, Name2);
    //myRecordManager->InsertProcess(4, attrVal5, Name2);
    //myRecordManager->InsertProcess(4, attrVal6, Name2);
    //测试全表删除
    //myRecordManager->DeleteProcess(Name, 0, nullptr, true);
    //测试部分删除
    string* restrict1 = new string[3]{ "sid", "EQUALTO", "0000000002" };
    //myRecordManager->DeleteProcess(Name, 1, restrict1, false);
    //测试多个条件删除
    string* restrict2 = new string[6]{ "sid", "EQUALTO", "0000000001", "sid", "EQUALTO", "0000000002" };
    //myRecordManager->DeleteProcess(Name, 2, restrict2, false);

    //测试全部选择
    //myRecordManager->SelectProcess(3, attr, Name, 0, nullptr, true, true);
    //测试条件选择
    //myRecordManager->SelectProcess(3, attr, Name, 1, restrict1, true, false);
    //myRecordManager->SelectProcess(3, attr, Name, 2, restrict2, true, false);
    //测试部分attribute选择
    string* attr3 = new string[2]{ "sno", "sid" };
    //myRecordManager->SelectProcess(2, attr3, Name, 1, restrict1, true, false);
    
    //测试多表查询(两张相同的表，全部选择）
    string* tableName = new string[2]{ "student", "student" };
    string* attr4 = new string[6]{ "sid", "sno", "sage", "sid", "sno", "sage" };
    //myRecordManager->SelectProcess(6, attr4, 2, tableName, 0, nullptr, true, true);
    //测试多表查询(两张不相同的表，全部选择)
    tableName[1] = "student2";
    string* attr5 = new string[7]{ "sid", "sno", "sage", "sid2", "sno2", "sage2", "sgender" };
    //myRecordManager->SelectProcess(7, attr5, 2, tableName, 0, nullptr, true, true);
    //测试多表查询(两张不相同的表，部分选择)
    string* attr6 = new string[2]{ "sid", "sid2" };
    //myRecordManager->SelectProcess(2, attr6, 2, tableName, 1, restrict1, true, false);

    //需要上层增加工作量，检查条件是否模糊。因为我们这里不想加上重新命名的功能（那样上层工作量更更更大，下层也是）

    //myRecordManager->DropTable(Name);
    //myCatalogManager->DropTableCatalog(Name);
    //myRecordManager->DropTable(Name2);
    //myCatalogManager->DropTableCatalog(Name2);

    //测试加入上层模块
    _CrtDumpMemoryLeaks();
    myInterpreter->InterpretCommand();

#endif
    delete myInterpreter;
    delete myAPI;
    delete myIndexManager;
    delete myRecordManager;
    delete myBufferManager;
    delete myCatalogManager;
   
    return 0;
}