#include <iostream>
#include "BufferManager.h"
#include "RecordManager.h"
#include "Interpreter.h"
using namespace std;

/**
 *  This is the test program for record manager.
*/
int main(void) {
    FILE* fp;

    CatalogManager* myCatalogManager = new CatalogManager();
    BufferManager* myBufferManager = new BufferManager(myCatalogManager);
    RecordManager* myRecordManager = new RecordManager(myBufferManager);
    API* myAPI = new API(myCatalogManager, myBufferManager, myRecordManager);
    Interpreter* myInterpreter = new Interpreter(myCatalogManager, myAPI);

    char* buffer = new char [4];
    //char* result = nullptr;
    int size = 0;

    memset(buffer, 0, 4);

    /*--- BufferManager::ReadFile test ---
    result = myBufferManager.ReadFile("test", &size, 0);
    for (int i = 0; i < size; i++) {
        cout << result[i];
    }
    */

    /*--- BufferManager::WriteFile test ---
    for (int i = 0; i < 10; i++) {
        buffer[i] = i + '0';
    }

    blockNode* BN = new blockNode();
    BN->offset = BLOCK_SIZE;
    BN->recordAddress = buffer;
    BN->size = 10; 

    myBufferManager.WriteFile("test", BN);

    delete BN;
    */

    /*--- BufferManager::GetBlock test ---
    for (int i = 0; i < MAX_BLOCK_NUM; i++) {
        myBufferManager.GetBlock("test", 0);
    }
    myBufferManager.GetBlock("test", 0);
    myBufferManager.GetBlock("test", 4097);
    */

    /*--- BufferManager::Record table create&drop, index create&drop ---
    myRecordManager.DropTable("test2");
    myRecordManager.DropIndex("index1");
    */

    /*--- Interpreter::InterpretCommand test ---*/

    myInterpreter->InterpretCommand();

    delete myInterpreter;
    delete myAPI;
    delete myBufferManager;
    delete myCatalogManager;
    delete myRecordManager;
   
    return 0;
}