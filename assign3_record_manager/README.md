# README #


### Function description ###
##### table and manager
* initRecordManager (void *mgmtData);
* shutdownRecordManager ();
* createTable (char *name, Schema *schema);
* openTable (RM_TableData *rel, char *name);
* closeTable (RM_TableData *rel);
* deleteTable (char *name);
* getNumTuples (RM_TableData *rel);
##### handling records in a table
* insertRecord (RM_TableData *rel, Record *record);
* deleteRecord (RM_TableData *rel, RID id);
* updateRecord (RM_TableData *rel, Record *record);
* RC getRecord (RM_TableData *rel, RID id, Record *record);
##### scans
* startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
* next (RM_ScanHandle *scan, Record *record);
* closeScan (RM_ScanHandle *scan);
##### dealing with schemas
* int getRecordSize (Schema *schema);
* Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int*keys);
* freeSchema (Schema *schema);
##### dealing with records and attribute values
* createRecord (Record **record, Schema *schema);
* freeRecord (Record *record);
* getAttr (Record *record, Schema *schema, int attrNum, Value **value);
* setAttr (Record *record, Schema *schema, int attrNum, Value *value);

### Run the code ###

    $ make
	$ ./test_3
	