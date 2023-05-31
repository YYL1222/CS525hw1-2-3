#include<stdio.h>
#include<stdlib.h>
#include <string.h>

# include "record_mgr.h"
# include "storage_mgr.h"
# include "buffer_mgr.h"

#include "dberror.h"
#include "expr.h"
#include "tables.h"

#define NUM_PAGES 100

int numTuples=0;
SM_FileHandle fh;


// table and manager
//Written by Yi Yi
extern RC initRecordManager (void *mgmtData){
    //initialize the record manager
    return RC_OK;
}
//Written by Yi Yi
extern RC shutdownRecordManager (){
    //shout down record manager and buffer pool
    return RC_OK;
}

/*
	The file containing table data consists of (in the following order):
		Integer for number of tuples
		Integer for number of free pages
		Integer for number of data pages (number of free pages + number of data pages = NUM_PAGES)
		Integer Array containing page numbers of free pages (array length = number of free pages)
		Integer Array containing page numbers of data pages (array length = number of data pages)
		Integer for number of attributes in schema
		Integer Array containing lengths of attribute names in schema (array length = number of attributes)
		Character Array containing all attribute names in schema back to back (array length = sum of attribute name lengths)
		DataType Array containing data types for attributes in schema (array length = number of attributes)
		Integer Array containing lengths of attribute types (0 for non-String, max length of String for String) (array length = number of attributes)
		Integer for number of key attributes (keysize)
		Integer Array containing positions of key attributes (array length = number of key attributes)

	The page file for the buffer manager is the name of the table followed by ".pf"
*/

// helper functions
// Written by Francis

//Written by Yi Yi
//Updated by Francis
extern RC createTable (char *name, Schema *schema){
    //create file
   // printf("start createTable-----------\n");
    int i;
    numTuples=0; //reset the number of tuple
    char *data=(char*) malloc(PAGE_SIZE);
    SM_FileHandle fh;
    if(openPageFile(name,&fh) != RC_OK){
        createPageFile(name);
    }
    if(openPageFile(name,&fh) != RC_OK){
        return RC_FILE_NOT_FOUND;
    }
    //store schema info in data
    sprintf(data, "%s,", name);
    sprintf(data+ strlen(data), "%d,", schema->numAttr);

    for(i=0;i<schema->numAttr;i++){
        sprintf(data+ strlen(data), "%s,",schema->attrNames[i]);
    }
    for(i=0;i<schema->numAttr;i++){
        sprintf(data+ strlen(data), "%d,",schema->dataTypes[i]);
    }
    for(i=0;i<schema->numAttr;i++){
        sprintf(data+ strlen(data), "%d,",schema->typeLength[i]);
    }

    sprintf(data+ strlen(data), "%d,", schema->keySize);
    for (i = 0; i < schema->keySize; i++) {
        sprintf(data+ strlen(data), "%d,", schema->keyAttrs[i]);
    }
    writeBlock(0, &fh, data);
    appendEmptyBlock(&fh); //page 0 store table information, so append page 1 to file
    //closePageFile (&fh);
    return RC_OK;
}

//Written by Yi Yi
//Updated by Francis
extern RC openTable (RM_TableData *rel, char *name){
   // printf("open table-------\n");
    rel->bm = MAKE_POOL();
    //initialize a buffer pool
    initBufferPool(rel->bm, name, NUM_PAGES, RS_FIFO, NULL);
    //initialize a page handle
    BM_PageHandle *ph = MAKE_PAGE_HANDLE();
    pinPage(rel->bm, ph, 0);

    //initailize schema
    Schema* schema = (Schema*) malloc(sizeof(Schema));
    schema->numAttr = 0;
    schema->attrNames = (char**) malloc(sizeof(char*));
    schema->dataTypes = (DataType*) malloc(sizeof(DataType));
    schema->typeLength = (int*) malloc(sizeof(int));
    schema->keyAttrs = (int*) malloc(sizeof(int));
   // char *tem = NULL;
    char *str = strtok(ph->data, ",");
    //get the schema in from data
    rel->name = str;
    str = strtok(NULL, ",");
    schema->numAttr = atoi(str);
    int i;
    for(i=0;i<schema->numAttr;i++){
        str = strtok(NULL, ",");
        schema->attrNames[i]=(char *) malloc(sizeof (char));
        schema->attrNames[i] = str;
    }
    for(i=0;i<schema->numAttr;i++){
        str = strtok(NULL, ",");
        schema->dataTypes[i] = atoi(str);
    }
    for(i=0;i<schema->numAttr;i++){
        str = strtok(NULL, ",");
        //schema->typeLength[i]=(int *) malloc(sizeof (int));
        schema->typeLength[i] = atoi(str);
       // printf("%d-----type\n",schema->typeLength[i]);
    }
    str = strtok(NULL, ",");
    schema->keySize = atoi(str);
    for(i=0;i<schema->keySize;i++){
        str = strtok(NULL, ",");
        schema->keyAttrs[i] =atoi(str) ;
    }
    //transfer char data to char and int
    //getSchemaInfo(data,schema);
    //int size = getRecordSize(schema);
    rel->numTuples=numTuples;
    rel->numPage=1;
    rel->numSlot=0;
    rel->deletepage=-1;
    rel->deleteslot=-1;
    //readPageNums(rel->freePages,size); // pageNums for free pages
    //printf("check free and data page length  %d   %d ----------\n",rel->freePagesLength,rel->dataPagesLength);
    unpinPage(rel->bm,ph);
    rel->schema = schema;
    free(ph);
    return RC_OK;

}

//Written by Yi Yi
//Updated by Francis
extern RC closeTable (RM_TableData *rel){

    shutdownBufferPool(rel->bm);
    free(rel->bm);
    return RC_OK;
}

//Written by Yi Yi
//Updated by Francis
extern RC deleteTable (char *name){
    // remove table file and page file
    if (destroyPageFile(name) != RC_OK)
        return RC_ERROR;
   // free(pageName);

    return RC_OK;
}

//Written by Yi Yi
extern int getNumTuples (RM_TableData *rel){
    return rel->numTuples;
}

/*
	Page headers consist of:
		Number of records in page (length of the slot array)

	The header is followed by a slot array (each slot is a pointer to a record).
		The slot array grows downards from the header.

	At the end of the page are the records.
		The records grow upwards from the end of the page.

	The order of the slot array is always in the reversed order of the records.
		This is because slots are appended to the end of the slot array, and records are prepended to the start of the records in the page.
*/

// Helper functions for parsing pages
// Written by Francis
/*
int getNumRecords(char *pageData){
    //printf("getNumRecords\n");
    return *((int *) pageData);
}

void setNumRecords(char *pageData, int numRecords){
    //printf("setNumRecords\n");
    *((int *) pageData) = numRecords;
}

int *getSlotArray(char *pageData){
    //printf("getSlotArray\n");
    return (int *) (pageData + sizeof(int));
}

int *getSlot(char *pageData, int slot){
    //printf("getSlot\n");
    return getSlotArray(pageData) + slot;
}
char *getRecordAtSlot(char *pageData, int slot){
        //printf("getRecordAtSlot\n");
    return pageData + *(getSlot(pageData, slot));
}

int *getEndOfSlotArray(char *pageData){
    //printf("getEndOfSlotArray\n");
    return getSlotArray(pageData) + getNumRecords(pageData);
}

char *getStartOfRecords(char *pageData){
    //printf("getStartOfRecords\n");
    int numRecords = getNumRecords(pageData);
    if (numRecords == 0){
        return pageData + PAGE_SIZE;
    }
    else
        return pageData + *(getSlot(pageData, numRecords - 1)); //location of first record (always in the last slot)
}



bool cannotFitInPage(int recordSize, char *pageData){
    //printf("cannotFitInPage\n");
    char *startOfRecords = getStartOfRecords(pageData);
    char *endOfSlotArray = (char *) (getEndOfSlotArray(pageData) + 1); //add 1 to account for new record

    return recordSize > startOfRecords - endOfSlotArray;
}
*/

// Written by Francis
// Assignment details specify that records for a given schema are fixed-length.
// This function will work for schemas where records can have variable lengths,
// 		but may mark the page full when it could still fit smaller records.
//Updated by Yi Yi
extern RC insertRecord (RM_TableData *rel, Record *record){
   // printf("start insertRecord----------------\n");
    RC rc;
    BM_BufferPool *bm = rel->bm;
    BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
    int recordSize = getRecordSize(rel->schema);
   //pin current page and check capacity
    int cur = rel->numPage;
    pinPage(bm,page,cur);
    //printf("number of record in this page %d----\n",numOfRecord);
    int pagesize = rel->numSlot * recordSize;
  //  printf("size in this page-----%d----\n",pagesize);
    //int count=0;
    while(PAGE_SIZE - pagesize < recordSize){ //not enough -> go to next page
        unpinPage(bm,page);
        cur++;
        if(cur > rel->numPage){
            appendEmptyBlock(&fh);
            rel->numSlot = 0;
            rel->numPage++;
            pinPage(bm,page,cur);
        }

        //rel->numPage++;
        //numOfRecord = getNumRecords(page->data);
        pagesize = rel->numSlot * recordSize;
        //printf("%d pagesize--------------\n",pagesize);
        if(PAGE_SIZE - pagesize < recordSize)
            unpinPage(bm,page);
    }
    int curSlots = rel->numSlot;
    int add = (curSlots)*(recordSize);
    //printf("current slot ------------------%d\n",curSlots);

    record->id.slot = curSlots;
    record->id.page=rel->numPage;
    //sprintf(curData,"%s",record->data+4);
    //printf("!!!!!!!!!!!!!!!!!!!!record data %s\n",curData);
    memcpy(page->data+add, record->data, recordSize);
   // sprintf(page->data+add,"%s",curData);
    numTuples++;
    rel->numTuples++;
    rel->numSlot ++;

    // Mark dirty and unpin page
    if ((rc = markDirty(bm, page)) != RC_OK)
        return rc;
    if ((rc = forcePage(bm, page)) != RC_OK)
        return rc;
    if ((rc = unpinPage(bm, page)) != RC_OK)
        return rc;
    free(page);

    return RC_OK;
}

// Written by Francis
//Updated by Yi Yi
extern RC deleteRecord (RM_TableData *rel, RID id){
   // printf("start deleteRecord----------------\n");
    RC rc;
    BM_BufferPool *bm = rel->bm;
    BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
    int pageNum = id.page;
    int slot = id.slot;
    int recordsize = getRecordSize(rel->schema);
    int add = slot * recordsize;

    // Get Page
    if ((rc = pinPage(bm, page, pageNum)) != RC_OK)
        return rc;

    char *mark;
    mark = page->data+add;
    memset(mark, '\0', recordsize);
    rel->deletepage = id.page;
    rel->deleteslot = id.slot;
    // Mark dirty and unpin page
    if ((rc = markDirty(bm, page)) != RC_OK)
        return rc;
    if ((rc = forcePage(bm, page)) != RC_OK)
        return rc;
    if ((rc = unpinPage(bm, page)) != RC_OK)
        return rc;

    free(page);
    // Update table metadata
    rel->numTuples--;
    numTuples--;

    return RC_OK;
}

// Written by Francis
/*
Assignment details specify that records for a given schema are fixed-length.
This function will NOT work for schemas where records can have variable lengths,
	because this function just replaces the old data with the new data.
This could cause data from other records to be overwritten or an invalid memory access.
*/
//Updated by Yi Yi
extern RC updateRecord (RM_TableData *rel, Record *record){
    //printf("start updateRecord----------------\n");
    RC rc;
    BM_BufferPool *bm = rel->bm;
    BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));

    // Get Page
    if ((rc = pinPage(bm, page, record->id.page)) != RC_OK)
        return rc;
    int recordsize = getRecordSize(rel->schema);
    int slot = record->id.slot;
    int add = slot*recordsize;
    char *pageData = page->data;
    //point to the slot of record
    pageData += add;
    memcpy(pageData, record->data, recordsize);
    // Mark dirty and unpin page
    if ((rc = markDirty(bm, page)) != RC_OK)
        return rc;
    if ((rc = forcePage(bm, page)) != RC_OK)
        return rc;
    if ((rc = unpinPage(bm, page)) != RC_OK)
        return rc;
    free(page);

    return RC_OK;
}

// Written by Francis
extern RC getRecord (RM_TableData *rel, RID id, Record *record){
   // printf("start getRecord----------------\n");
   // printf("RID page: %d\n", id.page);
    //printf("RID slot: %d\n", id.slot);
    RC rc;
    BM_BufferPool *bm = rel->bm;
    BM_PageHandle *page = (BM_PageHandle *) malloc(sizeof(BM_PageHandle));
    int size= getRecordSize(rel->schema);
    int slot = id.slot;
    int add = size*slot;
    //check this record exist
    if(id.page == rel->deletepage && id.slot == rel->deleteslot){
        return RC_ERROR;
    }
    // Get Page
    if ((rc = pinPage(bm, page, id.page)) != RC_OK)
        return rc;
    // Update record
    memcpy(record->data,page->data+add, size);
   // printf("%s---------getrecord\n",page->data+add);
   // record->data = page->data;
    record->id.page = id.page;
    record->id.slot = id.slot;

    // Unpin page
    if ((rc = unpinPage(bm, page)) != RC_OK)
        return rc;
    free(page);
    //free(mark);
    return RC_OK;
}

// scans
//Written by Yi Yi
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
    //printf("startScan----------------\n");
    scan->rel=rel;
    scan->cond=cond;
    scan->scanSum=0;
    scan->curpage=1;

    return RC_OK;
}
//Written by Yi Yi
extern RC next (RM_ScanHandle *scan, Record *record){
    //printf("startScan next----------------\n");
    RM_TableData *rel = scan->rel;
    BM_BufferPool *bm = rel->bm;
    BM_PageHandle  *ph = (BM_PageHandle*) malloc(sizeof (BM_PageHandle));
    int recordsize = getRecordSize(rel->schema);

    int i;
    Value *val; //store result
    record->id.page = scan->curpage; //from page 1

    while(scan->scanSum < rel->numTuples){
        pinPage(bm,ph,record->id.page);

        for(i = scan->scanSum; i < rel->numTuples; i++){
            //printf("totle tuple %d       now scan tuple %d\n",rel->numTuples,scan->scanSum);

            record->id.slot = scan->scanSum;
            int add = record->id.slot * recordsize;
            memcpy(record->data, ph->data+add, recordsize);

            if (evalExpr(record, scan->rel->schema, scan->cond, &val) != RC_OK)
                return RC_ERROR;

            scan->scanSum++;
            if(val->v.boolV){ //it matches
              //printf("scan TRUE__________________________________\n");
                unpinPage(bm,ph);
                free(val);
                free(ph);
                return RC_OK;
            }
        }
        //printf("to next page--------------\n");
        unpinPage(bm,ph);
        record->id.page++;
        scan->curpage++;
    }

    free(val);
    free(ph);
    return RC_RM_NO_MORE_TUPLES;
}
//written by Yi Yi
extern RC closeScan (RM_ScanHandle *scan){
    //free(scan->rel);
    //free(scan);
    return RC_OK;
}

// Written by Chris
// Dealing with schemas
// These helper functions are used to return the size in bytes of records for a given schema
// and create/free schema
extern int getRecordSize (Schema *schema){
    int recordSize = 0;
    //printf("getRecordSize----------------\n");
    //adding record size based on different DataType
    int i;
    for (i=0; i < schema->numAttr; i++){

        if (schema->dataTypes[i] == DT_INT){
            recordSize += sizeof(int);
        }

        if (schema->dataTypes[i] == DT_STRING){
            recordSize += schema->typeLength[i];
        }

        if (schema->dataTypes[i] == DT_FLOAT){
            recordSize += sizeof(float);
        }

        if (schema->dataTypes[i] == DT_BOOL){
            recordSize += sizeof(bool);
        }

    }
    //printf("finish      getRecordSize----------------\n");
    return recordSize;
}

// Written by Chris
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
    Schema* schema=(Schema*)malloc(sizeof(Schema));
   // printf("createSchema----------------\n");
    //setting new schema information based on input
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keySize = keySize;
    schema->keyAttrs = keys;

    return schema;
}

// Written by Chris
extern RC freeSchema (Schema *schema){
   // printf("start free schema-------\n");
    free(schema->attrNames);
    //  printf("start free schema attrNames-------\n");
    free(schema->dataTypes);
    //  printf("start free schema dataTypes-------\n");
    free(schema->typeLength);
    //printf("start free schema typeLength-------\n");
    free(schema->keyAttrs);
    //free(schema);
    //printf("finish free schema-------\n");
    return RC_OK;
}

// Written by Chris
// Dealing with records and attribute values
// These functions are used to get or set the attribute values of a record and create a new record for a given schema.
// Creating a new record should allocate enough memory to the data field to hold the binary
// representations for all attributes of this record as determined by the schema
extern RC createRecord (Record **record, Schema *schema){
    //printf("createRecord----------------\n");
    // Give record1 and its data a size and starting position
    Record *record1 = (Record *) malloc(sizeof(Record));
    int recordSize = getRecordSize(schema);
    record1->data= (char*) malloc(recordSize);
    record1->id.page = -1;
    record1->id.slot = -1;

    *record = record1;

    return RC_OK;
}

// Written by Chris
extern RC freeRecord (Record *record){
   // printf("freeRecord----------------\n");
    free(record->data);
    free(record);

    return RC_OK;
}

// Written by Chris
// Updated by Yi Yi
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    int adjust = 0;
   // printf("getAttr----------------\n");
    Value *value1 = (Value*) malloc(sizeof(Value));
    int i;
    //Adjusting based on dataTypes
    for (i=0; i < attrNum; i++){

        if (schema->dataTypes[i] == DT_INT){
            adjust += sizeof(int);
        }

        if (schema->dataTypes[i] == DT_STRING){
            adjust += schema->typeLength[i];
        }

        if (schema->dataTypes[i] == DT_FLOAT){
            adjust += sizeof(float);
        }

        if (schema->dataTypes[i] == DT_BOOL){
            adjust += sizeof(bool);
        }

    }

    //Adjusting position
    char *position = record -> data;
    position += adjust;

    //Getting attributes from each dataTypes
    if (schema->dataTypes[attrNum] == DT_INT){
        //newChar = (char *)calloc(sizeof(int), sizeof(char));

        /*
        int newInt;

        strcpy(newInt, *record->data + position);

        value1->v.intV = newInt;
        */
        value1->v.intV = *((int *) position); // modified from above
        value1->dt = DT_INT;
        //printf("value1->v.intV  %d\n",value1->v.intV);
    }

    if (schema->dataTypes[attrNum] == DT_STRING){
        value1->v.stringV = (char *)calloc(schema->typeLength[attrNum]+1, sizeof(char));

        strncpy(value1->v.stringV, position,schema->typeLength[attrNum] );

        value1->v.stringV[schema->typeLength[attrNum]] = '\0';
        value1->dt = DT_STRING;

    }

    if (schema->dataTypes[attrNum] == DT_FLOAT){
        //newChar = (char *)calloc(sizeof(float), sizeof(char));
        /*
        float newFloat;

        strcpy(&newFloat, *record->data + position);

        value1->v.floatV = newFloat;
        */
        value1->v.floatV = *((float *) position); // modified from above
        value1->dt = DT_FLOAT;
    }

    if (schema->dataTypes[attrNum] == DT_BOOL){
        //newChar = (char *)calloc(sizeof(bool), sizeof(char));
        /*
        bool newBool;

        strcpy(newBool, *record->data + position);

        value1->v.boolV = newBool;
        */
        value1->v.boolV = * ((bool *) position);
        value1->dt = DT_BOOL;
    }

    //setting value
    *value = value1;
    return RC_OK;
}

// Written by Chris
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
    int adjust = 0;
    //printf("setAttr----------------\n");
    //Adjusting based on dataTypes
    int i;
    for (i=0; i < attrNum; i++){

        if (schema->dataTypes[i] == DT_INT){
            adjust += sizeof(int);
        }

        if (schema->dataTypes[i] == DT_STRING){
            adjust += schema->typeLength[i];
        }

        if (schema->dataTypes[i] == DT_FLOAT){
            adjust += sizeof(float);
        }

        if (schema->dataTypes[i] == DT_BOOL){
            adjust += sizeof(bool);
        }

    }
    //Adjusting position
    char *position = record -> data;
    position += adjust;
    //printf("%d position\n",adjust);
    //Setting attributes according to each dataTypes
    if (schema->dataTypes[attrNum] == DT_INT){
        int newInt = value->v.intV;

        memcpy(position,&newInt,sizeof(int));
    }

    if (schema->dataTypes[attrNum] == DT_STRING){
        char *newChar = (char *)calloc(schema->typeLength[attrNum], sizeof(char));
        newChar = value->v.stringV;

        memcpy(position, newChar, schema->typeLength[attrNum]);
    }

    if (schema->dataTypes[attrNum] == DT_FLOAT){
        float newFloat = value->v.floatV;
        memcpy(position,&newFloat,sizeof(float));
    }

    if (schema->dataTypes[attrNum] == DT_BOOL){
        bool newBool = value->v.boolV;

        memcpy(position,&newBool,sizeof(bool));
    }

   // printf("finish set Attribute--------%s\n",record->data+adjust);
    return RC_OK;
}
