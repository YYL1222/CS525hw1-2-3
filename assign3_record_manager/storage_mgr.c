#include <stdlib.h>
#include <stdio.h>
#include "dberror.h"
#include "storage_mgr.h"

int page_record;

extern void initStorageManager(void) {
    return;
}

extern RC createPageFile(char *fileName) {
    FILE *file = fopen(fileName, "w+");
    if (file == NULL) {
        return RC_FILE_NOT_FOUND;
    }
    // record page number
    char *cur = (char*) malloc(PAGE_SIZE);
    page_record = 1;
    fwrite(cur, sizeof(PAGE_SIZE), 1, file);
    // assign a new page
    char *page = (char *) calloc(PAGE_SIZE, sizeof(char));
    fwrite(page, sizeof(char), PAGE_SIZE, file);
    free(page);
    free(cur);
    fclose(file);
    return RC_OK;
}

extern RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    // check file exist

    FILE *file = fopen(fileName, "r+");
    if (file == NULL) {
        return RC_FILE_NOT_FOUND;
    }
    // initialize
    fHandle->fileName = fileName;
    fHandle->curPagePos = 0;
    // go to first page to read
    char *cur = (char*) malloc(PAGE_SIZE);
    fread(cur, sizeof(PAGE_SIZE), 1, file);
    fHandle->totalNumPages = page_record;
    fHandle->mgmtInfo = file;
    free(cur);
    return RC_OK;
}

extern RC closePageFile(SM_FileHandle *fHandle) {
    fclose(fHandle->mgmtInfo);
    return  RC_OK;
}

extern RC destroyPageFile(char *fileName) {
    remove(fileName);
    return RC_OK;
}

extern RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // check pageNum is valid
    //printf("reading block_____\n");
    FILE *file = fHandle->mgmtInfo;
    if (file == NULL) {
        return RC_FILE_NOT_FOUND;
    }
    // point to pageNum position and read
    int size = sizeof(PAGE_SIZE) + pageNum * PAGE_SIZE * sizeof(char);
    fseek(file, size , SEEK_SET);
    fread(memPage, sizeof(char), PAGE_SIZE, file);
    fHandle->curPagePos = pageNum;
    return RC_OK;
}

extern int getBlockPos(SM_FileHandle *fHandle) {
    return fHandle->curPagePos;
}
extern RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if(RC_OK == readBlock(0, fHandle, memPage))
		return RC_OK;
    else
		return RC_READ_NON_EXISTING_PAGE;
}

extern RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if(RC_OK == readBlock(fHandle->curPagePos - 1, fHandle, memPage))
		return RC_OK;
    else
		return RC_READ_NON_EXISTING_PAGE;
}

extern RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if(RC_OK == readBlock(fHandle->curPagePos, fHandle, memPage))
		return RC_OK;
    else
		return RC_READ_NON_EXISTING_PAGE;
}

extern RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if(RC_OK == readBlock(fHandle->curPagePos + 1, fHandle, memPage))
		return RC_OK;
    else
		return RC_READ_NON_EXISTING_PAGE;
}

extern RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if(RC_OK == readBlock(fHandle->totalNumPages - 1, fHandle, memPage))
		return RC_OK;
    else
		return RC_READ_NON_EXISTING_PAGE;
}

/* writing blocks to a page file */
extern RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    //printf("writing block-------\n");
    FILE *file = fHandle->mgmtInfo;
    if (file == NULL) {
        return RC_FILE_NOT_FOUND;
    }
    // make sure file has next page
    //ensureCapacity(pageNum + 1, fHandle);
    int size = sizeof(PAGE_SIZE) + pageNum * PAGE_SIZE * sizeof(char);
    fseek(file, size, SEEK_SET);
    fHandle->curPagePos = pageNum;
    fwrite(memPage, sizeof(char), PAGE_SIZE, file);
    return RC_OK;
}

extern RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    //int size;
    int curpos=fHandle->curPagePos;
    return writeBlock(curpos, fHandle, memPage);
}

extern RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    int curpage = fHandle->totalNumPages;
    //check page is enough
    if (numberOfPages > curpage) {
        appendEmptyBlock(fHandle);
    }
    return RC_OK;
}

extern RC appendEmptyBlock(SM_FileHandle *fHandle) {
    FILE *file = fHandle->mgmtInfo;
    if (file == NULL) {
        return RC_FILE_NOT_FOUND;
    }
    char *page = (char *) calloc(PAGE_SIZE, sizeof(char));
    fseek(file, 0, SEEK_END);
    fwrite(page, sizeof(char), PAGE_SIZE, file);
    free(page);
   
    // record number of page
    char *cur = (char*) malloc(PAGE_SIZE);
    page_record = fHandle->totalNumPages;
    fseek(file, 0, SEEK_SET);
    fwrite(cur, sizeof(PAGE_SIZE), 1, file);
    free(cur);
    // add one page
    fHandle->totalNumPages +=1;
    return RC_OK;
}