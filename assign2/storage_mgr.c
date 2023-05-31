#include <stdlib.h>
#include <stdio.h>
#include "dberror.h"
#include "storage_mgr.h"

struct info {
    int page;
};


extern void initStorageManager(void) {
    return;
}

extern RC createPageFile(char *fileName) {
    // create file and check file pointer
    FILE *file = fopen(fileName, "w+");
    if (file == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    // record page number
    struct info *cur = (struct info *) calloc(1, sizeof(struct info));
    cur->page = 1;
    fwrite(cur, sizeof(struct info), 1, file);
    free(cur);

    // assign a new page
    char *page = (char *) calloc(PAGE_SIZE, sizeof(char));
    fwrite(page, sizeof(char), PAGE_SIZE, file);

    
    free(page);
    fclose(file);

    return RC_OK;
}

extern RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    // check file exist
    FILE *file = fopen(fileName, "r+");
    if (file == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    // go to first page to read
    struct info *cur = (struct info *) calloc(1, sizeof(struct info));
    fread(cur, sizeof(struct info), 1, file);

    // initialize 
    fHandle->fileName = fileName;
    fHandle->totalNumPages = cur->page;
    fHandle->curPagePos = 0;
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
    int size;
    // check pageNum is valid
    if (pageNum >= fHandle->totalNumPages) {
        return RC_READ_NON_EXISTING_PAGE;
    }
    //check file exist
    FILE *file = fHandle->mgmtInfo;
    if (file == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    // point to pageNum position and read
    size = sizeof(struct info) + pageNum * PAGE_SIZE * sizeof(char);
    fseek(file, size , SEEK_SET);
    fHandle->curPagePos = pageNum;
    fread(memPage, sizeof(char), PAGE_SIZE, file);

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
    int size;
    FILE *file = fHandle->mgmtInfo;
    if (file == NULL) {
        return RC_FILE_NOT_FOUND;
    }

    // make sure file has next page
    ensureCapacity(pageNum + 1, fHandle);

    size = sizeof(struct info) + pageNum * PAGE_SIZE * sizeof(char);
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
    while (numberOfPages > fHandle->totalNumPages) {
        int rc = appendEmptyBlock(fHandle);
        if (rc != RC_OK) {
            return rc;
        }
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
    struct info *cur = (struct info *) calloc(1, sizeof(struct info));
    cur->page = fHandle->totalNumPages;
    fseek(file, 0, SEEK_SET);
    fwrite(cur, sizeof(struct info), 1, file);
    free(cur);
    // add one page
    fHandle->totalNumPages +=1;
    return RC_OK;
}
