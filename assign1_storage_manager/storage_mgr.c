#include "dberror.h"
#include "storage_mgr.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

// Constants used to ensure that a file is in the valid state (open vs closed) for each function
#define ENSURE_CLOSED 0
#define ENSURE_OPEN 1

//FILE *file;

/* manipulating page files */
void initStorageManager (void) {
   //file = NULL;
}
RC createPageFile (char *fileName) {
    FILE* file= fopen(fileName,"r");
 	if(file == NULL){
 		file = fopen(fileName,"w");
	}
	
	if(file == NULL){
		return RC_FILE_NOT_FOUND;
	}
	else{
		// I moved the code here to addPage() so I could reuse it to append - Francis
		addPage(file);
		fclose(file);
		return RC_OK;
	}
}
RC openPageFile (char *fileName, SM_FileHandle *fHandle) {
    FILE* file= fopen(fileName,"rb+");
	if(file == NULL){
		return RC_FILE_NOT_FOUND;
	}
	else{
		//get the number of page of this file
		fseek(file,0L,SEEK_END);
        int size=ftell(file);
        fHandle->totalNumPages=size/PAGE_SIZE;
        fHandle->curPagePos=0;
		fHandle->fileName=fileName;
		//record the status of the file
        fHandle->mgmtInfo=file;
		return RC_OK;
	}
}
RC closePageFile (SM_FileHandle *fHandle) {
   fclose(fHandle->mgmtInfo);
}
RC destroyPageFile (char *fileName) {
   remove(fileName);
}

/* reading blocks from disc */
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	//check file exit
    FILE* file = fHandle->mgmtInfo;
    if(file == NULL){
    	return RC_FILE_NOT_FOUND;
    }
    //return error if file has less than pageNum pages
    if(pageNum > fHandle->totalNumPages){
    	fclose(file);
		//change status of file
    	fHandle->mgmtInfo = NULL;
    	return RC_READ_NON_EXISTING_PAGE;
    }
	
    //if seek, read into memPage
    if(!fseek(file, (pageNum)*PAGE_SIZE, SEEK_SET)) {
	fread(memPage, sizeof(char), PAGE_SIZE, file);
    //change current position into pageNum(for positioning later)
    fHandle->curPagePos = pageNum;
    //fclose(file);
	return RC_OK;
    }
    else{
    	fclose(file);
    	fHandle->mgmtInfo = NULL;
    	return RC_READ_NON_EXISTING_PAGE;
    }
}
int getBlockPos (SM_FileHandle *fHandle) {
	return fHandle->curPagePos;
}
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	if(RC_OK == readBlock(0,fHandle,memPage))
		return RC_OK;
	else
		return RC_READ_NON_EXISTING_PAGE;
}
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	if(RC_OK == readBlock(getBlockPos(fHandle) - 1, fHandle, memPage))
		return RC_OK;
	else
		return RC_READ_NON_EXISTING_PAGE;
}
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	if(RC_OK == readBlock(getBlockPos(fHandle), fHandle, memPage))
		return RC_OK;
	else
		return RC_READ_NON_EXISTING_PAGE;
}
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	if(RC_OK == readBlock(getBlockPos(fHandle) + 1, fHandle, memPage))
		return RC_OK;
	else
		return RC_READ_NON_EXISTING_PAGE;
}
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	if(RC_OK == readBlock(fHandle->totalNumPages - 1, fHandle, memPage))
		return RC_OK;
	else
		return RC_READ_NON_EXISTING_PAGE;
}

/* writing blocks to a page file */
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC errorCode = RC_WRITE_FAILED;

	if (isInvalidFileHandle(fHandle) 
			|| isPageNumBad(pageNum, fHandle))
		return errorCode;

	//seek to correct point in file, then write
	FILE* file=fHandle->mgmtInfo; //I use fHandle->mgmtInfo to store file status, so I edit all file in the code -Yiyi
	if (seekToPage(pageNum, file) != 0){
		fclose(file);
		//change status of file
		fHandle->mgmtInfo = NULL;
		return errorCode;
	}
	fwrite(memPage, PAGE_SIZE, 1, file);
	//fclose(file); 
	
	if (isFError(file)){
		fclose(file);
		fHandle->mgmtInfo = NULL;
		return errorCode;
	}

	//if the function reaches here, the write was successful
	return RC_OK;
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	RC errorCode = RC_WRITE_FAILED;
	int pageNum;

	if (isInvalidFileHandle(fHandle))
		return errorCode;

	//seek to correct point in file, then write
	FILE* file=fHandle->mgmtInfo;
	if (seekToPage(pageNum, file) != 0){
		fclose(file);
		fHandle->mgmtInfo = NULL;
		return errorCode;
	}
	fwrite(memPage, PAGE_SIZE, 1, file);
	//fclose(file);

	if (isFError(file)){
		fclose(file);
		fHandle->mgmtInfo = NULL;
		return errorCode;
	}

	//if the function reaches here, the write was successful
	return RC_OK;
}

RC appendEmptyBlock (SM_FileHandle *fHandle) {
	RC errorCode = RC_WRITE_FAILED;

	if (isInvalidFileHandle(fHandle))
		return errorCode;

	//append an empty block
	FILE* file=fHandle->mgmtInfo;
	addPage(file);
	//fclose(file);
	
	if (isFError(file)){
		fclose(file);
		fHandle->mgmtInfo = NULL;
		return errorCode;
	}
	fHandle->totalNumPages ++;
	//if the function reaches here, the write was successful
	return RC_OK;
}

//NOT DONE
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
	RC errorCode = RC_WRITE_FAILED;

	if (isInvalidFileHandle(fHandle))
		return errorCode;

	int requiredPages = fHandle->totalNumPages - numberOfPages;

	//if the file already has enough pages, the function does nothing
	if (requiredPages > 0)
		return RC_OK;

	//append a number of blocks equal to requiredPages
	FILE* file=fHandle->mgmtInfo;
	SM_PageHandle a_page = malloc(PAGE_SIZE);

	while (requiredPages > 0){
   	fwrite(a_page, PAGE_SIZE, 1, file);
   	requiredPages--;
	}
	
	free(a_page);
	//fclose(file);
	
	if (isFError(file))
		return errorCode;

	//if the function reaches here, the write was successful
	return RC_OK;
}


/* helper functions */

int isInvalidFileHandle(SM_FileHandle *fHandle){
	// Returns 0 if fHandle is valid (accoring to these tests), 1 otherwise
	// A function using an invalid fHandle should immediately return an error code
	// NOTE: this does not check that mgmtInfo has FILE info
	return fHandle == NULL 
			|| fHandle->totalNumPages < 0 
			|| isPageNumBad(fHandle->curPagePos,fHandle);
			//|| isOpen(fHandle) != ensure; I change to use mgmtInfo to find file open or not -Yiyi
}

int isPageNumBad(int pageNum, SM_FileHandle *fHandle){
	// Returns 0 if pageNum is valid, 1 otherwise
	return pageNum < 0 || pageNum >= fHandle->totalNumPages;
}

/*int isOpen(SM_FileHandle *fHandle) {
	// Returns 1 if the file is open, 0 if it is closed, and -1 for an invalid mgmtInfo state
	int tmp;
	if ((tmp = (int) *(fHandle->mgmtInfo)) != 0 && tmp != 1)
		return -1;
	return tmp;
}*/

int seekToPage(int pageNum, FILE *f){
	// Seeks to the correct page in the file
	fseek(f, ((long int) PAGE_SIZE) * pageNum, SEEK_SET);
}

int isFError(FILE* f){
	// Returns 0 if there is no error associated with f, 1 otherwise
	return ferror(f) != 0;
}

// 
int addPage(FILE *f){
	//assign new file one page
	SM_PageHandle a_page = malloc(PAGE_SIZE);
    fwrite(a_page, PAGE_SIZE, 1, f);
	free(a_page);
}
