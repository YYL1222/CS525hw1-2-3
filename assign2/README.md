# README #


### Function description ###

* initBufferPool	
	- creates a new buffer pool
* shutdownBufferPool		
	- destroys a buffer pool
	- it should free the memory allocated for page frame
* forceFlushPool			
	* causes all dirty pages (with fix count 0) from the buffer pool to be written to disk.
* pinPage			
	- pins the page with page number pageNum
	- data field should point to the page frame the page is stored in
* unpinPage		
	- unpins the page page
* markDirty				
	* marks a page as dirty
* forcePage			
	* write the current content of the page back to the page file on disk
* getFrameContents		
	* returns an array of PageNumbers (of size numPages) where the i th element is the number of the page stored in the ith page frame. An empty page frame is represented using the constant NO PAGE.
* getDirtyFlags			
	* returns an array of bools (of size numPages) where the ith element is TRUE if the page stored in the ith page frame is dirty. Empty page frames are considered as clean.
* getFixCounts		
	*returns an array of ints (of size numPages) where the ith element is the fix count of the page stored in the ith page frame. Return 0 for empty page frames.
* getNumReadIO 	
	* returns the number of pages that have been read from disk since a buffer pool has been initialized. 
* getNumWriteIO			
	* returns the number of pages written to the page file since the buffer pool has been initialized.

###### Data Types and Structures ######
* BM_BufferPool
```
	typedef struct BM_BufferPool { 
		char *pageFile;
		int numPages;
		int readIO;
		int writeIO;
		ReplacementStrategy strategy;
		void *page_frames;
		int fifo_num;	
		int lru_num;
		int size;
		SM_FileHandle fHandle;
		void *mgmtData;
	} BM_BufferPool;
```	

* BM_PageHandle
``` 
	typedef struct BM_PageHandle {
		PageNumber pageNum;
		char *data;
	} BM_PageHandle;
```

* buffer_pageinfo
```
	typedef struct buffer_pageinfo{ 
		PageNumber pageNum;
		char *data;
		int page_dirty;
		int page_fixCount;
		int num_visit; //record
	}buffer_pageinfo;
```

### Run the code ###

    $ make
	$ ./test_1
	