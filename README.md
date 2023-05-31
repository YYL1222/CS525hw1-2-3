# README #


### Function description ###

* initStorageManager	
	- Initalize a file 
* createPageFile		
	- Create a new page file and assign one page
* openPageFile			
	* Opens an existing page file and then writes the infomation this file handle  
 	* Should return RC FILE NOT FOUND if the file does not exist.
* closePageFile			
	- Close an open page file
* destroyPageFile		
	- Destroy (delete) a page file
* readBlock				
	* reads the block at position pageNum from a file and stores its content in the memory pointed to by the memPage page handle.
	* If the file has less than pageNum pages, the method should return RC READ NON EXISTING PAGE.
* getBlockPos			
	* Return the current page position in a file
* readFirstBlock		
	* Read the first page in a file
* readLastBlock			
	* Read the last page in a file
* readPreviousBlock		
	* Read the previous page relative to the curPagePos of the file.
	* The curPagePos should be moved to the page that was read.
	* If the user tries to read a block before the first page or after the last page of the file, the method should return RC READ NON EXISTING PAGE.
* readCurrentBlock 	
	* Read the current page relative to the curPagePos of the file.
* readNextBlock			
	* Read the next page relative to the curPagePos of the file.
* writeBlock 			
	* Write a page to disk using an absolute position.
* writeCurrentBlock		
	* Write a page to disk using an current position.
* appendEmptyBlock		
	* Increase the number of pages in the file by one. The new last page should be filled with zero bytes.
* ensureCapacity		
	* If the file has less than numberOfPages pages then increase the size to numberOfPages.

###### We add 5 helper functions ######
* isInvalidFileHandle 	
	* A function using an invalid fHandle should immediately return an error code
* isPageNumBad 			
	* Check if pageNum is valid
* seekToPage 			
	* Seeks to the correct page in the file
* isFError 			
	* Check if there is no error associated with file
* addPage 				
	* assign new file one page

### Run the code ###

    $ make
	$ ./test_assign1

### Check memory leak ###

    $ valgrid --leak-check=full --track-origins=yes ./test_assign1
	