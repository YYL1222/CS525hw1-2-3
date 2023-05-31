#include "buffer_mgr.h"
#include "storage_mgr.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

int readIO=0;
int writeIO=0;
buffer_pageinfo *getFrame (BM_BufferPool *const bm, PageNumber pageNum);
Map_Node *getReplacementNode (BM_BufferPool *const bm);
void initpageframe(Map_Node *node);
int NodeAddFix(BM_BufferPool *bm,const PageNumber pageNum);
Map_Node *createMap_Node(const int numPages);
void inserEnd(BM_BufferPool *bm, Map_Node *mn);
void getFIFO_LRUNode (BM_BufferPool *const bm);
// record more information of page frame in buffer pool 

// moved buffer_pageinfo to h file, and added map_node, a linked list of mappings. - Francis

/* This is the way I implemented the list of map nodes: - Francis
		Each node has a next and prev pointing to the next and prev nodes in the list.
		The head's prev is the last node, and the last node's next is the head.
		
		Nodes should only be added upon initialization of the BM and should only be deleted upon shutdown of the BM.
			In other words, most functions should not add/delete nodes.
			Init of BM will create nodes with pageNum of NO_PAGE (from buffer_mgr.h).
			For the stat functions to be correct, the the nodes should be created in the same order as their buffer_pageinfo.

		The node that should be replaced for all RSs is the first node that has a pageNum of NO_PAGE 
			or points to a buffer_pageinfo with a fixCount of 0.
			I wrote a function below called getReplacementNode(bm) that returns the first node eligible for replacement.

		We implement the RSs by moving nodes in the list such that nodes are replaced in the correct order.
			In other words, when a function may change the order nodes should be potentially replaced in, 
			it's the responsibility of the function to correctly change the order of the list by moving nodes.
			
		For FIFO and LRU, functions (I think) only need to move nodes to the end of the list in order to correctly implement this change.
			I wrote a function below called moveNodeToEnd(bm, node) that does this.
*/	
 int record_position; //record the buffer pool position for print the result
// Replacement Strategies
//Writen by Yi Yi
//I have sorted the node in other helper functions, so FIFO and LRU can use this replacement function togrther
RC Replacement(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum){
	printf("start replacement------\n");
	int buffer_size = bm->numPages;
	if((NodeAddFix(bm,pageNum)) == 0){  //need to do replacement
	     //if first node is free, we delete it and assign a new page at the end of node
	     Map_Node *mn=(Map_Node*)(bm->map_nodes);
	     if( mn->page_frame->page_fixCount==0){
	        if(mn->page_frame->page_dirty==1){
	            writeBlock (bm->map_nodes->pageNum, &bm->fh,bm->map_nodes->page_frame->data);
	            writeIO++;
	        }
	        record_position=bm->map_nodes->position;
	        bm->map_nodes=bm->map_nodes->next;
	     }
	     else{
	        //delete the free page we found in the map nodes
	        getFIFO_LRUNode(bm);
	     }

		//assign a new pageframe
		buffer_pageinfo *BP_page=(buffer_pageinfo *)malloc(sizeof(buffer_pageinfo));
		BP_page->data = (char*)malloc(PAGE_SIZE);
		readBlock (pageNum, &bm->fh, BP_page->data); //put new data context in page->data
        readIO++;
		page->data=BP_page->data;
		BP_page->page_dirty=0;
		BP_page->page_fixCount=1;
		page->pageNum=pageNum;
		Map_Node *newnode=(Map_Node*)malloc(sizeof(Map_Node));
		newnode->position=record_position;
		newnode->pageNum=pageNum;
		newnode->page_frame=BP_page;
		newnode->next=NULL;
		inserEnd(bm,newnode);  //insert new page frame to the end of Map_Node
		return RC_OK;
	}
	else{ //this page has been in the buffer pool already, so its fix +1 in NodeAddFix(mn,pageNum)
	    page->pageNum=pageNum;
		return RC_OK;
	}	
	//bm->mgmtData=q->page;
	//free(BP_page);
	//free(mn);
}

// Buffer Manager Interface Pool Handling
//initialize the buffer pages in this buffer pool
//Writen by Yi Yi
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		const int numPages, ReplacementStrategy strategy,
		void *stratData){
		    printf("initailize buffer pool-------\n");
			//buffer_pageinfo *BP_page=(buffer_pageinfo *)malloc(sizeof(buffer_pageinfo));
			SM_FileHandle fh;
			readIO=0;
            writeIO=0;
			char *filename;
			filename=(char*)pageFileName; //change const int to int
			//need to close file when close buffer pool
			if(openPageFile(filename,&fh) != RC_OK){
				return RC_FILE_NOT_FOUND;
			}
			else{
				bm->pageFile=filename;
				bm->numPages=numPages;
				bm->strategy=strategy;		
				//initialize first node
				Map_Node *mn=createMap_Node(numPages);
				bm->map_nodes=mn;
				bm->fh = fh;
				return RC_OK;
			}		
}
//Writen by Yi Yi
RC shutdownBufferPool(BM_BufferPool *const bm){
	forceFlushPool(bm);
	int buffer_size = bm->numPages;
	Map_Node *mn=(Map_Node*)(bm->map_nodes);

	closePageFile (&bm->fh);
	free(mn->page_frame->data);
	free(mn->page_frame);
	free(mn);
	return RC_OK;
	//free(bm);
}
//writen by Yi Yi
RC forceFlushPool(BM_BufferPool *const bm){
    printf("start force flysh pool----------");
	Map_Node *mn=(Map_Node*)(bm->map_nodes);
	//buffer_pageinfo *page_fr = mn->page_frame;

	//check no dirty page
	while(mn != NULL){
		if(mn->page_frame->page_dirty==1 && mn->page_frame->page_fixCount==0){
			writeBlock (mn->pageNum, &bm->fh,mn->page_frame->data);
			writeIO++;
			mn->page_frame->page_dirty=0;
		}
		mn=mn->next;
	}
	return RC_OK;
}

// Buffer Manager Interface Access Pages
//Writen by Francus
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
	buffer_pageinfo *frame = getFrame(bm, page->pageNum);
	if (frame == NULL)
		return RC_FILE_NOT_FOUND;
	frame->page_dirty = 1;
	return RC_OK;
}
//Writen by Francus
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
	buffer_pageinfo *frame = getFrame(bm, page->pageNum);
	if (frame == NULL)
		return RC_FILE_NOT_FOUND;
	frame->page_fixCount -= 1;
	return RC_OK;
}

//Writen by Francus
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
	buffer_pageinfo *frame = getFrame(bm, page->pageNum);
	if (frame == NULL)
		return RC_FILE_NOT_FOUND;

	RC error;
	if ((error = writeBlock(page->pageNum, &bm->fh, page->data)) != RC_OK)
		return error;
    writeIO++;
	frame->page_dirty = 0;
	return RC_OK;
}
//Written by Francus
//debugged by Yi Yi
/*Some problem in mapping and pointer, so I deleted getMapping function.
  I only check the buffer pool is full or not.
  If it is full, action will do in replacement strategy
  Otherwise, it can just assign in empty page frame in this function-- Yi Yi */
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
	//Map_Node *mapping = getMapping(bm, pageNum);
	Map_Node *mapping;
	//RS rs = bm->strategy;
    printf("start pin page------\n");
    mapping = getReplacementNode(bm);
    //buffer pool is not full
	if (mapping != NULL){
		//if (mapping == NULL)
			//return RC_FILE_NOT_FOUND;

		//if (mapping->pageNum != NO_PAGE){
		//	if (mapping->page_frame->page_dirty == 1)
		//		forcePage(bm, mapping->page_frame->page_handle);
		//	free(mapping->page_frame->page_handle);
		//}
        if ((readBlock(pageNum, &bm->fh, mapping->page_frame->data)) != RC_OK)
        	return RC_READ_NON_EXISTING_PAGE;
        readIO++;
        //buffer_pageinfo *page_frame=(buffer_pageinfo *)malloc(sizeof(buffer_pageinfo));
		mapping->pageNum = pageNum;
		mapping->position=record_position;
		mapping->page_frame->page_dirty=0;
		mapping->page_frame->page_fixCount=1;
		page->pageNum = pageNum;
		page->data = mapping->page_frame->data ;
		return RC_OK;
	}
	else{
	    if (bm->strategy == RS_FIFO)
    			Replacement(bm, page,pageNum);

    	else if(bm->strategy == RS_LRU)
                Replacement(bm, page,pageNum);
	}
}
        
// Statistics Interface
//Writen by Chris
PageNumber *getFrameContents (BM_BufferPool *const bm){
	Map_Node *curNode = bm -> map_nodes;
    PageNumber *frameContents = (PageNumber*)malloc(sizeof(PageNumber)*bm->numPages);
    
	int i;
	printf("create frame\n");
	for (i=0;i<bm->numPages;i++){
		frameContents[curNode->position] = (curNode->pageNum != -1 ? curNode->pageNum : NO_PAGE);
		curNode = curNode->next;
	}

    return frameContents;
}
//Writen by Chris
bool *getDirtyFlags (BM_BufferPool *const bm){
	Map_Node *curNode = bm -> map_nodes;
	bool *dirt = (bool *)malloc(sizeof(bool)*bm->numPages);

    int i;

	for (i=0;i<bm->numPages;i++){
		buffer_pageinfo *frame = curNode -> page_frame;
		dirt[curNode->position] = (frame -> page_dirty > 0 ? true : false);
		curNode = curNode -> next;
	}

    return dirt;    
}
//Writen by Chris
int *getFixCounts (BM_BufferPool *const bm){
	Map_Node *curNode = bm -> map_nodes;
	int *fixc = (int *)malloc(sizeof(int)*bm->numPages);
    int i;

	for (i=0;i<bm->numPages;i++){
		buffer_pageinfo *frame = curNode -> page_frame;
		fixc[curNode->position] = frame -> page_fixCount;
		curNode = curNode -> next;
	}

    return fixc;
}
//Writen by Chris
int getNumReadIO (BM_BufferPool *const bm){
	return readIO;
}
//Writen by Chris
int getNumWriteIO (BM_BufferPool *const bm){
	return writeIO;
}

// Helper Functions

//returns frame for a page with the given pageNum
//returns NULL if no frame contains data for the given pageNum
//writen by Francus
buffer_pageinfo *getFrame (BM_BufferPool *const bm, PageNumber pageNum){
	Map_Node *curNode = bm->map_nodes;
	if (curNode->pageNum == pageNum)
		return curNode->page_frame;

	curNode = curNode->next;
	while (curNode != NULL){
		if (curNode->pageNum == pageNum)
			return curNode->page_frame;
		curNode = curNode->next;
	}

	return NULL;
}

//returns the next available node for replacing
//returns NULL if no available replacement (all frames are in use)
//written by Francus
Map_Node *getReplacementNode (BM_BufferPool *const bm){
	Map_Node *curNode = bm->map_nodes;
	if (curNode->pageNum == NO_PAGE){
	    record_position=curNode->position;
	    return curNode;
	}

	curNode = curNode->next;
	while (curNode != NULL){
		if (curNode->pageNum == NO_PAGE){
		    record_position=curNode->position;
			return curNode;
		}
		curNode = curNode->next;
	}
	return NULL;
}
//initialize a pageframe to each page in buffer pool
//writen by Yi Yi
void initpageframe(Map_Node *node){
    printf("assign new page frame to each map----------\n");
	buffer_pageinfo *bp=(buffer_pageinfo *)malloc(sizeof(buffer_pageinfo));
	bp->page_dirty=0;
	bp->page_fixCount=0;
	bp->data=(char *) malloc(PAGE_SIZE);
	node->page_frame=bp;
}
//writen by Yi Yi
int NodeAddFix(BM_BufferPool *bm,const PageNumber pageNum){
    int i;
	Map_Node *node=bm->map_nodes;
	Map_Node *temp,*cur; //record the page want to move
	if(node->pageNum ==pageNum){
	    node->page_frame->page_fixCount++;
	    temp=node;
	    bm->map_nodes= bm->map_nodes->next; //delete first page frame and move it to the end
	    inserEnd(bm,temp);
        return 1;
	}
	while(node != NULL){
		if(node->pageNum ==pageNum){
			node->page_frame->page_fixCount++;
			//move to the last one
            temp=node;
            cur->next = node->next;
            inserEnd(bm,temp);
			return 1;
		}else{
		    cur = node;
			node=node->next;  
		}
	}
	return 0; //if this page doesn't exist, return 0 and do the replacement
}

//initialize all Map_Nodes with no_page
//writen by Yi Yi
Map_Node *createMap_Node(const int numPages){
	int i;
	printf("generate map_node--------\n");
	Map_Node *node,*cur,*pre;
	for(i=0;i<numPages;i++){
		cur=(Map_Node *) malloc(sizeof(Map_Node));
		cur->pageNum=NO_PAGE;
		cur->position = i;
		initpageframe(cur);
		if(i==0){
			node=cur;  
		}else{
			pre->next=cur;
		}
		cur->next=NULL; 
		pre=cur; 
	}
	return node;
}

// Use in FIFO & LRU, put new page frame to the end of the map_node
//writen by Yi Yi
void inserEnd(BM_BufferPool *bm, Map_Node *mn){
	Map_Node *node=bm->map_nodes;
	
	while(node->next != NULL){
		node=node->next;
	}
	node->next = mn;
	mn->next=NULL;
}
//writen by Yi Yi
void getFIFO_LRUNode (BM_BufferPool *const bm){
	Map_Node *curNode = bm->map_nodes;
	Map_Node *cur; //record previous one
	while (curNode != NULL){
		if (curNode->page_frame->page_fixCount<=0){
		    if(curNode->page_frame->page_dirty==1){
                writeBlock (curNode->pageNum, &bm->fh,curNode->page_frame->data);
                writeIO++;
		    }
		    record_position=curNode->position;
		    cur->next = curNode->next;
		    return;
		}
		cur = curNode;
		curNode = curNode->next;
	}
}