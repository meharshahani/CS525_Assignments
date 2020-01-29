#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"
#include "dt.h"

/* GLOBAL VARIABLES */
SM_FileHandle *fHandle; // file handler to store file's content and info
PageNumber *pageNumArr; // array of pageNum
bool *dirtyFlagArr; // array of dirtyFlag
int *fixCountArr; // array of fixCount
int readPages; // number of read page since the BUffer Pool initializes
int writePages; // number of write page since the BUffer Pool initializes
pthread_rwlock_t rwlock; // read-write lock

//searchPage Searches the requested page in the buffer pool
RC searchPage(BM_BufferPool * const bm, BM_PageHandle * const page, PageNumber pageNum) 
{
	ListPage *row = (ListPage *) bm->mgmtData;

	// If the pagelist size is less than zero then the pagelist is not initialised, and returns the error message
	// else if the size of the PageList is equal to zero then the page list is empty and so it returns the page not found message 
	if (row->size < 0) 
	{
		return_value(RC_PAGELIST_NOT_INIT) ;
	} 
	else if (row->size == 0) 
	{
		return_value(RC_PAGE_NOT_FOUND) ;
	}

	// Set the current pointer to the head of the PageList
	row->current = row->start;

	while (row->current != row->end && row->current->page->pageNum != pageNum) 
	{
		row->current = row->current->next;
	}

	
	// Find if the tail contains the requested page
	if (row->current == row->end && row->current->page->pageNum != pageNum) 
	{
		return_value(RC_PAGE_NOT_FOUND) ;
	}

	// Load the content into BM_PageHandle
	page->data = row->current->page->data;
	page->pageNum = row->current->page->pageNum;

	// return_value() RC_PAGE_FOUND so it can be used ahead in the page replacement algorithms
	return_value(RC_PAGE_FOUND) ;
} 

//Appends data into the tail of the page
RC appendPage(BM_BufferPool * const bm, BM_PageHandle * const page, PageNumber pageNum) 
{
	ListPage *row = (ListPage *) bm->mgmtData;
	RC rc; 

	// Require lock
	pthread_rwlock_init(&rwlock, NULL);
	pthread_rwlock_wrlock(&rwlock);

	// File open
	rc = -99;
	rc = openPageFile(bm->pageFile, fHandle);

	if (rc != RC_OK) {
		return_value(rc) ;
	}

	// if pagelist size equals zero then the PageList is empty
	// else, the PageList is neither empty nor full we can append to the tail of the pagelist
	if (row->size == 0) {
		row->start->count = 1;
		row->start->writeIO = 1;

		// if the page does not exist, then call ensureCapacity to add the requested page to the file
		if (fHandle->totalNumPages < pageNum + 1) {
			int sumPages = fHandle->totalNumPages;
			rc = -99;
			rc = ensureCapacity(pageNum + 1, fHandle);
			writePages += pageNum + 1 - sumPages;

			if (rc != RC_OK) {
				// Do not change fixCount and NumWriteIO back, for this indicates write IO error and need more info to proceed

				// Close file
				rc = -99;
				rc = closePageFile(fHandle);

				if (rc != RC_OK) {
					return_value(rc) ;
				}

				// Release lock
				pthread_rwlock_unlock(&rwlock); 
				pthread_rwlock_destroy(&rwlock);

				return_value(rc) ;
			}
		}
		row->start->writeIO = 0;

		// After ensureCapacity, now we can read the requested page from the file
		row->start->readIO++;
		rc = -99;
		rc = readBlock(pageNum, fHandle, row->start->page->data);
		readPages++;
		row->start->readIO--;

		if (rc != RC_OK) {
			
			// Close file
			rc = -99;
			rc = closePageFile(fHandle);

			if (rc != RC_OK) {
				return_value(rc) ;
			}

			// Release lock
			pthread_rwlock_unlock(&rwlock);
			pthread_rwlock_destroy(&rwlock);

			return_value(rc) ;
		}

		//  fixCount = 1,  numReadIO = 0, numWriteIO = 0
		row->start->page->pageNum = pageNum;
		row->start->flag = FALSE;
		row->start->flagClock = FALSE;

		// One page exists in the pagelist and all pointers are pointing towards it
	} else {
		row->end->next->count = 1;
		row->end->next->writeIO = 1;

		// if the page does not exist, then call ensureCapacity to add the requested page to the file
		if (fHandle->totalNumPages < pageNum + 1) {
			int sumPages = fHandle->totalNumPages;
			rc = -99;
			rc = ensureCapacity(pageNum + 1, fHandle);
			writePages += pageNum + 1 - sumPages;

			if (rc != RC_OK) {
				// Do not change fixCount and NumWriteIO 

				// Close file
				rc = -99;
				rc = closePageFile(fHandle);

				if (rc != RC_OK) {
					return_value(rc) ;
				}

				// Release lock
				pthread_rwlock_unlock(&rwlock);
				pthread_rwlock_destroy(&rwlock);

				return_value(rc) ;
			}
		}
		row->end->next->writeIO = 0;

		// Now we can read the requested page after we add it to  the file
		row->end->next->readIO++;
		rc = -99;
		rc = readBlock(pageNum, fHandle, row->end->next->page->data);
		readPages++;
		row->end->next->readIO--;

		if (rc != RC_OK) {
			// Do not change fixCount and NumWriteIO back

			// Close file
			rc = -99;
			rc = closePageFile(fHandle);

			if (rc != RC_OK) {
				return_value(rc) ;
			}

			// Release lock
			pthread_rwlock_unlock(&rwlock);
			pthread_rwlock_destroy(&rwlock);

			return_value(rc) ;
		}

		// fixCount = 1,  numReadIO = 0, numWriteIO=0 
		row->end->next->page->pageNum = pageNum;
		row->end->next->flag = FALSE;
		row->end->next->flagClock = FALSE;

		row->end = row->end->next;

		// The current pointer now points to the requested page, this now acts as the tail of the page list
		row->current = row->end;
	}

	// Pagelist size is incremented after appending the requested page
	row->size++;

	// Load the requested page into BM_PageHandle
	page->data = row->current->page->data;
	page->pageNum = row->current->page->pageNum;

	// Close file
	rc = -99;
	rc = closePageFile(fHandle);

	if (rc != RC_OK) {
		return_value(rc) ;
	}

	// Release lock
	pthread_rwlock_unlock(&rwlock);
	pthread_rwlock_destroy(&rwlock);

	return_value(RC_OK) ;
} // appendPage

//Replace the current page with the requested page read from the disk
RC replacePage(BM_BufferPool * const bm, BM_PageHandle * const page, PageNumber pageNum) 
{
	ListPage *row = (ListPage *) bm->mgmtData;
	RC rc; // init return_value() code

	// Require lock
	pthread_rwlock_init(&rwlock, NULL);
	pthread_rwlock_wrlock(&rwlock);

	// Open file
	rc = -99;
	rc = openPageFile(bm->pageFile, fHandle);

	if (rc != RC_OK) 
	{
		return_value(rc) ;
	}

	// If the removable page is dirty, then write it back to the disk before removing it.
	// Now fixCount = 0,numReadIO = 0 and numWriteIO = 0
	row->current->count = 1;
	row->current->writeIO = 1;

	// if the removable page is dirty, then write it back to the file
	if (row->current->flag == TRUE) {
		rc = -99;
		rc = writeBlock(row->current->page->pageNum, fHandle,
				row->current->page->data);
		writePages++;

		if (rc != RC_OK) {
			// Do not change fixCount and NumWriteIO back
			// Close file
			rc = -99;
			rc = closePageFile(fHandle);

			if (rc != RC_OK) {
				return_value(rc) ;
			}

			// Release unlock
			pthread_rwlock_unlock(&rwlock);
			pthread_rwlock_destroy(&rwlock);

			return_value(rc) ;
		}

		// After writeBlock, set the PageFrame back to clean
		row->current->flag = FALSE;
	}
	//After ensureCapacity, now we can read the requested page from the file
	if (fHandle->totalNumPages < pageNum + 1) 
	{
		int sumPages = fHandle->totalNumPages;
		rc = -99;
		rc = ensureCapacity(pageNum + 1, fHandle);
		writePages += pageNum + 1 - sumPages;

		if (rc != RC_OK) 
		{
			// Do not change fixCount and NumWriteIO back
			// Close file
			rc = -99;
			rc = closePageFile(fHandle);

			if (rc != RC_OK) 
			{
				return_value(rc) ;
			}

			// Release unlock
			pthread_rwlock_unlock(&rwlock);
			pthread_rwlock_destroy(&rwlock);

			return_value(rc) ;
		}
	}
	row->current->writeIO = 0;

	// After ensureCapacity, now we can read the requested page from the file
	row->current->readIO++;
	rc = -99;
	rc = readBlock(pageNum, fHandle, row->current->page->data);
	readPages++;
	row->current->readIO--;

	if (rc != RC_OK) {
		// Do not change fixCount and NumWriteIO back, for this indicates write IO error and need more info to proceed

		// Close file
		rc = -99;
		rc = closePageFile(fHandle);

		if (rc != RC_OK) 
		{
			return_value(rc) ;
		}

		// Release lock
		pthread_rwlock_unlock(&rwlock);
		pthread_rwlock_destroy(&rwlock);

		return_value(rc) ;
	}

	// Load the requested page to the current PageFrame in the BM_BufferPool
	// Now the fixCount = 1, the numReadIO = 0, and the numWriteIO = 0
	row->current->page->pageNum = pageNum;
	row->current->flagClock = FALSE;

	// Load the requested into BM_PageHandle
	page->data = row->current->page->data;
	page->pageNum = row->current->page->pageNum;

	// Close file
	rc = -99;
	rc = closePageFile(fHandle);

	if (rc != RC_OK) {
		return_value(rc) ;
	}

	// Release lock
	pthread_rwlock_unlock(&rwlock);
	pthread_rwlock_destroy(&rwlock);

	return_value(RC_OK) ;
} // replacePage


//FIFO replacement algorithm
RC FIFO(BM_BufferPool * const bm, BM_PageHandle * const page,
		PageNumber pageNum) {
	ListPage *row = (ListPage *) bm->mgmtData;
	RC rc; // init return_value() code

	// Search the page in the page list by calling the searchPage function
	printf("searchPage: Page-%d\n", pageNum);
	rc = -99;
	rc = searchPage(bm, page, pageNum);

	//  Search the requested page in the row, if its found then return RC_PAGE_FOUND else return rc
	if (rc == RC_PAGE_FOUND) {
		row->current->count++;
		printf("Page-%d found\n", pageNum);

		return_value(rc) ;
	} else if (rc != RC_PAGE_NOT_FOUND) {
		return_value(rc) ;
	}
	printf("Page-%d not found\n", pageNum);

	/*
	   If the code enters this condition then the Buffer Manager doesn't have the requested page
	   We have to read the page from the disk and load it into BM_PageHandle.
	 */

	// if the Buffer Manager has vacancy for the requested page then read the page from the disk and append it to the next of the tail of the PageList
	if (row->size < bm->numPages) {
		printf("appendPage: Page-%d\n", pageNum);
		rc = -99; // reset return_value() code
		rc = appendPage(bm, page, pageNum);

		if (rc == RC_OK) {
			printf("Page-%d appended\n", pageNum);
		}

		return_value(rc) ;
	}

	/*
	  If the code enters this condition, then neither the Buffer Manager has the requested page loaded nor vacancy for the requested page
	  Now the PageList is full, replace an existing page in the Buffer Manager with the requested page
	 */

	
	row->current = row->start;

	// Find page with fixCount=0
	while (row->current != row->end
			&& (row->current->count != 0 || row->current->readIO != 0
					|| row->current->writeIO != 0)) {
		row->current = row->current->next;
	}

	// If the current pointer comes to the tail then we still to determine if the tail's fixCount = 0
	if (row->current == row->end
			&& (row->current->count != 0 || row->current->readIO != 0
					|| row->current->writeIO != 0)) {
		return_value(RC_NO_REMOVABLE_PAGE) ;
	}

	
	 //The current pointer now points to the page that can be removed
	 

	// Here we replace the page that is to be removed with the page that is requested	
	printf("Replace the Page: Page-%d\n", pageNum);
	rc = -99; // reset the return_value() value
	rc = replacePage(bm, page, pageNum);

	// if replacePage completes without error and the requested page is not in the tail spot
	// then move it to the tail of the PageList
	if (rc == RC_OK && row->current != row->end) {
		// Remove the current PageFrame
		
		if (row->current == row->start) {
			row->start = row->start->next;
			row->current->next->pre = NULL;
		} else {
			row->current->pre->next = row->current->next;
			row->current->next->pre = row->current->pre;
		}

		// Add the requested page to the tail of the PageList
		// connect tail and current
		row->current->pre = row->end;
		row->end->next = row->current;

		//  set current's next
		row->current->next = NULL;

		// set tail
		row->end = row->end->next;

		printf("Page-%d is replaced\n", pageNum);

		// Now the current pointer points to the requested page
	}

	return_value(rc) ;
} // FIFO

//LRU replacement algorithm
RC LRU(BM_BufferPool * const bm, BM_PageHandle * const page, PageNumber pageNum) {
	ListPage *row = (ListPage *) bm->mgmtData;
	RC rc; 

	// Run FIFO first
	rc = -99;
	rc = FIFO(bm, page, pageNum);

	/* if FIFO meets error, then return_value() the error code
	else if  RC_PAGE_FOUND, then FIFO completes with searchPage,
	if the requested page is not in the tail then move it to the tail */
	if (rc != RC_OK && rc != RC_PAGE_FOUND) {
		return_value(rc) ;
	} else if (rc == RC_PAGE_FOUND && row->current != row->end) {
	
		if (row->current == row->start) {
			row->start = row->start->next;
			row->current->next->pre = NULL;
		} else {
			row->current->pre->next = row->current->next;
			row->current->next->pre = row->current->pre;
		}

		// Add the current PageFrame to the tail
		// connect tail and current
		row->current->pre = row->end;
		row->end->next = row->current;

		// set current's next
		//check if the pagelist is not full
		// if the PageList is not full, then we should also set the previous pointer of the next PageFrame to the tail to the requested page
		if (row->size < bm->numPages) {
			row->current->next = row->end->next;
			row->end->next->pre = row->current;
		} else {
			row->current->next = NULL;
		}

		// set tail
		row->end = row->end->next;

		// Now the current pointer still points to the requested page
	}

	/*
	  If the code enters, then LRU complete
	  Now the current pointer points to the requested page
	 */

	return_value(RC_OK) ;
} // LRU

//CLOCK replacement algorithm
RC CLOCK(BM_BufferPool * const bm, BM_PageHandle * const page, PageNumber pageNum) 
{
	ListPage *row = (ListPage *) bm->mgmtData;
	RC rc; // init return_value() code

	// First search the page in the PageList
	printf("Search the Page: Page-%d\n", pageNum);
	rc = -99;
	rc = searchPage(bm, page, pageNum);

	// if we find the page then set the current flagclock to true
	
	if (rc == RC_PAGE_FOUND) {
		row->current->count++;
		row->current->flagClock = TRUE;
		printf("Page-%d is found\n", pageNum);

		return_value(rc) ;
	} 
	else if (rc != RC_PAGE_NOT_FOUND) 
	{
		return_value(rc) ;
	}
	printf("Page-%d not found\n", pageNum);

	/*
	  Load the page in the buffer manager if its not loaded in it
	 */

	// if the Buffer Manager has vacancy for the requested page then read the page from the disk and append it 
	if (row->size < bm->numPages) 
	{
		printf("Append the Page: Page-%d\n", pageNum);
		rc = -99; // reset return_value() code
		rc = appendPage(bm, page, pageNum);

		// if the PageList is not full, set the clock pointer to the next to the current pointer
		// else, now the current pointer points to the tail, then set the clock pointer points back to the head
		if (rc == RC_OK) 
		{
			if (row->size < bm->numPages)
			{
				row->clk = row->current->next;
			} 
			else if (row->size == bm->numPages) 
			{
				row->clk = row->start;
			}

			printf("Page-%d is appended\n", pageNum);
		}

		return_value(rc) ;
	}

	/*
	  If the code enters this condition, then neither the Buffer Manager has the requested page loaded nor vacancy for the requested page
	  Now the PageList is full, replace an existing page in the Buffer Manager with the requested page
	 */

	// Find the first page with clockFlag = TRUE and fixCount = 0
	while (row->clk->flagClock == TRUE || row->clk->count != 0 || row->clk->readIO != 0 || row->clk->writeIO != 0) 
	{
		row->clk->flagClock = FALSE;

		// Into the while loop means this page does not fit the requirement of the removable page
		// Move the clock pointer to the next. If it points to the tail, move it to head
		if (row->clk == row->end) {
			row->clk = row->start;
		} else {
			row->clk = row->clk->next;
		}
	}

	// We find the first PageFrame whose clockFlag = FALSE and fixCount = 0
	// Set the current pointer to the clock pointer, so that we can call replacePage
	row->current = row->clk;

	// Replace the removable page with the requested page
	printf("Replace the Page: Page-%d\n", pageNum);
	rc = -99;
	rc = replacePage(bm, page, pageNum);

	if (rc == RC_OK) {
		// After the replacement of the requested page, set its clockFlag to TRUE
		row->clk->flagClock = TRUE;

		// Set the clock pointer to the next to the current pointer
		if (row->clk == row->end) {
			row->clk = row->start;
		} else {
			row->clk = row->clk->next;
		}

		printf("Page-%d is replaced\n", pageNum);
	}

	return_value(rc) ;
} // CLOCK

//Initialize the PageList 
void initPageList(BM_BufferPool * const bm) {
	ListPage *row = (ListPage *) bm->mgmtData;
	FramePage *pf[bm->numPages];

	int i;
	for (i = 0; i < bm->numPages; i++) {
		
		
		pf[i] = (FramePage *) allocate_value(FramePage);

		// Initialize pageframe content
		pf[i]->page = MAKE_PAGE_HANDLE();

	
		pf[i]->page->data = (char *) memory_allocation;
		pf[i]->page->pageNum = NO_PAGE;
		pf[i]->numberOfFrame = i;
		pf[i]->readIO = 0;
		pf[i]->writeIO = 0;
		pf[i]->count = 0;
		pf[i]->flag = FALSE;
		pf[i]->flagClock = FALSE;

		// Add this new PageFrame to the tail

		if (i == 0) {
			pf[i]->pre = NULL;
			pf[i]->next = NULL;
		} else {
			pf[i - 1]->next = pf[i];
			pf[i]->pre = pf[i - 1];
			pf[i]->next = NULL;
		}
	}

	// Reset all pointers and row's size to the initial state
	row->start = pf[0];
	row->end = row->start;
	row->current = row->start;
	row->clk = row->start;
	row->size = 0;

	return_value();
} // initPageList

/**********************************************Buffer Manager Interface Pool Handling*************************************************/

//Initialize the Buffer Pool
RC initBufferPool(BM_BufferPool * const bm, const char * const pageFileName,
		const int numPages, ReplacementStrategy strategy, void *stratData) {
	// Make sure the capacity of the Buffer Pool is valid
	if (numPages <= 0) {
		return_value(RC_INVALID_NUMPAGES) ;
	}

	// init fHandle, pageNumArr, dirtyFlagArr, fixCountArr -> position 1
	// free_space them in shutdownBufferPool -> position 4
	fHandle = (SM_FileHandle *) allocate_value(SM_FileHandle);
	pageNumArr = (PageNumber *) allocate_memPage(PageNumber); 
	dirtyFlagArr = (bool *) allocate_memPage(bool); 
	fixCountArr = (int *) allocate_memPage(int); 

	// init readPages, writePages
	readPages = 0;
	writePages = 0;

	// init BM_BufferPool's profiles
	bm->pageFile = (char *) pageFileName; // set the name of the requested page file
	bm->numPages = numPages; // set the capacity of the Buffer Pool
	bm->strategy = strategy; // set the replacement strategy

	// init PageList and store the entry 
	
	ListPage *row = (ListPage *) allocate_value(ListPage);
	bm->mgmtData = row;


	initPageList(bm);

	return_value(RC_OK) ;
} // initBufferPool

//Close the buffer pool
RC shutdownBufferPool(BM_BufferPool * const bm) {
	ListPage *row = (ListPage *) bm->mgmtData;
	RC rc; 

	// Flush all dirty pages in the Buffer Pool back to the disk
	rc = -99;
	rc = forceFlushPool(bm);

	if (rc != RC_OK) {
		return_value(rc) ;
	}

	/*
	  Now all the pages in the Buffer Poll are clean
	  Now we need to free space in PageFrame of the PageList inside out
	 */

	// Set the current pointer to the tail
	row->current = row->end;

	
	if (bm->numPages == 1) {
		
		free_space(row->start->page->data);

		
		free_space(row->start->page);
	} else {
		while (row->current != row->start) {
			
			row->current = row->current->pre;
			free_space(row->current->next->page->data);
			free_space(row->current->next->page);
		}

		// After the while loop, the current pointer points to the head, then free_space the only left block
		
		free_space(row->start->page->data);

		free_space(row->start->page);
	}
	free_space(row);


	free_space(fHandle);
	free_space(pageNumArr);
	free_space(dirtyFlagArr);
	free_space(fixCountArr);

	return_value(RC_OK) ;
} // close bufferpool

//Write data in all dirty pages
RC forceFlushPool(BM_BufferPool * const bm) {
	ListPage *row = (ListPage *) bm->mgmtData;
	int noWriteCount = 0;

	// Set the current pointer to the head
	row->current = row->start;
	// check if fixcount=0
	// point the current pointer to tail
	while (row->current != row->end) {
		
		if (row->current->flag == TRUE && row->current->count > 0) 
		{
			noWriteCount++;
		} 
		else if (row->current->flag == TRUE && row->current->count == 0)
		 {
			forcePage(bm, row->current->page);
		}

		row->current = row->current->next;
	}

	//  the current points to the tail of the PageList
	
	if (row->current == row->end) 
	{
		if (row->current->flag == TRUE && row->current->count > 0) 
		{
			noWriteCount++;
		} 
		else if (row->current->flag == TRUE && row->current->count == 0) 
		{
			forcePage(bm, row->current->page);
		}
	}

	// if there is any unwritable page, then return_value() error code
	if (noWriteCount != 0) {
		return_value(RC_FLUSH_POOL_ERROR) ;
	}

	return_value(RC_OK) ;
} // forceFlushPool

/**************************************************Buffer Manager Interface Access Pages****************************************************/

//Pin the page with the requested pageNum in the BUffer Pool. If the page is not in the Buffer Pool, load it from the file to the Buffer Pool
RC pinPage(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum) {
	RC rc = -99; // init the return_value() code

	if (bm->strategy == RS_FIFO) {
		rc = FIFO(bm, page, pageNum);

		if (rc == RC_PAGE_FOUND) {
			rc = RC_OK;
		}
	} else if (bm->strategy == RS_LRU) {
		rc = LRU(bm, page, pageNum);
	} else if (bm->strategy == RS_CLOCK) {
		rc = CLOCK(bm, page, pageNum);
	} else if (bm->strategy == RS_LFU) {
		
		return_value(RC_RS_NOT_IMPLEMENTED);
	} else if (bm->strategy == RS_LRU_K) {
	
		return_value(RC_RS_NOT_IMPLEMENTED);
	}

	return_value(rc) ;
} 

//Set the requested page as dirty
RC markDirty(BM_BufferPool * const bm, BM_PageHandle * const page) {
	ListPage *row = (ListPage *) bm->mgmtData;
	RC rc; 

	// Search the requested page in the PageList
	rc = -99;
	rc = searchPage(bm, page, page->pageNum);

	// if page not found, then return rc
	if (rc != RC_PAGE_FOUND) {
		return_value(rc) ;
	}

	// Now the current pointer points to the requested page
	
	row->current->flag = TRUE;

	return_value(RC_OK) ;
} 

//Unpin a page
RC unpinPage(BM_BufferPool * const bm, BM_PageHandle * const page) {
	ListPage *row = (ListPage *) bm->mgmtData;
	RC rc;

	// First search the requested page in the row
	rc = -99;
	rc = searchPage(bm, page, page->pageNum);

	// if page not found, then return rc
	if (rc != RC_PAGE_FOUND) {
		return_value(rc) ;
	}

	
	row->current->count--;
	return_value(RC_OK) ;
} 

//Write the requested page back to the page file on disk
RC forcePage(BM_BufferPool * const bm, BM_PageHandle * const page) {
	ListPage *row = (ListPage *) bm->mgmtData;
	RC rc = -99;

	// Require lock
	pthread_rwlock_init(&rwlock, NULL);
	pthread_rwlock_wrlock(&rwlock);

	// Open file
	rc = -99;
	rc = openPageFile(bm->pageFile, fHandle);

	if (rc != RC_OK) {
		return_value(rc) ;
	}

	row->current->writeIO = 1;

	// Write the requested page back to the disk
	rc = -99;
	rc = writeBlock(page->pageNum, fHandle, page->data);
	writePages++;

	if (rc != RC_OK) {
		// Close file
		rc = -99;
		rc = closePageFile(fHandle);

		if (rc != RC_OK) {
			return_value(rc) ;
		}

		// Release unlock
		pthread_rwlock_unlock(&rwlock);
		pthread_rwlock_destroy(&rwlock);

		return_value(rc) ;
	}

	// Set the page back to clean
	row->current->flag = FALSE;

	// set numWriteIO back
	row->current->writeIO = 0;

	// Close file
	rc = -99;
	rc = closePageFile(fHandle);

	if (rc != RC_OK) {
		return_value(rc) ;
	}

	// Release lock
	pthread_rwlock_unlock(&rwlock);
	pthread_rwlock_destroy(&rwlock);

	return_value(RC_OK) ;
} 

/**********************************************************Statistics Interface***********************************************************/


// An empty page frame is represented using the constant NO_PAGE
PageNumber *getFrameContents(BM_BufferPool * const bm) {
	ListPage *row = (ListPage *) bm->mgmtData;

	// Set the current pointer to head
	row->current = row->start;
	// initially set index to zero
	int index = 0; 

	while (row->current != row->end) {
		pageNumArr[index] = row->current->page->pageNum;

		row->current = row->current->next;
		index++;
	}

	// Now the current pointer points to the tail
	// Include the tail's info into the array, then increment the pos to set it be equal to the size of the PageList
	pageNumArr[index++] = row->current->page->pageNum;

	// check if the PageList is full or not, add the values to the pool
	if (index < bm->numPages) {
		int i;
		for (i = index; i < bm->numPages; i++) {
			pageNumArr[i] = NO_PAGE;
		}
	}

	row->current = row->end;

	return_value(pageNumArr) ;
} // getFrameContents

//return_value()s an array of bools (of size numPages). Empty page frames are considered as clean
bool *getDirtyFlags(BM_BufferPool * const bm) {
	ListPage *row = (ListPage *) bm->mgmtData;

	row->current = row->start; //set current to head

	int index = 0; // let pos be the position in array, initially it's 0
	while (row->current != row->end) {
		// Put the value of the page pointed to by the current pointer into the current position of array
		dirtyFlagArr[index] = row->current->flag;

		row->current = row->current->next;
		index++; 
		//increment index
	}

	// Now the current pointer points to the tail

	dirtyFlagArr[index++] = row->current->flag;

	// Now pos = the size of the PageList

	// check if the PageList is full or not, add the values to the pool
	if (index < bm->numPages) {
		int i;
		for (i = index; i < bm->numPages; i++) {
			dirtyFlagArr[i] = FALSE;
		}
	}

	row->current = row->end;

	return_value(dirtyFlagArr) ;
} // getDirtyFlags


 //return_value()s an array of ints (of size numPages)
int *getFixCounts(BM_BufferPool * const bm) {
	ListPage *row = (ListPage *) bm->mgmtData;

	row->current = row->start; //set current to head

	int index = 0; // let pos be the position in array, initially it's 0
	while (row->current != row->end) {
		// Put the value of the page pointed to by the current pointer into the current position of array
		fixCountArr[index] = row->current->count;

		row->current = row->current->next;
		index++; // pos moves to next position
	}

	// Now the current pointer points to the tail
	fixCountArr[index++] = row->current->count;


	// check if the PageList is full or not, add the values to the pool
	if (index < bm->numPages) 
	{
		int i;
		for (i = index; i < bm->numPages; i++) 
		{
			fixCountArr[i] = 0;
		}
	}

	row->current = row->end;

	return_value(fixCountArr) ;
} // getFixCounts


 //return_value()s the number of pages that have been read from disk since the Buffer Pool has been initialized
int getNumReadIO(BM_BufferPool * const bm) {
	return_value(readPages) ;
} // getNumReadIO


 //return_value() the number of pages written to the page file since the Buffer Pool has been initialized
int getNumWriteIO(BM_BufferPool * const bm) {
	return_value(writePages) ;
} // getNumWriteIO
