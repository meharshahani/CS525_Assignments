

#include "buffer_mgr.h"
#include "dberror.h"
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"
#include "bufferMgrDataStructures.h"

/******************************************************************************************
Buffer Manager Interface Access Pages
Aswini Anki Reddy (A20422757)
Functions name: markDirty, unpinPage, forcePage, pinPage
********************************************************************************************/

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
	pageAndDirtyBitIndicator *temp;
	//here[START]
	ramPageMap *start = firstRamPageMapPtr;
	while(start->discPageFrameNumber != page->pageNum)
	{
		assign_val(start,start->nextRamPageMap);
	}
	//here[END]
	assign_val(temp,firstPageAndDirtyBitMap);
	//here
	//while(page->pageNum != temp->ramPageFrameNumber) {
	while(start->ramPageFrameNumber != temp->ramPageFrameNumber) {
		assign_val(temp,temp->nextPageDirtyBit);
	}

	if(temp != NULL)
	{
		assign_val(temp->isPageDirty,1);
		return(RC_OK);
	}
	else
	{
		return(RC_DIRTY_UPDATE_FAILED);
	}
}

frameList *createBufferPool(int numPages)
{
	int counter = 1;
	frameList *currentFramePtr = NULL;
	frameList *previousFramePtr = NULL;

	while(counter <= numPages) //Ex : create 3 pages and link them to form a list.
	{
        assign_val(currentFramePtr,(frameList *)malloc(sizeof(frameList)));

		if(counter == 1){
			assign_val(firstFramePtr,currentFramePtr); // firstFramePtr. deallocate Pool using this.
        }
		else
        {
			assign_val(previousFramePtr->nextFramePtr,currentFramePtr);
        }

		assign_val(previousFramePtr,currentFramePtr);
		counter++;
	}
	assign_val(currentFramePtr->nextFramePtr,NULL);
	return(firstFramePtr);
}

ramPageMap *createRamPageMapList(int numPages)
{
	int counter = 0;
	ramPageMap *currentRamPagePtr = NULL;
	ramPageMap *previousRamPagePtr = NULL;
	ramPageMap *start = NULL;
	while(counter < numPages) //Ex : create 3 Maps and link them.
	{
		currentRamPagePtr = (ramPageMap *)malloc(sizeof(ramPageMap));

		if(counter == 0)
		{
			assign_val(start,currentRamPagePtr); // firstFramePtr. deallocate Pool using this.
			assign_val(clockPtr,start);
		}
		else
			assign_val(previousRamPagePtr->nextRamPageMap,currentRamPagePtr);

		assign_val(currentRamPagePtr->ramPageFrameNumber,counter);
		assign_val(currentRamPagePtr->discPageFrameNumber,-1);
		assign_val(currentRamPagePtr->clockReferenceBit, 0);

		assign_val(previousRamPagePtr,currentRamPagePtr);
		counter++;
	}
	assign_val(currentRamPagePtr->nextRamPageMap,NULL);
	return(start);
}

pageAndDirtyBitIndicator *createPageAndDirtyBitMap(int numPages)
{
	int counter = 0;
	pageAndDirtyBitIndicator *currrentPageDirtyBitPtr = NULL;
	pageAndDirtyBitIndicator *previousPageDirtyBitPtr = NULL;
	pageAndDirtyBitIndicator *start = NULL;
	while(counter < numPages) //Ex : create 3 Maps and link them.
	{
		assign_val(currrentPageDirtyBitPtr,(pageAndDirtyBitIndicator *)malloc(sizeof(pageAndDirtyBitIndicator)));

		if(counter == 0){
			assign_val(start,currrentPageDirtyBitPtr); // firstFramePtr. deallocate Pool using this.
        }
		else{
			assign_val(previousPageDirtyBitPtr->nextPageDirtyBit,currrentPageDirtyBitPtr);
        }
		assign_val(currrentPageDirtyBitPtr->ramPageFrameNumber,counter);
		assign_val(currrentPageDirtyBitPtr->isPageDirty,0);

		assign_val(previousPageDirtyBitPtr,currrentPageDirtyBitPtr);
		counter++;
	}
	assign_val(currrentPageDirtyBitPtr->nextPageDirtyBit,NULL);
	return(start);
}

pageAndFixCount *createPageAndFixCountMap(int numPages)
{
	int counter = 0;
	pageAndFixCount *currrentPageandFixCountPtr = NULL;
	pageAndFixCount *previousPageandFixCountPtr = NULL;
	pageAndFixCount *start = NULL;
	while(counter < numPages) //Ex : create 3 Maps and link them.
	{
		assign_val(currrentPageandFixCountPtr,(pageAndFixCount *)malloc(sizeof(pageAndFixCount)));

		if(counter == 0){
			assign_val(start,currrentPageandFixCountPtr); // firstFramePtr. deallocate Pool using this.
        }
		else{
			assign_val(previousPageandFixCountPtr->nextPageFixCount,currrentPageandFixCountPtr);
        }
		assign_val(currrentPageandFixCountPtr->ramPageFrameNumber,counter);
		assign_val(currrentPageandFixCountPtr->fixCount,0);

		assign_val(previousPageandFixCountPtr,currrentPageandFixCountPtr);
		counter++;
	}
	assign_val(currrentPageandFixCountPtr->nextPageFixCount,NULL);
	return(start);
}
/******************************************************************************************
Buffer Manager Interface Pool Handling
Author: Vinay krishna nelly (A20428194)
Functions name: initBufferPool, shutdownBufferPool, forceFlushPool
*******************************************************************************************/
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
				  const int numPages, ReplacementStrategy strategy,
				  void *stratData)
{
	numberOfWrites = 0;
	numberOfReads  = 0;

	frameContentPtr = (PageNumber *)malloc(sizeof(PageNumber) * numPages);
	dirtyBitPtr = (bool *)malloc(sizeof(bool) * numPages);
	fixCountPtr = (int *)malloc(sizeof(int) * numPages);

	numberOfFrames = numPages;
	//fHandle = (SM_FileHandle *)malloc(sizeof(SM_FileHandle));
	assign_val(bm->mgmtData,createBufferPool(numPages)); //gives address of the first Frame.
	assign_val(firstRamPageMapPtr,createRamPageMapList(numPages));//give address of the first RamPage Map.
	assign_val(firstPageAndDirtyBitMap,createPageAndDirtyBitMap(numPages));
	assign_val(firstPageAndFixCountPtr,createPageAndFixCountMap(numPages));
	openPageFile((char *)pageFileName, &fHandle);

	assign_val(bm->numPages,numPages);
	assign_val(bm->pageFile,(char *)pageFileName);
	assign_val(bm->strategy,strategy);
	if(bm->mgmtData != NULL && firstRamPageMapPtr !=NULL && firstPageAndDirtyBitMap != NULL && firstPageAndFixCountPtr != NULL)
    {
		return(RC_OK);
    }
	else
    {
		return(RC_BUFFER_POOL_INIT_ERROR);
    }
}

RC checkIfPagePresentInFramePageMaps(const PageNumber pageNum)
{

	ramPageMap *start = firstRamPageMapPtr;
	while(start != NULL)
	{
		if(start->discPageFrameNumber == pageNum)
        {
			return(start->ramPageFrameNumber);
        }
		assign_val(start,start->nextRamPageMap);
	}
	return(RC_NO_FRAME);
}

void getFrameData(int frameNumber,BM_PageHandle * page)
{
	frameList *start = firstFramePtr;
	int counter = 0;
	while(counter < frameNumber)
	{
		assign_val(start,start->nextFramePtr);
		counter++;
	}
	assign_val(page->data,start->frameData);
}

void getFirstFreeFrameNumber(int *firstfreeFrameNumber,PageNumber PageNum)
{
	ramPageMap *start = firstRamPageMapPtr;
	while(start != NULL && start->discPageFrameNumber != -1)
	{
		assign_val(start,start->nextRamPageMap);
	}
	if(start != NULL)
	{
		*firstfreeFrameNumber = start->ramPageFrameNumber;
		assign_val(start->discPageFrameNumber,PageNum);
	}
	else
	{
		*firstfreeFrameNumber = -99;
	}

}

RC changeFixCount(int flag,int page)
{
	ramPageMap *startFramePtr = firstRamPageMapPtr;
	while(startFramePtr != NULL && startFramePtr->discPageFrameNumber != page)
	{
		assign_val(startFramePtr,startFramePtr->nextRamPageMap);
	}

	pageAndFixCount *startFixCountPtr = firstPageAndFixCountPtr;
	while((startFixCountPtr != NULL) && (startFixCountPtr->ramPageFrameNumber != startFramePtr->ramPageFrameNumber))
	{
		assign_val(startFixCountPtr,startFixCountPtr->nextPageFixCount);
	}
	if(flag == 1)
		startFixCountPtr->fixCount++;
	else
		startFixCountPtr->fixCount--;

	return(RC_OK);
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {

	if(writeBlock (page->pageNum, &fHandle, page->data) == RC_OK)
	{
		numberOfWrites++;
		return(RC_OK);
	}
	else
	{
		return(RC_WRITE_FAILED);
	}
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	//If discNumber is passed, page->pageNum will be assigned with pageNumber
	//here

	ramPageMap *startFramePtr = firstRamPageMapPtr;
	while(startFramePtr != NULL && startFramePtr->discPageFrameNumber != page->pageNum)
	{
		assign_val(startFramePtr,startFramePtr->nextRamPageMap);
	}
	RC status = changeFixCount(2,startFramePtr->discPageFrameNumber); //

	int frameNumber = startFramePtr->ramPageFrameNumber;//here

	//If dirty, call forcepage.

	pageAndDirtyBitIndicator *startDirtyPointer = firstPageAndDirtyBitMap;
	while(startDirtyPointer != NULL && startDirtyPointer->ramPageFrameNumber != frameNumber)
	{
		assign_val(startDirtyPointer,startDirtyPointer->nextPageDirtyBit);
	}

	if(startDirtyPointer->isPageDirty == 1)
		forcePage(bm,page);
	if(status == RC_OK)
    {
		return(RC_OK);
    }
	else
    {
		return(RC_UNPIN_FAILED);
    }
}

RC FIFO(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum,SM_PageHandle pgHn)
{

	pageAndFixCount *fixCountStart = firstPageAndFixCountPtr;
	//getFrameNumber of firstNode in ramPagePtr;
	//go to that fixCountPtr whose frameNumber = above frameNumber,
	//and check its fix count for > 0.
	ramPageMap *begin =firstRamPageMapPtr;
	while(fixCountStart != NULL && fixCountStart->ramPageFrameNumber != begin->ramPageFrameNumber)
		fixCountStart = fixCountStart->nextPageFixCount;


	if(fixCountStart != NULL && fixCountStart->fixCount > 0) //means node should'nt be deleted...
	{
		assign_val(begin,firstRamPageMapPtr);
		int FrameNumberOfNewPage = begin->nextRamPageMap->ramPageFrameNumber;

		//make a new node ,add discPageDetails to it and make it last node

		//try writing this in a function...
		ramPageMap *currentRamPagePtr = (ramPageMap *)malloc(sizeof(ramPageMap));
		assign_val(currentRamPagePtr->discPageFrameNumber,pageNum);
		assign_val(currentRamPagePtr->ramPageFrameNumber,FrameNumberOfNewPage);
		assign_val(currentRamPagePtr->nextRamPageMap,begin->nextRamPageMap->nextRamPageMap);
		free(begin->nextRamPageMap);
		assign_val(begin->nextRamPageMap,currentRamPagePtr);

		//write to buffer..
		int counter = 0;
		frameList *beginFrame = firstFramePtr;
		while(counter < FrameNumberOfNewPage)
		{
			assign_val(beginFrame,beginFrame->nextFramePtr);
			counter++;
		}
		memory_set(beginFrame->frameData,'\0',PAGE_SIZE + 1);
		if(pgHn != NULL)
			string_copy(beginFrame->frameData,pgHn);
		assign_val(page->data,beginFrame->frameData);
		assign_val(page->pageNum,pageNum);
	}
		//get first node's frame Number;
	else
	{
		int frameNumberOfNewPage = firstRamPageMapPtr->ramPageFrameNumber;
		//remove the first node[START];
		ramPageMap *temp;
		assign_val(temp,firstRamPageMapPtr);
		assign_val(firstRamPageMapPtr,firstRamPageMapPtr->nextRamPageMap);
		releasing(temp);
		assign_val(temp,NULL);
		//remove the first node[END];

		//make a new node ,add discPageDetails to it and make it last node
		ramPageMap *currentRamPagePtr = (ramPageMap *)malloc(sizeof(ramPageMap));
		assign_val(currentRamPagePtr->discPageFrameNumber,pageNum);
		assign_val(currentRamPagePtr->ramPageFrameNumber,frameNumberOfNewPage);
		assign_val(currentRamPagePtr->nextRamPageMap,NULL);

		assign_val(temp,firstRamPageMapPtr);
		while(temp->nextRamPageMap != NULL)

		{
			assign_val(temp,temp->nextRamPageMap);
		}
		assign_val(temp->nextRamPageMap,currentRamPagePtr);

		//writing data to buffer
		int counter = 0;
		frameList *beginFrame = firstFramePtr;
		while(counter < frameNumberOfNewPage)
		{
			assign_val(beginFrame,beginFrame->nextFramePtr);
			counter++;
		}
		memory_set(beginFrame->frameData,'\0',PAGE_SIZE + 1);
		if(pgHn != NULL)
			string_copy(beginFrame->frameData,pgHn);
		assign_val(page->data,beginFrame->frameData);
		//
		assign_val(page->pageNum,pageNum);
	}
	return(RC_OK);
}
//All these are dummy methods. Required functionality has to be added later[START]

void attachAtEndOfList(ramPageMap *temp)
{
	ramPageMap *start = firstRamPageMapPtr;
	while(start->nextRamPageMap != NULL)
		assign_val(start,start->nextRamPageMap);
	assign_val(start->nextRamPageMap,temp);
}
void sortFixCounts(int *intArray, int size)
{
	char flag = 'Y';
	int j = 0;
	int temp;
	while (flag == 'Y')
	{
		flag = 'N';j++;int i;
		for (i = 0; i <size-j; i++)
		{
			if (intArray[i] > intArray[i+1])
			{
				assign_val(temp,intArray[i]);
				assign_val(intArray[i],intArray[i+1]);
				assign_val(intArray[i+1],temp);
				assign_val(flag ,'Y');
			}
		}
	}
}
void moveClockPtr()
{
	if(clockPtr->nextRamPageMap == NULL){
		assign_val(clockPtr,firstRamPageMapPtr);
    }
	else{
		assign_val(clockPtr,clockPtr->nextRamPageMap);
    }
}
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
	SM_PageHandle pgHn = (SM_PageHandle)malloc(PAGE_SIZE);


	//call ensure capacity.
	assure((pageNum + 1),&fHandle);

	PageNumber frameNumber = checkIfPagePresentInFramePageMaps(pageNum);
	if(frameNumber != RC_NO_FRAME)
	{
		assign_val(page->pageNum,frameNumber);
		getData(frameNumber,page);
		assign_val(page->pageNum,pageNum);
		if(bm->strategy == RS_LRU)
		{
			ramPageMap *temp = firstRamPageMapPtr;
			ramPageMap *prev = NULL;
			int counter = 0;

			while(temp !=NULL && temp->discPageFrameNumber != pageNum)
			{
				assign_val(prev,temp);
				assign_val(temp,temp->nextRamPageMap);
				counter ++;
			}

			if(temp != NULL)
			{
				if(counter == 0)
				{
					prev = firstRamPageMapPtr;
					assign_val(firstRamPageMapPtr,firstRamPageMapPtr->nextRamPageMap);
					assign_val(prev->nextRamPageMap,NULL);
					end_of_list(temp);
				}
				else
				{
					assign_val(prev->nextRamPageMap,temp->nextRamPageMap);
					assign_val(temp->nextRamPageMap,NULL);
					end_of_list(temp);
				}
			}

		}
		else if(bm->strategy == RS_CLOCK)
		{

			ramPageMap *start = firstRamPageMapPtr;
			while(frameNumber != start->ramPageFrameNumber)
			{
				assign_val(start,start->nextRamPageMap);
			}
			assign_val(start->clockReferenceBit,1);
		}
	}
	else
	{
		int freeframeNumber = - 99;
		getFirstFreeFrameNumber(&freeframeNumber,pageNum);
		reding_blc(pageNum, &fHandle,pgHn);
		numberOfReads++;
		if(freeframeNumber != -99)
		{
			//go to freeframeNumberTh frame and put pgHn contents onto it's frameData [START]

			int counter = 0;
			frameList *start = firstFramePtr;
			while(counter < freeframeNumber)
			{
				assign_val(start,start->nextFramePtr);
				counter++;
			}
			memory_set(start->frameData,'\0',PAGE_SIZE+1);
			if(pgHn != NULL)
				string_copy(start->frameData,pgHn);
			assign_val(page->data,start->frameData);
			assign_val(page->pageNum,pageNum);


			ramPageMap *begin = firstRamPageMapPtr;
			while(begin->ramPageFrameNumber != freeframeNumber)
			{
				assign_val(begin,begin->nextRamPageMap);
			}
			assign_val(begin->discPageFrameNumber,pageNum);
			//go to freeframeNumberTh [END]
			//bufferPoool[freeFrameNumber] = readBlock(i);
			if(bm->strategy == RS_CLOCK)
			{
				assign_val(clockPtr->clockReferenceBit,0);
				moveClockPtr();
			}

		}
		else
		{
			if(bm->strategy == RS_FIFO || bm->strategy == RS_LRU)
			{
				first_in_out(bm,page,pageNum,pgHn);
			}

			else if(bm->strategy == RS_LFU)
			{
				//get the frameNumber (f) which has the least fixCount
				//go to that f and
				//1.update fixCount to 0 in (FixCountPtr)
				//2.update discNum in (ramPageMap)
				pageAndFixCount *start = firstPageAndFixCountPtr;
				int sortedFixCountArray[bm->numPages];
				int index = 0;
				while(start != NULL)
				{
					assign_val(sortedFixCountArray[index++],start->fixCount);
					assign_val(start,start->nextPageFixCount);
				}
				sort(sortedFixCountArray,bm->numPages);

				start = firstPageAndFixCountPtr;
				while(start->fixCount != sortedFixCountArray[0])
				{
					assign_val(start,start->nextPageFixCount);
				}
				assign_val(start->fixCount,0);

				ramPageMap *tempRPM = firstRamPageMapPtr;
				while(tempRPM->ramPageFrameNumber != start->ramPageFrameNumber)
				{
					assign_val(tempRPM,tempRPM->nextRamPageMap);
				}
				assign_val(tempRPM->discPageFrameNumber,pageNum);
				assign_val(page->pageNum,pageNum);
			}
			else if(bm->strategy == RS_CLOCK)
			{
				while(clockPtr->clockReferenceBit == 1)
				{
					assign_val(clockPtr->clockReferenceBit,0);
					moveClockPtr();
				}
				assign_val(clockPtr->discPageFrameNumber,pageNum);
				moveClockPtr();
				assign_val(page->pageNum,pageNum);
			}
		}
		//if ramDisc has free nodes, get the first free node's frame number.
	}
	changeFixCount(1,pageNum);
	releasing(pgHn);
	assign_val(pgHn,NULL);
	return(RC_OK);
}
/***********************************************************************************************
Statistics Interface
Author Name: Purna Sahithi Adduri (A20416173)
Functions name: PageNumber *getFrameContents, *getDirtyFlags, *getFixCounts, getNumReadIO, getNumWriteIO
************************************************************************************************/

PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	ramPageMap *start = firstRamPageMapPtr;
	while(start != NULL)
	{
		assign_val(frameContentPtr[start->ramPageFrameNumber],start->discPageFrameNumber);
		assign_val(start,start->nextRamPageMap);
	}
	return(frameContentPtr);
}

bool *getDirtyFlags (BM_BufferPool *const bm)
{
	pageAndDirtyBitIndicator *start = firstPageAndDirtyBitMap;
	int i = 0;
	while(start != NULL)
	{
		if(start->isPageDirty == 1)
			dirtyBitPtr[i++] = true;
		else
			dirtyBitPtr[i++] = false;
		assign_val(start,start->nextPageDirtyBit);
	}
	return(dirtyBitPtr);
}
int *getFixCounts (BM_BufferPool *const bm)
{
	pageAndFixCount *start = firstPageAndFixCountPtr;
	int i = 0;
	while(start != NULL)
	{
		assign_val(fixCountPtr[i++],start->fixCount);
		assign_val(start,start->nextPageFixCount);
	}
	return(fixCountPtr);
}
RC shutdownBufferPool(BM_BufferPool *const bm)
{
	bool NonZeroFixIndicator = false;
	pageAndFixCount *start = firstPageAndFixCountPtr;
	while(start != NULL)
	{
		if(start->fixCount >0)
		{
			NonZeroFixIndicator = true;
			break;
		}
		assign_val(start,start->nextPageFixCount);
	}
	assign_val(start,NULL);

	if(NonZeroFixIndicator == false)
	{
		assign_val(clockPtr,NULL);
		flushing(bm);

		releasing(frameContentPtr);
		releasing(dirtyBitPtr);
		releasing(fixCountPtr);

		int counter = 0;
		ramPageMap *RPMtemp = NULL;
		pageAndDirtyBitIndicator *PADBItemp = NULL;
		pageAndFixCount *PAFCtemp = NULL;
		frameList *FLtemp = NULL;
		frameList *firstFramePtr = NULL;
		assign_val(firstFramePtr,(frameList*) bm->mgmtData);
		assign_val(bm->mgmtData,NULL);


		while(counter < bm->numPages)
		{

			assign_val(PAFCtemp,firstPageAndFixCountPtr);
			assign_val(firstPageAndFixCountPtr,firstPageAndFixCountPtr->nextPageFixCount);
			releasing(PAFCtemp);


			assign_val(RPMtemp,firstRamPageMapPtr);
			assign_val(firstRamPageMapPtr,firstRamPageMapPtr->nextRamPageMap);
			releasing(RPMtemp);

			assign_val(PADBItemp,firstPageAndDirtyBitMap);
			assign_val(firstPageAndDirtyBitMap,firstPageAndDirtyBitMap->nextPageDirtyBit);
			releasing(PADBItemp);

			assign_val(FLtemp,firstFramePtr);
			assign_val(firstFramePtr,firstFramePtr->nextFramePtr);
			releasing(FLtemp);

			counter++;
		}
		assign_val(firstPageAndFixCountPtr,NULL);
		assign_val(firstRamPageMapPtr,NULL);
		assign_val(firstPageAndDirtyBitMap,NULL);

		closePageFile(&fHandle);
	}
	if(firstPageAndFixCountPtr == NULL && firstRamPageMapPtr == NULL
	   && firstPageAndDirtyBitMap == NULL && bm->mgmtData ==NULL && NonZeroFixIndicator == false)
    {
		return(RC_OK);
    }
	else
    {
		return(RC_SHUT_DOWN_ERROR);
    }
}


int getNumWriteIO(BM_BufferPool *const bm)
{
	return(numberOfWrites);
}

int getNumReadIO(BM_BufferPool *const bm)
{
	return(numberOfReads);
}
RC forceFlushPool(BM_BufferPool *const bm)
{
	//If a frame is Dirty in DirtyBitIndicator,from BufferPool, write that frame onto disc.
	pageAndDirtyBitIndicator *dirtyStart = firstPageAndDirtyBitMap;
	frameList *frameStart = firstFramePtr;
	while (dirtyStart != NULL)
	{
		if(dirtyStart->isPageDirty == 1)
		{
			ramPageMap *ramPageStart = firstRamPageMapPtr;
			while(ramPageStart->ramPageFrameNumber != dirtyStart->ramPageFrameNumber)
			{
				ramPageStart = ramPageStart->nextRamPageMap;
			}
			writing_block (ramPageStart->discPageFrameNumber, &fHandle,frameStart->frameData);
			assign_val(dirtyStart->isPageDirty,0);
		}
		assign_val(dirtyStart,dirtyStart->nextPageDirtyBit);
		assign_val(frameStart,frameStart->nextFramePtr);
	}
	//If a page is dirty, write it back to the disc.
	return(RC_OK);
}
