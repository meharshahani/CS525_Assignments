#include "storage_mgr.h"
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include "dberror.h"

//defining a file pointer
FILE *file_ptr;
/*********************************************** MANIPULATING PAGE FILES ************************************************/

//initialising of the storage manager
void initStorageManager(void)
{
	printf("Storage manager has been successfully initialised.\n");
	printf("Now to create a page file.\n");
}

//creating the page file
extern RC createPageFile(char *fileName) {
	RC rc = -99; // return value.

	// setting up a file pointer with the mode "wb+"
	// If the file already exists, make it empty
	open_file(file_ptr,fileName,"wb+");

	// check the file pointer,if NULL then return RC_FILE_NOT_FOUND
	if (file_ptr == NULL) {
		return RC_FILE_NOT_FOUND;
	}

	//Allocating memory PAGE_SIZE elements,
	SM_PageHandle blankPage = (SM_PageHandle) calloc_allocation;

	//writing empty page in the file
	int write_page_status = fwrite(blankPage, sizeof(char), PAGE_SIZE, file_ptr);

	if (write_page_status == 0) {
		rc = RC_WRITE_FAILED;
	} else {
		rc = RC_OK;
	}

	//free blankPage
	free(blankPage);

	//close file
	int closing_the_file = fclose(file_ptr);

	if (closing_the_file == EOF) {
		rc = RC_CLOSE_FILE_FAILED;
	}

	return rc;
}

extern RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
	
	// Opens a file to update both reading and writing. 
	open_file(file_ptr,fileName,"r+");


	//checks status. if NULL or file doesnt't exist, returns the message RC_FILE_NOT_FOUND
	if (file_ptr == NULL) {
		return RC_FILE_NOT_FOUND;
	}

	// Calculating files total pages and placing the pointer to the last page.
	int positionOfSeek = fseek(file_ptr, 0, SEEK_END);

	if (positionOfSeek != 0) {
		return RC_SEEK_FILE_POSITION_ERROR;
	}

	//retriving last position of the file
	long last_position = ftell(file_ptr);

	if (last_position == -1) {
		return RC_SEEK_FILE_TAIL_ERROR;
	}

	int lenght_of_file = (int) last_position + 1;
	int totalNum = lenght_of_file / PAGE_SIZE;

	//changing position of the pointer to the start of the file
	positionOfSeek = fseek(file_ptr, 0, SEEK_SET);

	if (positionOfSeek != 0) {
		return RC_SEEK_FILE_POSITION_ERROR;
	}

	//initialing the file information to the fHandle
	fHandle->fileName = fileName;
	fHandle->totalNumPages = totalNum;
	fHandle->curPagePos = 0;
	fHandle->mgmtInfo = file_ptr;
    //return OK
	return RC_OK;
}

//this function is used to close the page file
extern RC closePageFile(SM_FileHandle *fHandle)
{
	printf("Closing the file\n");
	
	int close_file = fclose(fHandle->mgmtInfo);

	if(!close_file)
	{
		return RC_OK;
	}
	else
	{    
		return RC_FILE_NOT_FOUND;
	}
}
//this is used to delete the page file
extern RC destroyPageFile(char *fileName)
{
	//we will store the value of remove() in a variable and check whether the file was deleted or not
	int delete = remove(fileName);

	//remove() returns 0 if file was successfully deleted, now we will check that
	if(!delete)
	{
		return RC_OK;

	}
	else
	{
		return RC_FILE_NOT_FOUND;
	}
}


/******************************************** READING BLOCKS FROM DISK****************************************/

//read blocks from file and store content in memory
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	
	// check if the page number is valid
	// invalid page number includes pageNum + 1 > totalNumPage and pageNum < 0

	if (fHandle->totalNumPages < (pageNum + 1) || pageNum < 0) {
		return RC_READ_NON_EXISTING_PAGE;
	}

	// calculate the set from the starting point
	int set = sizeof(char) * (pageNum * PAGE_SIZE);

	// use set_success to check if the pointer is successfully set
	
	int set_success = seekSuccess(fHandle->mgmtInfo,set, SEEK_SET)

	if (set_success != 0) {
		return RC_SEEK_FILE_POSITION_ERROR;
	}

	// read_success is used to check if read successfully
	// if read_success != PAGE_SIZE, the read is not successful, return RC_READ_FAILED
	// else return RC_OK
	int read_success = read_file(memPage,PAGE_SIZE,fHandle->mgmtInfo);

	if (read_success != PAGE_SIZE) {
		return RC_READ_FILE_FAILED;
	}

	// set current page to the entered pageNum
	fHandle->curPagePos = pageNum;

	return RC_OK;
}

//gives current pointer position
extern RC getBlockPos(SM_FileHandle *fHandle)
{
	int currentPagePosition;
	currentPagePosition = fHandle -> curPagePos;
	return currentPagePosition;
}

//read first page from file 
extern RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(0,fHandle,memPage);
}

//read previous page from file
extern RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	//int previousPage = fHandle->curPagePos - 1;
	return readBlock(fHandle->curPagePos - 1,fHandle,memPage);
}

//read current page from file
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(fHandle->curPagePos, fHandle, memPage);
}

//read next page from file
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	//int currentPageNumber = getBlockPos(fHandle);
	//int nextPage = currentPageNumber + 1;
	return readBlock(fHandle->curPagePos+1,fHandle, memPage);
}

//read last page from file
extern RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	//int lastPage = ;
	return readBlock(fHandle -> totalNumPages, fHandle, memPage);
}


/*********************************************** Writing blocks to the page file ******************************************s*/

//to write a block of memory to page file
extern RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	// If pageNum < 0 or file's totalNumPages <= pageNum,
	// then the page cannot be written to the file.
	// Return RC_WRITE_FAILED.
	if (pageNum < 0 || fHandle->totalNumPages < (pageNum + 1)) {
		return RC_READ_NON_EXISTING_PAGE;
	}

	// Get the offset and seek the position in the file.
	// If the position that supposes to be the start point is not found,
	// then the page cannot be written to the file.
	// Return RC_SEEK_FILE_POSITION_ERROR
	int offset = pageNum * PAGE_SIZE * sizeof(char); // offset in the file from the absolute position

	int positionOfSeek = seekSuccess(fHandle->mgmtInfo,offset, SEEK_SET); // return label 

	if (positionOfSeek != 0) {
		return RC_SEEK_FILE_POSITION_ERROR;
	}

	// If the writing operation fails,page si not successfully return to the file.
	// Return RC_WRITE_FAILED
	
	int writeSize = fwrite(memPage, sizeof(char), PAGE_SIZE,fHandle->mgmtInfo); 

	if (writeSize != PAGE_SIZE) {
		return RC_WRITE_FAILED;
	}

	// set current position of the to PageNum
	fHandle->curPagePos = pageNum;

	return RC_OK;
}

//to writing current block of the memory to the page file
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return writeBlock (fHandle->curPagePos, fHandle, memPage);
}

extern RC appendEmptyBlock (SM_FileHandle *fHandle){
	// int seekDone;
	// size_t writeSizeofBlock;
 //    SM_PageHandle ptr;

 //     /* allocates memory and return a pointer to it */

 //    ptr = (char *) calloc_allocation;

 //    int set = (fHandle->totalNumPages + 1)*PAGE_SIZE*sizeof(char) ;

 //    //set pointer to end of file
 //    seekDone = seekSuccess(fHandle->mgmtInfo,set, SEEK_END); 


 //    if (seekDone == 0){
 //        writeSizeofBlock = fwrite(ptr, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);  //writes data from the memory block pointed by pointer to the file i.e last page is filled with zero bytes. 
 //        fHandle->totalNumPages = fHandle->totalNumPages + 1;
 //        fHandle->curPagePos = fHandle->totalNumPages;
	// 	rewind(fHandle->mgmtInfo);
	// 	fprintf(fHandle->mgmtInfo, "%d\n" , fHandle->totalNumPages); // update total number of pages
 //        seekSuccess(fHandle->mgmtInfo, set, SEEK_SET);
 //        free(ptr);
 //        return RC_OK;
	// }
	// else{
 //        free(ptr);
	// 	return RC_WRITE_FAILED;
	// }

	RC rc = -99; // return code, initialized as -99

	// file pointer to the file being handled
	FILE *fp = fHandle->mgmtInfo;

	if (fp == NULL) {
		return RC_FILE_NOT_FOUND;
	}

	// create an empty page
	SM_PageHandle emptyPage = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));

	// move position pointer to the end of the file
	int seekLabel = fseek(fp, 0L, SEEK_END);

	if (seekLabel != 0) {
		return RC_SEEK_FILE_POSITION_ERROR;
	}

	// write into the file, return the written size
	// TODO - what if only part of the stream is written to the file?
	int writtenSize = fwrite(emptyPage, sizeof(char), PAGE_SIZE,
			fHandle->mgmtInfo);

	if (writtenSize != PAGE_SIZE) {
		rc = RC_WRITE_FAILED;
	} else {
		fHandle->curPagePos = fHandle->totalNumPages++;
		rc = RC_OK;
	}

	// free heap memory
	free(emptyPage);

	return rc;

}

//to ensure that the file has a more number of pages than what is requested, less page then increase it
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
	// if(numberOfPages > fHandle->totalNumPages)
	// {
	// 	int a;
	// 	int extra_pages = numberOfPages - fHandle->totalNumPages;
	// 	for(a = 0; a< extra_pages; a++)
	// 	{
	// 		appendEmptyBlock(fHandle);
	// 	}
	// }
	// 	return RC_OK;

	if (fHandle->totalNumPages >= numberOfPages) {
		return RC_ENOUGH_PAGES;
	}
    
    // store totalNumPages to a local variable
    int totalNum = fHandle->totalNumPages;
    int i;
    
	for (i = 0; i < (numberOfPages - totalNum); i++) {
		RC rc = appendEmptyBlock(fHandle);

        // TODO - what should I do to clean the unfinished write of the empty block?
		if (rc != RC_OK) {
			return rc;
		}
	}
	return RC_OK;
}



