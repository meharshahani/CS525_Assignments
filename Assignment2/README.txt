Group26 - Disha Patel(A20452866)
          Mehar Shahani(A20439871)  
          Yash Raj(A20453633)

PROCEDURE TO RUN THE PROJECT:
1.Open the terminal and go to the project directory
		cd<project-path>

2.Enter 'make clean' to remove all the object files

3.Enter 'make' to compile and create the new object(.o) files

4.Enter 'make run' to run the files 

IT CONSISTS THE FOLLOWING FILES:

1. buffer_mgr.c
2. buffer_mgr.h
3. buffer_mgr_stat.c
4. buffer_mgr_stat.h
5. dberror.c
6. dberror.h
7. dt.h
8. storage_mgr.c
9. storage_mgr.h
10. test_assign2_1.c
11. test_assign2_2.c
12. test_helper.h
13. readme.txt


FUNCTIONS EXECUTED ARE:

    function name		     description

   	searchPage				Used to search the requested page in the Buffer Pool,
							if the requested page is found, load it into the BM_pageHandle and return RC_OK
							else, return error code
	
	appendPage()			Used to read the requested page from the disk and will append it to the end of the ListPage
	
	replacePage()			Used to replace the current page with the page which is requested from the disk
	
	FIFO()					Used as FIFO replacement strategy
	
	LRU()					Used as LRU replacement strategy
	
	CLOCK()					Used as CLOCK replacement strategy
	
	initPageList()			Used to initialize the ListPage to store pages in the Buffer Pool
	
	initBufferPool()		Used for initializing of the Buffer Pool
	
	shutdownBufferPool()	Used to shut down the Buffer Pool
	
	forceFlushPool()		Used to write the data back in the dirty pages in Buffer Pool
	
	pinPage()				Used to pin the page with the pageNum which is requested in the BUffer Pool
							If the page not found,then load the page from the file to the Buffer Pool
	
	markDirty()				Used to mark the requested page as dirty page
	
	unpinPage()				Used to unpin a page
	
	forcePage()				Used to write the requested page back to the page file on the disk
	
	getFrameContents()		Used to return an array of PageNumbers whose size is of pageNum and
							for represnting the empty page frame,use the constant namely NO_PAGE
	
	getDirtyFlags()			Used to return an array of bools whose size of numPages and
							then the empty page frames are considered as clean 
	
	getFixCounts()			Used to return an array of ints whose size is of numPages
	
	getNumReadIO()			Used to return the number of pages that we have read from disk
							since we initialized the Buffer Pool
	
	getNumWriteIO()			Used to return the count of pages written to the page file
							since we initialized Buffer Pool 

