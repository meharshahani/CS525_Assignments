#ifndef STORAGE_MGR_H
#define STORAGE_MGR_H

#include "dberror.h"

/************************************************************
 *                    handle data structures                *
 ************************************************************/
typedef struct SM_FileHandle {
  char *fileName;
  int totalNumPages;
  int curPagePos;
  void *mgmtInfo;
} SM_FileHandle;

typedef char* SM_PageHandle;

/************************************************************
 *                    interface                             *
 ************************************************************/
/* manipulating page files */
extern void initStorageManager (void);
extern RC createPageFile (char *fileName);
extern RC openPageFile (char *fileName, SM_FileHandle *fHandle);
extern RC closePageFile (SM_FileHandle *fHandle);
extern RC destroyPageFile (char *fileName);

/* reading blocks from disc */
extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
extern int getBlockPos (SM_FileHandle *fHandle);
extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);

/* writing blocks to a page file */
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC appendEmptyBlock (SM_FileHandle *fHandle);
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle);

#endif

#define memory_set(a,c,b) memset(a,'\0',b);
#define assign_value(a,b) a=b;
#define string_concat(a,b) strcat(a,b);
#define getMetaData(iLoop, metaInformation,currentMetaKeyValue) getMeNthMetaData(iLoop, metaInformation,currentMetaKeyValue);
#define return(a) return a;
#define file_open(a,b) fopen(a,b);
#define file_close(a) fclose(a);
#define string_copy(a,b) strcpy(a,b);
#define string_comp(a,b) strcmp(a,b);
#define string_concate(a,b) strcat(a,b);
#define file_write(a,b,c,d) fwrite(a,b,c,d);
#define remove_file(a) remove(a);

