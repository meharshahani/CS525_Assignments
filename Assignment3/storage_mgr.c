#include "storage_mgr.h"
#include <sys/stat.h>
#include <string.h>
#include <stdlib.h>
#include "test_helper.h"

//Global Data Structures [START]
typedef struct _metaDataList // data structure to metaData as a Linked List of Maps.
{
		char key[50];
		char value[50];
		struct _metaDataList *nextMetaDataNode;
} metaDataList;

metaDataList *firstNode = NULL;
metaDataList *currentNode = NULL;
metaDataList *previousNode = NULL;

int whoIsCallingCreate = 1; // 1--> test case calls, else -->any other function
//Global Data Structures [END]

/***********************************************************************************************
MANIPULATING PAGE FILES
Author Name: Purna Sahithi Adduri (A20416173)
Functions name: initStorageManager, createPageFile, openPageFile, closePageFile, destroyPageFile
************************************************************************************************/
void initStorageManager()
{

}


void getMeNthMetaData(int n, char * string,char *nthKeyValuePair)
{
	char newString[PAGE_SIZE];
	memory_set(newString,'\0',PAGE_SIZE);
	assign_value(newString[0],';');
	string_concat(newString, string);

	//store position of ; in arrays, say delimiterPosition [START]
	char delimiterPosition[1000];
	int iLoop;
	int delPostion = 0;

	for (iLoop = 0; iLoop < strlen(newString); iLoop++)
	{
		if (newString[iLoop] == ';')
		{
			assign_value(delimiterPosition[delPostion],iLoop);
			delPostion++;
		}
	}
	//store position of ; in arrays, say delimiterPosition [END]

	int currentPos = 0;
	for (iLoop = delimiterPosition[n - 1] + 1;
			iLoop <= delimiterPosition[n] - 1; iLoop++)
	{
		assign_value(nthKeyValuePair[currentPos],newString[iLoop]);
		currentPos++;
	}
	assign_value(nthKeyValuePair[currentPos] ,'\0');
}




metaDataList * constructMetaDataLinkedList(char *metaInformation,
		int noOfNodesToBeConstructed)
{
	int iLoop;
	char currentMetaKeyValue[100];

	char currentKey[50];
	memory_set(currentKey,'\0',50);

	char currentValue[50];
	memory_set(currentValue,'\0',50);

	for (iLoop = 1; iLoop <= noOfNodesToBeConstructed; iLoop++)
	{
		memory_set(currentMetaKeyValue,'\0',100);
		getMetaData(iLoop, metaInformation,currentMetaKeyValue);
        char colonFound;
        int keyCounter,ValueCounter;
        assign_value(colonFound,'N');
        assign_value(keyCounter,0);
        assign_value(ValueCounter,0);
		
		int i;
		for (i = 0; i < strlen(currentMetaKeyValue); i++)
		{
			if (currentMetaKeyValue[i] == ':')
				assign_value(colonFound,'Y');

			if (colonFound == 'N')
				currentKey[keyCounter++] = currentMetaKeyValue[i];
			else if (currentMetaKeyValue[i] != ':')
				currentValue[ValueCounter++]=currentMetaKeyValue[i];
		}
		assign_value(currentKey[keyCounter],'\0');
		assign_value(currentValue[ValueCounter],'\0');

		assign_value(currentNode,(metaDataList *) malloc(sizeof(metaDataList)));

		string_copy(currentNode->value,currentValue);
		string_copy(currentNode->key,currentKey);
		currentNode->nextMetaDataNode = NULL;

		if (iLoop == 1)
		{
			firstNode= currentNode;
			previousNode = NULL;
		}
		else
		{
			previousNode->nextMetaDataNode = currentNode;
		}
		assign_value(previousNode,currentNode);
	}
	return(firstNode);
}

RC createPageFile(char *filename)
{
	FILE *open;
	open = file_open(filename, "a+b"); // create and open the file for read/write

	if (whoIsCallingCreate == 1) // If Test case calls this function, reserve 3 blocks
	{
		if (open != NULL)
		{
			char nullString2[PAGE_SIZE]; // 2nd metaBlock
			char nullString3[PAGE_SIZE]; // actual Data Block

			//to Store PageSize in string format.[start]
			char stringPageSize[5];
			sprintf(stringPageSize, "%d", PAGE_SIZE);

			char strMetaInfo[PAGE_SIZE * 2];
			string_copy(strMetaInfo, "PS:"); // PS == PageSize
			string_concate(strMetaInfo, stringPageSize);
			string_concate(strMetaInfo, ";");
			string_concate(strMetaInfo, "NP:0;"); //NP == No of Pages
			//to Store PageSize in string format.[end]

			int i;
			for (i = strlen(strMetaInfo); i < (PAGE_SIZE * 2); i++)
				strMetaInfo[i] = '\0';
			memory_set(nullString2, '\0', PAGE_SIZE);
			memory_set(nullString3, '\0', PAGE_SIZE);

			file_write(strMetaInfo, PAGE_SIZE, 1, open);
			file_write(nullString2, PAGE_SIZE, 1, open);
			file_write(nullString3, PAGE_SIZE, 1, open);

			file_close(open);
			return(RC_OK);
		} else
		{
			return(RC_FILE_NOT_FOUND);
		}
	}
	else
	{
		if (open != NULL)
		{
			char nullString[PAGE_SIZE];

			memory_set(nullString, '\0', PAGE_SIZE);
			file_write(nullString, PAGE_SIZE, 1, open);

			file_close(open);
			return(RC_OK);
		} else
		{
			return(RC_FILE_NOT_FOUND);
		}
	}

}


RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
	struct stat statistics;
	FILE *open;

	open = file_open(fileName, "r");
	if (open != NULL)
	{
		//Initialize our structure with required values.
		fHandle->fileName = fileName;
		fHandle->curPagePos = 0;
		stat(fileName, &statistics);
		fHandle->totalNumPages = (int) statistics.st_size / PAGE_SIZE;
		fHandle->totalNumPages -= 2; // 2 pages are reserved for metaInfo. Hence Subtracting.

		//Read MetaData Information and dump it into a Linked List [START]
		char metaDataInformationString[PAGE_SIZE * 2];
		fgets(metaDataInformationString, (PAGE_SIZE * 2), open);
		//Read MetaData Information and dump it into a String [END]

		//Count the number of metaData Nodes to be constructed [START]
		int iLoop;
		int noOfNodes = 0;
		for (iLoop = 0; iLoop < strlen(metaDataInformationString); iLoop++)
			if (metaDataInformationString[iLoop] == ';')
				noOfNodes++;
		//Count the number of metaData Nodes to be constructed [END]

		fHandle->mgmtInfo = constructMetaDataLinkedList(
				metaDataInformationString, noOfNodes);
		//This fileHandle now has all the metaInfo needed.. Phew !

		//Read MetaData Information and dump it into a Linked List [END]
		file_close(open);

		return(RC_OK);
	}
	else
	{
		return(RC_FILE_NOT_FOUND);
	}
}


void convertToString(int someNumber,char * reversedArray)
{
	char array[4];
	memory_set(array, '\0', 4);
	int i = 0;
	while (someNumber != 0)
	{
		array[i++] = (someNumber % 10) + '0';
		someNumber /= 10;
	}
	array[i] = '\0';

	//char reversedArray[4];
	int j=0;
	int x;
	for(x = strlen(array)-1;x>=0;x--)
	{
		reversedArray[j++] = array[x];
	}
	reversedArray[j]='\0';
}


RC writeMetaListOntoFile(SM_FileHandle *fHandle,char *dataToBeWritten)
{
	FILE *open = file_open(fHandle->fileName,"r+b");

	if(open != NULL)
	{
		file_write(dataToBeWritten,1,PAGE_SIZE,open);
		file_close(open);
		return(RC_OK);
	}
	else
	{
		return(RC_WRITE_FAILED);
	}
}


void freeMemory()
{
	metaDataList *previousNode;
	metaDataList *current  = firstNode;
	previousNode = firstNode;
	while(current != NULL)
	{
		current = current->nextMetaDataNode;
		if(previousNode!=NULL)
			free(previousNode);
		previousNode = current;
	}
	previousNode = NULL;
	firstNode = NULL;
}


RC closePageFile(SM_FileHandle *fHandle)
{
	if (fHandle != NULL)
	{
		//update the NP to totalPages.
		//write LL to Disk.
		metaDataList *temp = firstNode;
		char string[4];
		memory_set(string,'\0',4);
		while (1 == 1)
		{
			if(temp != NULL)
			{
				if(temp->key != NULL)
				{
					if (strcmp(temp->key, "NP") == 0)
					{
						convertToString(fHandle->totalNumPages,string);
						string_copy(temp->value,string);
						break;
					}
				}
				temp = temp->nextMetaDataNode;
			}
			else
				break;
		}
		temp = firstNode;

		char metaData[2 * PAGE_SIZE];
		memory_set(metaData, '\0', 2 * PAGE_SIZE);
		int i = 0;
		while (temp != NULL)
		{
			int keyCounter = 0;
			int valueCounter = 0;
			while (temp->key[keyCounter] != '\0')
				metaData[i++] = temp->key[keyCounter++];
			metaData[i++] = ':';
			while (temp->value[valueCounter] != '\0')
				metaData[i++] = temp->value[valueCounter++];
			metaData[i++] = ';';
			temp = temp->nextMetaDataNode;
		}
		writeMetaListOntoFile(fHandle,metaData);
		fHandle->curPagePos = 0;
		fHandle->fileName = NULL;
		fHandle->mgmtInfo = NULL;
		fHandle->totalNumPages = 0;
		fHandle = NULL;
		//free(temp); commented by karthik
		freeMemory();
		return(RC_OK);
	}
	else
	{
		return(RC_FILE_HANDLE_NOT_INIT);
	}
}


RC destroyPageFile(char *fileName)
{
	if (remove(fileName) == 0)
		return RC_OK;
	else
		return RC_FILE_NOT_FOUND;
}
/******************************************************************************************
WRITING BLOCKS TO A PAGE
Author: Vinay krishna nelly (A20428194)
Functions name: writeBlock, writeCurrentBlock, appendEmptyBlock, ensureCapacity
*******************************************************************************************/

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	//get all the contents of memPage to a variable.
	// if   	PageNum <0 || pageNum >count ; invalid page Num ; return -1
	// else		write to file.

	if (pageNum < 0 || pageNum > fHandle->totalNumPages)//
		return RC_WRITE_FAILED;
	else
	{
		int startPosition = (pageNum * PAGE_SIZE) + (2 * PAGE_SIZE);

		FILE *open = file_open(fHandle->fileName, "r+b");
		if (open != NULL)
		{
			if (fseek(open, startPosition, SEEK_SET) == 0)
			{
				file_write(memPage, 1, PAGE_SIZE, open);
				if (pageNum > fHandle->curPagePos)
					fHandle->totalNumPages++;
				fHandle->curPagePos = pageNum;
				file_close(open);
				return(RC_OK);
			}
			else
			{
				return(RC_WRITE_FAILED);
			}
		} else
		{
			return(RC_FILE_HANDLE_NOT_INIT);
		}
	}
}



RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (writeBlock(fHandle->curPagePos, fHandle, memPage) == RC_OK)
    {
		return(RC_OK);
    }
	else
    {
		return(RC_WRITE_FAILED);
    }
}


RC appendEmptyBlock(SM_FileHandle *fHandle)
{
	whoIsCallingCreate = 2;
	if (createPageFile(fHandle->fileName) == RC_OK)
	{
		//Changing the value of totalNumPages and curPagePos as we are adding new blocks
		fHandle->totalNumPages++;
		fHandle->curPagePos = fHandle->totalNumPages - 1;
		whoIsCallingCreate = 1;
		return(RC_OK);
	}
	else
	{
		whoIsCallingCreate = 1;
		return(RC_WRITE_FAILED);
	}
}


RC ensureCapacity(int numberOopenages, SM_FileHandle *fHandle)
{
	int extraPagesToBeAdded = numberOopenages - (fHandle->totalNumPages);
	int iLoop;
	if (extraPagesToBeAdded > 0)
	{
		for (iLoop = 0; iLoop < extraPagesToBeAdded; iLoop++)
		{
			whoIsCallingCreate = 3;
			createPageFile(fHandle->fileName);
			//Changing the value of totalNumPages and curPagePos as we are adding new blocks
			whoIsCallingCreate = 1;
			fHandle->totalNumPages++;
			fHandle->curPagePos = fHandle->totalNumPages - 1;
		}
		return(RC_OK);
	} else
		return(RC_READ_NON_EXISTING_PAGE); // this is the closest macro i could fit in here..
}
/******************************************************************************************
READING BLOCKS FROM DISK
Aswini Anki Reddy (A20422757)
Functions name: readblock, getBlockPos, readFirstBlock, readPreviousBlock, readCurrentBlock, readNextBlock, readLastBlock
********************************************************************************************/
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	//if pageNum is n, read n+2th page.
	if (fHandle->totalNumPages < pageNum)
	{
		return RC_READ_NON_EXISTING_PAGE;
	}
	else
	{
		FILE *open;
		open = file_open(fHandle->fileName, "r");

		if (open != NULL)
		{
			if (fseek(open, ((pageNum * PAGE_SIZE) + 2 * PAGE_SIZE), SEEK_SET)== 0)
			{
				fread(memPage, PAGE_SIZE, 1, open);
				fHandle->curPagePos = pageNum;
				file_close(open);
				return(RC_OK);
			}
			else
			{
				return(RC_READ_NON_EXISTING_PAGE);
			}
		} else
		{
			return(RC_FILE_NOT_FOUND);
		}
	}
}


int getBlockPos(SM_FileHandle *fHandle)
{
	return fHandle->curPagePos;
}


RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (readBlock(0, fHandle, memPage) == RC_OK)
    {
		return(RC_OK);
    }
	else
    {
		return(RC_READ_NON_EXISTING_PAGE);
    }
}


RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (readBlock(getBlockPos(fHandle) - 1, fHandle, memPage) == RC_OK)
    {
		return(RC_OK);
    }
	else
    {
		return(RC_READ_NON_EXISTING_PAGE);
    }
}


RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (readBlock(getBlockPos(fHandle), fHandle, memPage) == RC_OK)
    {
		return(RC_OK);
    }
	else
    {
		return(RC_READ_NON_EXISTING_PAGE);
    }
}


RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (readBlock(getBlockPos(fHandle) + 1, fHandle, memPage) == RC_OK)
    {
		return(RC_OK);
    }
	else
    {
		return(RC_READ_NON_EXISTING_PAGE);
    }
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (readBlock(fHandle->totalNumPages - 1, fHandle, memPage) == RC_OK)
    {
		return(RC_OK);
    }
	else
    {
		return(RC_READ_NON_EXISTING_PAGE);
    }
}

