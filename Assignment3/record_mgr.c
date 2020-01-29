// #include "tables.h"

#include "tables.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <stdlib.h>
#include <string.h>



int list[100];//100 is biggest numInserts Value
int record = 0;
SM_FileHandle handle;

extern RC isNull(Record **Record)
{
     if(record != NULL)
         return RC_OK;
     else
         return RC_CREATE_FAILED;
}

extern RC toCheck(Record **record,Value *value)
{
	char * colVal = serializeValue(value);
		 if(colVal != NULL)
		 	return RC_OK;
	 	else
		 	return RC_SET_ATTR_FAILED;
}

void fetchData(int columnNum,char * record,int *semWhere,char * cellValue)
{
	int begin;

	if(columnNum != 0)
		begin = semWhere[columnNum - 1] + 1;
	else
		begin = 0;

	int i;
	int j;
    var_assign(j,0);
	for(i = begin ; i < semWhere[columnNum];i++)
		cellValue[j++] = record[i];

}
/***********************************************************************************************
Table and manager
************************************************************************************************/

//initialise the record manager
RC initRecordManager (void *mgmtData)
{
	//initialise a variable index with 0
	int index = 0;

	//execute the following code 
	do
	{
		//assign the value to the list[index]
		var_assign(list[index],-99);

		//increment index
		index++;
	}
	//until the condition is met
	while(index < 100);

    return(RC_OK);
}

//creating the table
RC createTable (char *name, Schema *schema)
{
	//page handle
    SM_PageHandle pgHn;
    
    //allocate malloc memory of the page size to the page handle
    pgHn = (SM_PageHandle) malloc(PAGE_SIZE);
    

    //new file with file name of the table
    createFile(name);       

    //open the file                
    openFile(name,handle); 

    //set the memory of the page handle         
    mem_record(pgHn,b,PAGE_SIZE);

    //copy the string 
    cpy_str(pgHn,schema); 

    //write the block that starts from 0 to the page handle 
    writeBlock (0, &handle, pgHn);

    mem_record(pgHn,b,PAGE_SIZE);
    writeBlock (1, &handle, pgHn);

    //free space of the page handle
    free(pgHn);
    return(RC_OK);
}

//open the table which we created above
RC openTable (RM_TableData *rel, char *name)
{
	//allocate the malloc size of the schema to the Schema and assign that value to the schema of the rel
    var_assign(rel->schema,(Schema *)malloc(sizeof(Schema)));

    //allocate the size of char times 100 to the name field of the rel
     var_assign(rel->name,(char *)malloc(sizeof(char)*100));   

    SM_PageHandle pgHn;

    //allocate the malloc of the page size to the page handle
    var_assign(pgHn,(SM_PageHandle) malloc(PAGE_SIZE));

    //check if the filename of the handle to null
    if(handle.fileName == NULL)
    {
    	//if the condition is true then open the page 
    	openFile2(rel,name,handle);
    }
 
 	//copy the string name to name field of the rel
    cpy_str2(rel,name);

    //read the block
    blockRead(x,y,z);

    //deserialise the data in the page handle
    deserialize(pgHn,rel,schema);

    //assign the value NULL to the management data field of the relation
    val_assign(rel,mgmtData,NULL);

    //free the space occupied of the page handle
    free(pgHn);

    //check if the schema field of the relation is NULL and the relation is NULL 
    if(rel->schema != NULL && rel != NULL)        
    {
    	//if condition is true then return RC_OK
    	return RC_OK;
    }
    else      
    {
    	//otherwise return that the table opening has failed
    	return RC_OPEN_TABLE_FAILED;
    }

	//close the page file
	shutdownFile(handle);
	return(RC_OK);
}

//shutdown the record manager
RC shutdownRecordManager ()
{
    return(RC_OK);
}

//delete the table by detroying the page file
RC deleteTable (char *name)
{
    remove(name);
    return(RC_OK);
}

//close the table 
RC closeTable (RM_TableData *rel)
{
	char blkPgList[PAGE_SIZE];

	//set the memory of PAGESIZE to the blkPgList
    mem_record(blkPgList,b,PAGE_SIZE);

	SM_PageHandle pgHn;

	//assign the value of malloc(PAGESIZE) to the page handle
    var_assign(pgHn,(SM_PageHandle) malloc(PAGE_SIZE));

    //read the block
    blockRead1(x,y,z);

    //copy the string to page handle
    cpy_str3(blkPgList,pgHn);
	
	int j =0;

	char blkPage[10];

	//set the memory of the blank page 
    mem_record(blkPage,b,10);

    char blkStr[PAGE_SIZE];
    mem_record(blkStr,b,PAGE_SIZE);

	do
	{
		sprintf(j,"%d",(list[j]));
        cpy_str3(blkPgList,blkPage);
        cpy_str3(blkPgList,";");

        //write to the block 
        blk_write(a,handle,blkPgList);

        //the deleted block will be written
		blk_write2(list[j]+2,handle,blkStr);

		//set the memory of the block page
        mem_record(blkPage,b,10);

        j++;
	}
	while(list[j] != -99);

	//close the the handle
	shutdownFile(handle);

	//free space fpr the schema and name field of the relation
    free(rel->schema);
    free(rel->name);
    return(RC_OK);
}

//get the number if tuples in the record
int getNumTuples (RM_TableData *rel)
{
	//initialise two variables pgCount and index
    int pgCount = 0;
    int index = 0;

    SM_PageHandle pgHn;

    //assign the value of the malloc(PAGESIZE) to the page handle
    var_assign(pgHn,(SM_PageHandle) malloc(PAGE_SIZE));

    //read the block from the page handle from 1
    blockRead1(a,handle,pgHn);
   
    do
    {
    	//check to see of the index of the page handle is equal to ;
    	 if (pgHn[index] == ';')
    	 {
    	 	//if condition is true then increment page count
             pgCount ++;
    	 }
    	 index++;
    }
    //continue executing the code until  the condition is true
    while(pgHn[index] != NULL);

    //close the file handle
    shutdownFile(handle);

    //open the page file and return the value
    openFile(rel->name,handle);
    return(handle.totalNumPages - pgCount);
}



/******************************************************************************************
Dealing with schemas
********************************************************************************************/

//free the space occupied by the schema
RC freeSchema (Schema *schema)
{
	//use free() to free space of the schema
    free(schema);  
    return(RC_OK);
}

//this function will create the schema 
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
	//allocate the size of malloc of the schema 
	Schema *schema = (Schema *)malloc(sizeof(Schema));

	//assign the variable values to the field of the schemas
	var_assign(schema->dataTypes,dataTypes);
	var_assign(schema->numAttr,numAttr);
	var_assign(schema->attrNames,attrNames);
	var_assign(schema->keySize,keySize);
	var_assign(schema->keyAttrs,keys);
	var_assign(schema->typeLength,typeLength);	

	return(schema);
}

//this function is used to get the record size of the schema
int getRecordSize (Schema *schema)
{
	int i=0;
    int size;

    //assiggn the value 0 to the variavble size
    var_assign(size,0);

    //execute the following code until the 
    do 
    {	//switch the case and the appropriate case will be executed
    	switch(schema->dataTypes[i]) 
        {
        	//if the data type is float
        	case DT_FLOAT:
                size += sizeof(float);
                break;
            //if the data type is int
            case DT_INT:
                size += sizeof(int);
                break;
            //if the data type is string
            case DT_STRING:
                size += schema->typeLength[i];
                break;
            //if the data type is bool
            case DT_BOOL:
                size += sizeof(bool);
                break;
        }
    }
    //check if this condition is met or not
    while(i < schema->numAttr);

    //assign the value of 8 to the size variable
    var_assign(size,8);
    return(size);
}

/******************************************************************************************
Handling records in a table
*******************************************************************************************/

RC insertRecord (RM_TableData *rel, Record *record)
{
	if(handle.fileName == NULL)
	{
        openFile(rel->name,handle);
	}

	int recNum;
    char *strWrite = (char *)malloc(sizeof(char) *PAGE_SIZE);
    mem_record(strWrite,b,PAGE_SIZE);
    cpy_str3(strWrite,record->data);

    char blkPgList[PAGE_SIZE];
 mem_record(blkPgList,b,PAGE_SIZE);
	SM_PageHandle pgHn;
     var_assign(pgHn,(SM_PageHandle) malloc(PAGE_SIZE));
    blockRead3(1,&handle,pgHn);

    //copy string to blkpglist and then free space
	cpy_str3(blkPgList,pgHn);
    free(pgHn);

	char isFree = 'N';
   
	int x;
	if(blkPgList != NULL)
	{

	do
	{
		//checks if blkpglist has encountered a ;
		if(blkPgList[x] == ';')
			{
                var_assign(isFree,'Y');
				break;
			}
			x++;
	}
	while(x < strlen(blkPgList));
	}

	if(isFree == 'Y')
	{
		char pgFree[10];

		//set the memory of the pgFree to 10 starting from 0
		memset(pgFree,'\0',10);
		int x = 0;
		do
		{
			pgFree[x] = blkPgList[x];
		}
		while(blkPgList[x] != ';');

		int y = atoi(pgFree) + 2;

		//write the block to the memory and then decrement the number of pages
		writeBlock(y,&handle,strWrite);
		handle.totalNumPages--; 


		int i = 0;

		//take a new pagelist of char type and allocate malloc page size
		//then set its memory to PAGESIZE
		char *newblkPgList = (SM_PageHandle) malloc(PAGE_SIZE);
        mem_record(newblkPgList,b,PAGE_SIZE);

        x = (strlen(pgFree) + 1);
        do
        {
        	newblkPgList[i++] = blkPgList[x];
        	x++;
        }
        while(x < strlen(blkPgList));

        blk_write2(1,handle,newblkPgList);
		free(newblkPgList);
		recNum = atoi(pgFree);
	}
	else
	{
        blk_write2(handle.totalNumPages,handle,strWrite);
		var_assign(recNum,handle.totalNumPages - 3);
        
	}

	//assign the variables page and slot to the values and then free the space for the strWrite
	var_assign(record->id.page,recNum);
	var_assign(record->id.slot,-99); 

    free(strWrite);


    return(RC_OK);


}


//updating the record
RC updateRecord (RM_TableData *rel, Record *record)
{
    openFile(rel->name,handle);

    //write the file to the memory and then close the file
    blk_write2(record->id.page + 2,handle,record->data);
    shutdownFile(handle);

    //return ok if successful
    return(RC_OK);
}

//getting the record
RC getRecord (RM_TableData *rel, RID id, Record *record)
{
	//take a page handle and file handle type of variable
    SM_PageHandle pgHn;

    SM_FileHandle fh;

    //open the file and then assign the value of the malloc of the page size to the page handle
    openFile(rel->name,fh);
    var_assign(pgHn,(SM_PageHandle) malloc(PAGE_SIZE));
    
	mem_record(pgHn,b,PAGE_SIZE);
    blockRead3(id.page + 2, &fh, pgHn);
	var_assign(record->id.page,id.page);
    cpy_str3(record->data, pgHn);
    shutdownFile(fh);
    free(pgHn);

    return(RC_OK);
}

//deleting thre record
RC deleteRecord (RM_TableData *rel, RID id)
{
	int i = 0 ;

	//loop through the list until the end is reached
	for(;list[i] != -99;i++)
		//then assign the value to the list with the particular index
		var_assign(list[i],id.page);

    return(RC_OK);
}
/******************************************************************************************
Scans Functions
*******************************************************************************************/

RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    var_assign(scan->rel,rel);
    var_assign(scan->mgmtData,cond);
    return(RC_OK);
}


void storeSemiColonPostion(char * record,int *p)
{
	int i;
	int j;
    var_assign(j,0);
	for(i = 0 ; i < strlen(record); i++)
		if(record[i] == ';')
			p[j++] = i;
}



RC next (RM_ScanHandle *scan, Record *record)
{
	if(handle.fileName == NULL)
		//openPageFile(scan->rel->name,&handle);
    openFile(scan->rel->name,handle)

	Expr *expression = (Expr *) scan->mgmtData;
	while(record < handle.totalNumPages - 2)
	{
		SM_PageHandle pgHn;
        var_assign(pgHn,(SM_PageHandle) malloc(PAGE_SIZE));
		mem_record(pgHn,'\0',PAGE_SIZE);

		//read the file from the block
		blockRead3(record+2,&handle,pgHn);

		char cellValue[PAGE_SIZE];
		mem_record(cellValue,'\0',PAGE_SIZE);

		int semWhere[3];  //scan->rel->schema->numAttr
		colPlace(pgHn,semWhere);

		if(expression->expr.op->type == OP_BOOL_NOT)
		{
			//fetch the data that was in the column
			fetchData(expression->expr.op->args[0]->expr.op->args[0]->expr.attrRef,pgHn,semWhere,cellValue);

			//check to see if the data is <= atoi(cellValue)
			if(expression->expr.op->args[0]->expr.op[0].args[1]->expr.cons->v.intV <= atoi(cellValue))
			{
				//if it true then copy string 
				cpy_str3(record->data,pgHn);

				//increment record
				record++;

				//free the page handle
				free(pgHn);
				return(RC_OK);
			}

		}
		else if(expression->expr.op->type == OP_COMP_EQUAL)
		{
			//fetch the data that was in the column
			fetchData(expression->expr.op->args[1]->expr.attrRef,pgHn,semWhere,cellValue);
			if(expression->expr.op->args[0]->expr.cons->dt == DT_INT)
			{
				if(atoi(cellValue) == expression->expr.op->args[0]->expr.cons->v.intV)
				{
					//if it true then copy string 
					cpy_str3(record->data,pgHn);

					//increment record
					record++;

					//free the page handle
					free(pgHn);
					return(RC_OK);
				}
			}
			else if(expression->expr.op->args[0]->expr.cons->dt == DT_STRING)
			{
				if(strcmp(cellValue,expression->expr.op->args[0]->expr.cons->v.stringV) == 0)
				{
					//if it true then copy string 
					cpy_str3(record->data,pgHn);

					//increment record
					record++;

					//free the page handle
					free(pgHn);
					return(RC_OK);
				}
			}
		}
		record++;
		free(pgHn);
	}
	var_assign(record,0); 
	return(RC_RM_NO_MORE_TUPLES);
}

//closing the scan
RC closeScan (RM_ScanHandle *scan)
{
    return(RC_OK);
}


/******************************************************************************************
Dealing with records and attribute values
********************************************************************************************/

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
	

	//take a char array called now of the size of the page
    char now[PAGE_SIZE + 1];

    //initialise 2 counters
    int nowCount = 1;
    int count = 0;

  //set the memory of the PAGESIZE + 1 to the counter and set it to \0 which means null
	mem_record(now,'\0',PAGE_SIZE+1);

	//allocate memory using malloc to the value attribute
    *value = (Value *)malloc(sizeof(Value) * schema->numAttr);
    
    //we will now initialise a looping variable and loop through a do-while to check for a switch case
    int index = 0;

    //we will execute the code inside do until the index variable is greater than or equal to PAGE_SIZE
    //that means we will stop when end of file is reached
    do
    {
    	//we will check is the data in the record is equal to ";" which means end of file has been reached
    	//or if the data in the record encounters a '\0' which is a null character
    	 if ((record->data[index] != ';') || (record->data[index] != '\0'))
        {
        	//if the condition is false then we keep moving forward by incrementing the counter
            var_assign(now[nowCount ++],record->data[index]);
        }
        else
        {
        	//if the number of attribute is equal to the counter
             if (attrNum == count)
             {
             	int c = 0;
             	c = schema->dataTypes[count];
             	//we will take a switch case and will execute the case based on the data type of the value in the 0th index of the counter
             	//we will store the initial of the datatype in the 0 index of the counter
                switch (c) 
                {
                	//if the value is of type bool

                	case DT_BOOL:
                        now[0] = 'b';
                        break;
                   //if the value is of type int
                    case DT_INT:
                        now[0] = 'i';
                        break;
                    //if the value is of type string
                    case DT_STRING:
                        now[0] = 's';
                        break;
                    //if the value is of type float
                    case DT_FLOAT:
                        now[0] = 'f';
                        break;                       
                }

                //we will now convert the particular data in the counter to a string and store it in the Value pointer
                *value = stringToValue(now);
                break;
            }

            //we increment the count and assign the value 1 to the nowCount counter
            count ++;
            var_assign(nowCount,1);

            //we will set the memory of the pagesize 
            mem_record(now,'\0',PAGE_SIZE+1);
        }
        //increment index and loop through the do-while
        index++;
    }
    //loop through the do{} until this condition is defied
    while(index < PAGE_SIZE);
    return(RC_OK);
}


//creating a record
RC createRecord (Record **record, Schema *schema)
{

    //we allocate malloc of the size of record to the record pointer
	*record = (Record *)malloc(sizeof(Record));

    //in the data attribute of the record we allocate memory of the page size
	 (*record)->data = (char *)malloc(PAGE_SIZE);
    
    //set memory
    mem_record((*record)->data,'\0',PAGE_SIZE);

    //check to see if the record is empty
    //if it is then we return that the createRecord function failed
	isNull(record);
}


//sets the attribute values 
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{

	//take two counter type variables and set the semCount which keeps the track of the number of semicolons to 0
	 int semCount,i;
    var_assign(semCount,0);


    //take a char pointer which stores the value of the serialised data
    char * colVal = serializeValue(value);


    i = 0;

    //loop through the length of the data in the record 
	while(i < strlen(record->data))
	{
		//check if it encounters a ; and increment the count if it does
		if(record->data[i] == ';')
			semCount++;
	}

	//check if the semcount is equal to the number of attributes
	 if(semCount == schema->numAttr)
	 {
		 int semWhere[schema->numAttr];

		 int j ;
         var_assign(j,0);

		while(i < strlen(record->data))
		{
			//check if it encounters a ; and increment 
			if(record->data[i] == ';')
				//assign value of i to it if so
				var_assign(semWhere[j++],i);
		}

		 if( attrNum == 0)
		 {
			 char strLast[PAGE_SIZE];
             mem_record(strLast,'\0',PAGE_SIZE);
			 i = 0;
			 int j;

			 //loop through the positions of semicolon and assign the value of data in the record to the last string
			 for(j = semWhere[attrNum] ; j < strlen(record->data) ; j++ )
			 {
				 var_assign(strLast[i++],record->data[j]);
			 }
			 var_assign(strLast[i],'\0');

			 mem_record(record->data,'\0',PAGE_SIZE);

			 //copy the string colVal to the data in the record
			 cpy_str3(record->data,colVal);

			 //copy the string strLast to the data in the record
			 cpy_str3(record->data,strLast);
		 }
		 else
		 {
			 char strStart[PAGE_SIZE];
			 char strLast[PAGE_SIZE];

			 mem_record(strStart,'\0',PAGE_SIZE);
			 mem_record(strLast,'\0',PAGE_SIZE);

			//this nested for loop will run from 0  to the position of the semicolon and then assign value
			 //of the data in the record to the strLast and strStart respectively
			 for(i = 0; i <= semWhere[attrNum - 1]; i++)
			 {
			 	for(i = semWhere[attrNum] ; i < strlen(record->data) ; i++)
			 	{
			 		 var_assign(strLast[j++],record->data[i]);
			 	}
			 	var_assign(strStart[i],record->data[i]);
			 }
			 strStart[i] = '\0';
			 strLast[j] = '\0';

			 //add the string colval to the startstring
			 stradd(strStart,colVal);

			 //append the last string to the first string
			 stradd(strStart,strLast);

			 mem_record(record->data,'\0',PAGE_SIZE);

			 cpy_str3(record->data,strStart);
		 }
	 }
	 else
	 {
	 	//add the string in the colval to the data in the record
		 stradd(record->data,colVal);

		 //append a semicolon at the end
		 stradd(record->data,";");
	 }
	 toCheck(record,value);

}

//free the record
RC freeRecord (Record *record)
{
    free(record);
    return(RC_OK);
}
