Group 26:

Disha Patel(A20452866)
Mehar Shahani (A20439871)
Yash Raj(A20453633)

To run the code

Test Cases:

1. Go to the assignment directory.

2. Run "make" in the terminal to make the Object files.

3. For test cases run:

	./test_assign3
	./test_expr

4. Run "make clean" in the terminal to clean all the object files.
(PS: when we run the 'make clean' and then the 'make' command,there is an error that is generated. However, the test cases run perfectly fine)

This project consists of following files:

storage_mgr.c - which holds the function definition of all the functions requried for the assignment
storage_mgr.h - is a Headerfile consists of all the function declared which are requried for the assignment
buffer_mgr.c - holds the function definition of all the functions related to the buffer.
buffer_mgr.h - is a header file that consists of all the function and macos declaration related to the buffer.
dberror.h - is a Headerfile consists of all the constants RC
dberror.c - consists of functions definition for error messages
test_assign3_1.c - is a Test case file which tests all the requried functions

FUNCTIONS DESCRIPTION:

initRecordManager ()
- It is used to initialize the record manager.

shutdownRecordManager()
- It is used to deallocate all the allocated resources of the record manager and shutdwon the record manager.
-For deallocating, set the recordManager structure pointer = NULL and call free() function.

createTable()
- It is used to open the table named in the parameter and initializes the bufferpool.
- It also intializes all the values in the table along with their proper attribute and then it creates,open,write blocks in the page file and closes the page file.

openTable()
- Opens the table with the name specifies in the schema parameter.

closeTable()
- It is used to close the table specified in the parameter and it is done using the shutdown function of buffer pool.

deleteTable()
- It is used to delete the table ausing the  destroy page file function of the storage manager and it also free the memory allocated for the table.

getNumTuples()
- It is used to return the number of tuples in the table.

insertRecord()
- It is used to insert and update the record using the record id as we maintain the record id while inserting or updating the record.
- It is also used to copy the record's into the new record using functions.

deleteRecord()
- It is used to delete the record using the record id and it also maintains one metadata freePage for maintaing the PAgeId of the page of which the records are deleted so that pages can be used for new records.

updateRecord()
- It is used to update the record. It is done with the help of record id. It will navigate to the location where the record is stored using the record id.

getRecord()
- It is used to retrive a record using the record id.

startScan()
- It is used to scan the data which we can get using the function startScan().

next()
- It is used to return the next tuple who can satify the test expressions. If the tuple satisfy the condition then we will unpin the page and will return RC_OK.

closeScan() 
- It is used to close the scanning operation on the data. We can also whether the scan is complete or incmplete by checking the scanCount value of the table's metadata.

getRecordSize()
- It is used to get the size of a record specifies in the schema.

freeSchema()
-It is used to remove the schema from the memory and after that we deallocate all the memory or resources allocated to that schema using functions.

createSchema()
-It is used to create new schema in the memory. We create the schema object and allocate the memory to the object of that schema. And then we set the schema's paramter as passed. 

createRecord()
-It is used to creata new record in the schema and is used to allocate memory space to the new record that has been inserted in the schema. And also give a new record Id to maintain it's existence.

freeRecord()
- It is used to free all the memory used for the record.

getAttr()
- It is used to retrive an attribute from the record. The record number,schema number and the attribute number whhich is to be retrieved is mentioned in the paramter. It can iterate through the records using these parameters for getting the attribute value.

setAttr()
-It is used to set the attribute value in the record in the specified schema. The record, schema and attribute number is passed through the paramter whose values are to be set. It will iterate through the records using these parameters and set the attribute value.



















 

