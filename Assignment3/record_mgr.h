#ifndef RECORD_MGR_H
#define RECORD_MGR_H

#include "dberror.h"
#include "expr.h"
#include "tables.h"

// Bookkeeping for scans
typedef struct RM_ScanHandle
{
  RM_TableData *rel;
  void *mgmtData;
} RM_ScanHandle;

// table and manager
extern RC initRecordManager (void *mgmtData);
extern RC shutdownRecordManager ();
extern RC createTable (char *name, Schema *schema);
extern RC openTable (RM_TableData *rel, char *name);
extern RC closeTable (RM_TableData *rel);
extern RC deleteTable (char *name);
extern int getNumTuples (RM_TableData *rel);

// handling records in a table
extern RC insertRecord (RM_TableData *rel, Record *record);
extern RC deleteRecord (RM_TableData *rel, RID id);
extern RC updateRecord (RM_TableData *rel, Record *record);
extern RC getRecord (RM_TableData *rel, RID id, Record *record);

// scans
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
extern RC next (RM_ScanHandle *scan, Record *record);
extern RC closeScan (RM_ScanHandle *scan);

// dealing with schemas
extern int getRecordSize (Schema *schema);
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys);
extern RC freeSchema (Schema *schema);

// dealing with records and attribute values
extern RC createRecord (Record **record, Schema *schema);
extern RC freeRecord (Record *record);
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value);
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value);

#endif // RECORD_MGR_H

#define var_assign(a,b) a=b;
#define createFile(name) createPageFile(name); 
#define openFile(a,b) openPageFile(a,&b); 
#define mem_record(pgHn,b,PAGE_SIZE) memset(pgHn,'\0',PAGE_SIZE);
#define cpy_str(pgHn,schema) strcpy(pgHn,serializeSchema(schema));
#define openFile2(rel,name,handle) openPageFile(rel->name,&handle);
#define cpy_str2(rel,name) strcpy(rel->name,name);
#define blockRead(x,y,z) readBlock(0, &handle, pgHn);
#define blockRead1(x,y,z) readBlock(1, &handle, pgHn);
#define deserialize(pgHn,rel,schema) deSerializeSchema(pgHn,rel->schema);
#define val_assign(rel,mgmtData,NULL) rel->mgmtData = NULL;
#define remove(a) destroyPageFile(a);
#define cpy_str3(a,pgHn) strcpy(a,pgHn);
#define blk_write(a,b,c) writeBlock(1,&b,c);
#define blk_write2(a,b,c) writeBlock(a,&b,c);
#define shutdownFile(handle) closePageFile(&handle);
#define stradd(a,b) strcat(a,b);
#define return(a) return a;
#define blockRead3(a,b,c) readBlock(a,b,c);
#define colPlace(a,b) storeSemiColonPostion(a,b);
#define writeToTheBlock(a,b,c) writeBlock(0,&b,c);
#define colValues(a,b,c,d) fetchData(a,b,c,d);




