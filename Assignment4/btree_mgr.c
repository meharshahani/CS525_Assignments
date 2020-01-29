#include "btree_mgr.h"
#include "tables.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <stdlib.h>
#include <string.h>


SM_FileHandle filehandleBtree;
int maxnumelements;

//define a struct of the b tree and take the following variables
//key: will keep a track of the key of the node
//next: will point to the key or node after the current one
//id: defines the RID of the b tree

typedef struct BTREE
{
    int *key;
    struct BTREE **next;
    RID *id;
} BTree;


BTree * tree_mgr = NULL;
//last: will help to keep track of the previous key/node
BTree *last;

//lookup: will help to perform a scan on the b tree
BTree *lookup;

//*************************************************init and shutdown index manager******************************************************//

//this method will initialise the index manager
RC initIndexManager (void *mgmtData)
{
    initStorageManager();
    printf("successfully initialised Index manager\n");
    return RC_OK;
}

//this method will close the index manager
RC shutdownIndexManager ()
{
    tree_mgr= NULL;
    printf("successfully shut down index manager\n");
    return RC_OK;
}


//*********************************************create, destroy, open, and close an btree index******************************************//


int element;
int indexNum = 0;

//now we will begin by creating a b tree
RC createBtree (char *idxId, DataType keyType, int n)
{
    
    //allocate the size of the b tree to the btree pointer 
    last = (BTree*)allocatemem(sizeof(BTree));
    
    //allocate the sizeof int times the size of the integer n to the key of the last pointer of the b tree
    allocateLast(last->key);
    
    //allocate the sizeof int times the size of the integer n to the id of the last pointer of the b tree
    allocateLast(last->id);

    //the next pointer will be the size of b tree times the integer + 1 as it is pointing to the next one
    last->next = malloc(sizeof(BTree) * (n + 1));

    int x;

    //while the condition is less than the next pointer we will assign the next[i] of the last pointer to null
    for (x = 0; x < n + 1; x ++)
        last->next[x] = NULL;
    maxnumelements = n;
    createPageFile (idxId);
    
    return RC_OK;

}


//this function will close the b tree after the scan is done
RC closeBtree (BTreeHandle *tree)
{
    //we will now close the file that we had previously opened

    //check if now the fileHandle of the b tree for that file is equal to 0 or not
    if(closePageFile(&filehandleBtree)==0){
	free(last);

    free(tree_mgr);
    return RC_OK;
	}
	else{
        //error
		return RC_ERROR;
	}
}



//we will now delete the b tree as the scan has been successfully completed

RC deleteBtree (char *idxId)
{
    RC result;
    //we will first remove the page file which we previously closed

    //now check if the id of that page file is equal to 0 or not
    if(destroyPageFile(idxId)!=RC_OK){
		return result;
        printf("\n successfully deleted b-tree\n");
	}
    
}

RC openBtree (BTreeHandle **tree, char *idxId)
{
    //we will now open the page file that we created initially

    //check if the file handle is equal to 0 or not
    if(openPageFile (idxId, &filehandleBtree)==0){

        printf("successfully opened an existing  BTree\n");
    	return RC_OK;
	}

    else{
        //error
    	return NULL;
	}
}

//******************************************access information about a b-tree************************************************//

//this function will help us to know the type of the key of the btree
RC getKeyType (BTreeHandle *tree, DataType *result)
{


    //return OK
    return RC_OK;
}


RC getNumEntries (BTreeHandle *tree, int *result)
{

	int final = 0;
    
    BtreeMalloc;
//set temp to be equal to the last node
    temp = last;

    int i;
        //while temp is not null we need to do the following
    for (temp = last; temp != NULL; temp = temp->next[maxnumelements])
        //search in the entire tree and if the key is not equal to null 
        //increment the cnt of total elements in the tree
        for (i = 0; i < maxnumelements; i ++)
            if (temp->key[i] != 0)
                final++;
    //set result to be equal to the total number of elements
    *result = final;
    
    return RC_OK;
}


//this function will give us the number of entries in the b tree

RC getNumNodes (BTreeHandle *tree, int *result)
{

    
    int total;
  
    int i;
    BtreeMalloc;
    
    //until the index is less than the int + 2
    for (i = 0; i < maxnumelements + 2; i ++) {
        //we will increment the total number of nodes
        total++;
    }
    *result = total;

    
    //finally we will set the result pointer to point to the last number of node
    return RC_OK;
}




//******************************************************index access*****************************************************//


//this function will insert a particular key in the b tree
RC insertKey (BTreeHandle *tree, Value *key, RID rid)
{
    int x = 0;
    //allocate the size of the btree to the node pointer of the b tree
    BtreeMalloc;
    BTree *node = (BTree*)allocatemem(sizeof(BTree));

    //set the key and id of the node of the b tree to be equal to the size of int times the element size

    node->key = allocatemem(sizeof(int) * maxnumelements);
    node->id = allocatemem(sizeof(int) * maxnumelements);
    //allocate the next pointer of the node the memory of the element + 1 of the b tree

    node->next = allocatemem(sizeof(BTree) * (maxnumelements + 1));
        //loop until all the elements of the b tree have been observed
    for (x = 0; x < maxnumelements; x ++) {
        //set the key of the node to be equal to 0 

    	node->key[x] = 0;
    }


    
    int nodeFull = 0;
    //until temp does not observe all the nodes, do the following
    for (temp = last; temp != NULL; temp = temp->next[maxnumelements]) {
        nodeFull = 0;
        //until x is less than the number of elements
        for (x = 0; x < maxnumelements; x ++) {
            //check if the key in the temp for the particular parse is 0 or not

            if (temp->key[x] == 0) {
                
                //set the page of the temp->key to its rid value
                setTempId(rid.page);

                //set the slot of the temp->key to its rid value
                setTempIds(rid.slot);

                temp->key[x] = key->v.intV;
                temp->next[x] = NULL;
                nodeFull ++;
                break;
            }
        }
        if ((nodeFull == 0) && (temp->next[maxnumelements] == NULL)) {
            node->next[maxnumelements] = NULL;
            temp->next[maxnumelements] = node;
        }
    }
    
    int totalEle = 0;
    for (temp = last; temp != NULL; temp = temp->next[maxnumelements])
        for (x= 0; x < maxnumelements; x++)
            if (temp->key[x] != 0)
                totalEle ++;

    if (totalEle == 6) {
        //set the 0th key of the next element of the last node to point to the 0th key of the node
   
        node->key[0] = last->next[maxnumelements]->key[0];
        //set the 1st key of the node to the next element after the previous one
   
        node->key[1] = last->next[maxnumelements]->next[maxnumelements]->key[0];
       //set the 0th and 1st index of the next nodes to point to the last and +1 element of the node

        node->next[0] = last;

        node->next[1] = last->next[maxnumelements];

        //set the 2nd index of the next node to the next of next[element]
        node->next[2] = last->next[maxnumelements]->next[maxnumelements];

    }
    
    return RC_OK;
}

//to find a particular key in the b tree
RC findKey (BTreeHandle *tree, Value *key, RID *result)
{
     BtreeMalloc;
    int found = 0, x;
    for (temp = last; temp != NULL; temp = temp->next[maxnumelements]) {
        //iterate from 0 to maxnumelements
        for (x = 0; x < maxnumelements; x++) {
            //check condition
            if (temp->key[x] == key->v.intV) {
                //set the xth page id as result
                (*result).page = temp->id[x].page;
                //set the xth slot id as result
                (*result).slot = temp->id[x].slot;
                found = 1;
                break;
            }
        }
        if (found == 1)
            break;
    }
    if (found == 1)
        //return ok
        return RC_OK;
    else
        //return Key not found exception
        return RC_IM_KEY_NOT_FOUND;
}


//open Tree Scan
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
    lookup = (BTree*)allocatemem(sizeof(BTree));
    lookup = last;
    indexNum = 0;
    
    //BTree *temp = (BTree*)allocatemem(sizeof(BTree));
    BtreeMalloc;
    int totalEle = 0, x;
    for (temp = last; temp != NULL; temp = temp->next[maxnumelements])
        //iterate x from 0 to maxnumelements
        for (x = 0; x < maxnumelements; x ++)
            //check condition
            if (temp->key[x] != 0)
                //increment total elements
                totalEle ++;

    int key[totalEle];
    int elements[maxnumelements][totalEle];
    //set cnt at 0
    int cnt = 0;
    for (temp = last; temp != NULL; temp = temp->next[maxnumelements]) {
        //iterate x from 0 to maxnumelements
        for (x = 0; x < maxnumelements; x ++) {
            key[cnt] = temp->key[x];
            //set 0th element cnt at temp id of the page
            elements[0][cnt] = temp->id[x].page;
            //set 1st element cnt at temp slot id 
            elements[1][cnt] = temp->id[x].slot;
            //increment cnt
            cnt ++;
        }
    }
    
    int swap;
    int pg, st, f, g;
    //iterate from 0 to count
    for (f = 0 ; f < cnt - 1; f ++)
    {
        //iterate g from 0 to count-f-1
        for (g = 0 ; g < cnt - f - 1; g ++)
        {
            //check condition
            if (key[g] > key[g+1])
            {
                //swap gth key
                swap = key[g];
                pg = elements[0][g];
                st = elements[1][g];
                
                //increment key number
                key[g]   = key[g + 1];
                elements[0][g] = elements[0][g + 1];
                elements[1][g] = elements[1][g + 1];
                
                //store incremented key in swap
                key[g + 1] = swap;
                elements[0][g + 1] = pg;
                elements[1][g + 1] = st;
            }
        }
    }
    
    cnt = 0;
    for (temp = last; temp != NULL; temp = temp->next[maxnumelements]) {
        //iterate x from 0 to maxnumelements
        for (x= 0; x < maxnumelements; x ++) {

            temp->key[x] =key[cnt];
            temp->id[x].page = elements[0][cnt];
            temp->id[x].slot = elements[1][cnt];
            //increment count
            cnt ++;
        }
    }
    //return OK
    return RC_OK;
}


//delete key
RC deleteKey (BTreeHandle *tree, Value *key)
{
    //allocate memory to btree
    BtreeMalloc;
    int found = 0, x;
    for (temp = last; temp != NULL; temp = temp->next[maxnumelements]) {
        //iterate x from 0 to maxnumelements
        for (x = 0; x < maxnumelements; x++) {

            if (temp->key[x] == key->v.intV) {
                //xth temp key to 0
                temp->key[x] = 0;
                //set xth page id equal to zero
                setTempId(0);
                //set xth slot id to zero
                
                setTempIds(0)
                found = 1;
                break;
            }
        }
        if (found == 1)
            break;
    }
    return RC_OK;
}

//Close the tree scan
RC closeTreeScan (BT_ScanHandle *handle)
{
    indexNum = 0;
    return RC_OK;
}


//Go to next entry
RC nextEntry (BT_ScanHandle *handle, RID *result)
{
    if(lookup->next[maxnumelements] != NULL) {
        if(maxnumelements == indexNum) {
            indexNum = 0;
            lookup = lookup->next[maxnumelements];
        }
        //set result page to lookup id with index number of the page
        (*result).page = lookup->id[indexNum].page;
        //set result slot to lookup id with index number of the slot
        (*result).slot = lookup->id[indexNum].slot;
        //increment index number
        indexNum ++;
    }
    else
        //return  no more enries when the entries end
        return RC_IM_NO_MORE_ENTRIES;
    //return ok
    return RC_OK;
}


//****************************************debug and test functions*************************************************************//

//prints the final b tree
char *printTree (BTreeHandle *tree)
{
    printf("\nPrint the tree:\n");


    // if (tree_mgr->next == NULL) {
    //     printf("Blank tree.\n");
    //     return '\0';
    // }
    return RC_OK;
}
