#include "minirel.h"
#include "bufmgr.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btfilescan.h"
#include <cstring>

//-------------------------------------------------------------------
// BTreeFileScan::~BTreeFileScan
//
// Input   : None
// Output  : None
// Purpose : Clean Up the B+ tree scan.
//-------------------------------------------------------------------

BTreeFileScan::~BTreeFileScan ()
{
	//TODO: add your code here
	if(leaf != NULL) MINIBASE_BM -> UnpinPage(leaf->PageNo(), false);
}


//-------------------------------------------------------------------
// BTreeFileScan::GetNext
//
// Input   : None
// Output  : rid  - record id of the scanned record.
//           keyPtr - and a pointer to it's key value.
// Purpose : Return the next record from the B+-tree index.
// Return  : OK if successful, DONE if no more records to read.
//-------------------------------------------------------------------
Status BTreeFileScan::GetNext (RecordID & rid, char* keyPtr)
{	
	if (leaf == NULL || current_entry.pageNo == INVALID_PAGE || curKey == NULL) return DONE; //there was never anything to scan
	rid = current_data;
	memcpy(keyPtr, curKey, strlen(curKey) + 1);
	//Get the next recordid on this page
	if((*leaf).GetNext(current_entry, curKey, current_data) == OK) {
		//We've reached a key that is above our range, unpin the current page and return DONE
		if (upperBounded && strcmp(curKey, hi) > 0) {
			UNPIN(leaf->PageNo(), false);
			leaf = NULL; //make sure we return done next time
			return OK;
		}
		return OK;
	}
	//Need to look in next page
	PageID newLeafPid = (*leaf).GetNextPage();
	UNPIN(leaf->PageNo(), false);
	if(newLeafPid == INVALID_PAGE) {
		leaf = NULL; //make sure we return done next time
		return OK;
	}
	//next page is valid
	PIN(newLeafPid, (Page*&)leaf);
	if ((*leaf).GetFirst(current_entry, curKey, current_data) == OK) {
		//We've reached a key that is above our range, unpin the current page and return DONE
		if (upperBounded && strcmp(curKey, hi) > 0) {
			UNPIN(leaf->PageNo(), false);
			leaf = NULL; //make sure we return done next time
			return OK;
		}
		
		return OK;
	}
	UNPIN(leaf->PageNo(), false);
	leaf = NULL; //make sure we return done next time
    return OK;
}
