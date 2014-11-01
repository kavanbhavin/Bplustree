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
	//TODO : unpin current page
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
	if (leaf == NULL) return DONE; //there was never anything to scan
	RecordID mayberid;
	char * maybekeyptr;
	//Get the next recordid on this page
	if((*leaf).GetNext(current, maybekeyptr, mayberid) == OK) {
		//We've reached a key that is above our range, unpin the current page and return DONE
		if (!upperBounded || strcmp(maybekeyptr, hi) > 0) {
			UNPIN(leaf->PageNo(), false);
			return DONE;
		}
		//We're still in the ok range, update the required pointers and return OK
		rid = mayberid;
		keyPtr = maybekeyptr;
		return OK;
	}
	//Need to look in next page
	PageID newLeafPid = (*leaf).GetNextPage();
	UNPIN(leaf->PageNo(), false);
	if(newLeafPid == INVALID_PAGE) return DONE;
	//next page is valid
	PIN(newLeafPid, (Page*&)leaf);
	if ((*leaf).GetFirst(current, maybekeyptr, mayberid) == OK) {
		//We've reached a key that is above our range, unpin the current page and return DONE
		if (!upperBounded || strcmp(maybekeyptr, hi) > 0) {
			UNPIN(leaf->PageNo(), false);
			return DONE;
		}
		//We're still in the ok range, update the required pointers and return OK
		rid = mayberid;
		keyPtr = maybekeyptr;
		return OK;
	}
	UNPIN(leaf->PageNo(), false);
    return DONE;
}
