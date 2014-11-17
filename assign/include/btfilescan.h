#ifndef _BTREE_FILESCAN_H
#define _BTREE_FILESCAN_H

#include "btfile.h"
#include "btleaf.h"

class BTreeFile;

class BTreeFileScan : public IndexFileScan {
	
public:
	
	friend class BTreeFile;

    Status GetNext (RecordID & rid, char* keyptr);

	~BTreeFileScan();	

private:
	BTreeFileScan::BTreeFileScan(BTLeafPage * lp, RecordID rid, RecordID dataRid, char * curKey, const char * hi = NULL, bool upperBounded = true)
	 :current_entry(rid), hi(hi), leaf(lp), upperBounded(upperBounded), current_data(dataRid){
		 if(lp !=NULL) memcpy(this->curKey, curKey, strlen(curKey)+1);
	}

	BTLeafPage * leaf;
	RecordID current_entry;
	RecordID current_data;
	const char * hi;
	char curKey[MAX_KEY_SIZE];
	bool upperBounded;
};

#endif