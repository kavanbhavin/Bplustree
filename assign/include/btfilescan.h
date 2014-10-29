#ifndef _BTREE_FILESCAN_H
#define _BTREE_FILESCAN_H

#include "btfile.h"

class BTreeFile;

class BTreeFileScan : public IndexFileScan {
	
public:
	
	friend class BTreeFile;

    Status GetNext (RecordID & rid, char* keyptr);

	~BTreeFileScan();	

private:
	BTreeFileScan::BTreeFileScan(BtLeafPage * lp, RecordID low, const char * hi){
		current = low;
		this -> hi = hi;
		leaf = lp;
	}
	BtLeafPage * leaf;
	RecordId current;
	const char * hi;
};

#endif