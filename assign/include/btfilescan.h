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
	BTreeFileScan::BTreeFileScan(BTLeafPage * lp, RecordID low, const char * hi = NULL, bool upperBounded = true)
	 :current(low), hi(hi), leaf(lp), upperBounded(upperBounded){ }

	BTLeafPage * leaf;
	RecordID current;
	const char * hi;
	bool upperBounded;
};

#endif