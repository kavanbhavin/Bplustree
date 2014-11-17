#include "minirel.h"
#include "bufmgr.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btfilescan.h"
#define CHECK(S)\
	if(S!=OK) return S;
//-------------------------------------------------------------------
// BTreeFile::BTreeFile
//
// Input   : filename - filename of an index.
// Output  : returnStatus - status of execution of constructor.
//           OK if successful, FAIL otherwise.
// Purpose : Open the index file, if it exists.
//			 Otherwise, create a new index, with the specified
//           filename. You can use
//                MINIBASE_DB->GetFileEntry(filename, headerID);
//           to retrieve an existing index file, and
//                MINIBASE_DB->AddFileEntry(filename, headerID);
//           to create a new one. You should pin the header page
//           once you have read or created it. You will use the header
//           page to find the root node.
//-------------------------------------------------------------------
BTreeFile::BTreeFile (Status& returnStatus, const char *filename) {
	// Save the name of the file so we delete appropriately
	// when DestroyFile is called.
	dbname = strcpy(new char[strlen(filename) + 1], filename);

	Status stat = MINIBASE_DB->GetFileEntry(filename, headerID);
	Page *_headerPage;
	returnStatus = OK;

	// File does not exist, so we should create a new index file.
	if (stat == FAIL) {
		//Allocate a new header page.
		stat = MINIBASE_BM->NewPage(headerID, _headerPage, 1);

		if (stat != OK) {
			std::cerr << "Error allocating header page." << std::endl;
			headerID = INVALID_PAGE;
			header = NULL;
			returnStatus = FAIL;
			return;
		}

		header = (BTreeHeaderPage *)(_headerPage);
		header->Init(headerID);
		stat = MINIBASE_DB->AddFileEntry(filename, headerID);

		if (stat != OK) {
			std::cerr << "Error creating file" << std::endl;
			headerID = INVALID_PAGE;
			header = NULL;
			returnStatus = FAIL;
			return;
		}
	} else {
		stat = MINIBASE_BM->PinPage(headerID, _headerPage);

		if (stat != OK) {
			std::cerr << "Error pinning existing header page" << std::endl;
			headerID = INVALID_PAGE;
			header = NULL;
			returnStatus = FAIL;
		}

		header = (BTreeHeaderPage *) _headerPage;
	}
}


//-------------------------------------------------------------------
// BTreeFile::~BTreeFile
//
// Input   : None
// Return  : None
// Output  : None
// Purpose : Free memory and clean Up. You should be sure to
//           unpin the header page if it has not been unpinned
//           in DestroyFile.
//-------------------------------------------------------------------
BTreeFile::~BTreeFile ()
{
	delete [] dbname;

	if (headerID != INVALID_PAGE) 
	{
		Status st = MINIBASE_BM->UnpinPage (headerID, CLEAN);
		if (st != OK)
		{
			cerr << "ERROR : Cannot unpin page " << headerID << " in BTreeFile::~BTreeFile" << endl;
		}
	}
}

//Frees all pages within sp recursively.
//Precondition : sp is pinned.
Status freeRecursive(SortedPage* sp){

	//leaf node : just free us
	if (sp->GetType() == LEAF_NODE){
		FREEPAGE(sp->PageNo());
		return OK;
	}

	//index node : free all descendents first
	BTIndexPage* pageI = (BTIndexPage*)sp;

	//first get the very first left link
	PageID leftLinkID = pageI->GetLeftLink();
	if (leftLinkID != INVALID_PAGE){
		SortedPage* leftLinkPointer;
		PIN(leftLinkID, leftLinkPointer);
		freeRecursive(leftLinkPointer);
		RecordID currRid;
		KeyType currKey;
		PageID nextChild;
		Status s = pageI->GetFirst(currRid, currKey, nextChild);
		while (s != DONE){
			SortedPage* actualPage;
			PIN(nextChild, actualPage);
			freeRecursive(actualPage);
			s = pageI->GetNext(currRid, currKey, nextChild);
		}
	}
	FREEPAGE(sp->PageNo());
	return OK;
}


//-------------------------------------------------------------------
// BTreeFile::DestroyFile
//
// Input   : None
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Free all pages and delete the entire index file. Once you have
//           freed all the pages, you can use MINIBASE_DB->DeleteFileEntry (dbname)
//           to delete the database file.
//-------------------------------------------------------------------
Status BTreeFile::DestroyFile ()
{
	if (header->GetRootPageID() != INVALID_PAGE){
		//Get the root page 
		SortedPage *page;
		PIN(header->GetRootPageID(), (Page *&)page);
		if(page->GetType() == LEAF_NODE){
			//free root page
			FREEPAGE(header->GetRootPageID());
		}else{
			//we have an index root, free all pages recursively
			freeRecursive(page);
		}
	}
	FREEPAGE(headerID);
	headerID = INVALID_PAGE;
	header = NULL;
	Status s = MINIBASE_DB -> DeleteFileEntry(dbname);
	return s;
}

//Rebalances index according to slides and returns index to push up.
//caller must set left link
Status BTreeFile::RebalanceIndex(BTIndexPage* leftPage, BTIndexPage* rightPage, IndexEntry *& indexToPush){
	KeyType movedKey;
	PageID pointerToChild;
	RecordID firstRid, dontcare;
	while (true) {
		Status s = leftPage->GetFirst(firstRid, movedKey, pointerToChild);
		if (s == DONE) break;
		s = rightPage->Insert(movedKey, pointerToChild, dontcare); 
		CHECK(s);
		s= leftPage->DeleteRecord(firstRid);
		CHECK(s);
	}
	while(leftPage->AvailableSpace() > rightPage->AvailableSpace()){
		Status s = rightPage->GetFirst(firstRid, movedKey, pointerToChild);
		CHECK(s);
		s= leftPage->Insert(movedKey, pointerToChild, dontcare);
		CHECK(s);
		s =rightPage->DeleteRecord(firstRid);
		CHECK(s);
	}
	Status s = rightPage ->GetFirst(firstRid, movedKey, pointerToChild);
	CHECK(s);
	rightPage->SetLeftLink(pointerToChild);
	s = rightPage->Delete(movedKey, dontcare);
	//indexToPush = new IndexEntry;
	indexToPush->value = rightPage->PageNo();
	/*for(int i=0; i<MAX_KEY_SIZE; i++){
	indexToPush->key[i] = movedKey[i];
	}*/
	memcpy(indexToPush->key, movedKey, strlen(movedKey)+1);
	return s;
}


//-------------------------------------------------------------------
// BTreeFile::InsertIntoIndex
//
// Input   : key - pointer to the value of the key to be inserted.
//           rid - RecordID of the record to be inserted.
//			 curPage - pointer to the current page to insert into
// Output  : newEntry - the entry we have just inserted.
// Return  : OK if successful, FAIL otherwise.
// Purpose : Insert a key,rid pair into an index 
//-------------------------------------------------------------------
Status BTreeFile::InsertIntoIndex(const char * key, const RecordID rid, BTIndexPage* curPage, IndexEntry *&newEntry){
	RecordID currRid;
	PageID prevPointerToChild;
	KeyType currKey;
	Status s = curPage->GetFirst(currRid, currKey, prevPointerToChild);
	CHECK(s);
	SortedPage * childPage;
	if(KeyCmp(key, currKey) <0){
		prevPointerToChild = curPage->GetLeftLink();
		PIN(prevPointerToChild, childPage);
	}else{
		PageID nextPointerToChild;
		while(curPage->GetNext(currRid, currKey, nextPointerToChild)!=DONE){
			if(KeyCmp(key, currKey) <0) break;
			prevPointerToChild = nextPointerToChild;
		}
		PIN(prevPointerToChild, childPage);
	}
	if(childPage->GetType() ==INDEX_NODE){
		s = InsertIntoIndex(key, rid, ((BTIndexPage *)childPage), newEntry); 
	}else if(childPage->GetType()==LEAF_NODE){
		s = InsertIntoLeaf(key, rid, ((BTLeafPage *)childPage), newEntry);
	}else {
		cout << "page of invalid type in InsertIntoIndex" << endl;
	}
	CHECK(s);
	UNPIN(childPage->PageNo(), true);
	if(newEntry->value !=INVALID_PAGE){
		if(curPage->AvailableSpace() >= GetKeyDataLength(newEntry->key, INDEX_NODE)){
			RecordID dontcare;
			s = curPage->Insert(newEntry->key, newEntry->value, dontcare);
			newEntry->value=INVALID_PAGE;
		}else{
			BTIndexPage * newRightIndexPage;
			PageID newRightIndexPID;
			NEWPAGE(newRightIndexPID, newRightIndexPage);
			newRightIndexPage->Init(newRightIndexPID);
			newRightIndexPage->SetType(INDEX_NODE);
			IndexEntry *temp = new IndexEntry;
			temp->value = newEntry->value;
			/*for(int i=0; i<=MAX_KEY_SIZE; i++){
			temp->key[i] = newEntry->key[i];
			}*/
			memcpy(temp->key, newEntry->key, strlen(newEntry->key)+1);
			RebalanceIndex(curPage, newRightIndexPage, newEntry);
			RecordID dontcare;
			//insert new index into appropriate index
			if(KeyCmp(temp->key,newEntry->key) <0){
				s = curPage->Insert(temp->key, temp->value, dontcare); 
			}else{
				s = newRightIndexPage->Insert(temp->key, temp->value, dontcare);
			}
			UNPIN(newRightIndexPID, true);
			delete temp;
		}
	}
	return s;
}
//-------------------------------------------------------------------
// BTreeFile::InsertIntoLeaf
//
// Input   : key - pointer to the value of the key to be inserted.
//           rid - RecordID of the record to be inserted.
//			 curPage - pointer to the current page to insert into
// Output  : newEntry - the entry we have just inserted.
// Return  : OK if successful, FAIL otherwise.
// Purpose : Insert a key,rid pair into an leaf 
//-------------------------------------------------------------------
Status BTreeFile::InsertIntoLeaf(const char * key, const RecordID rid, BTLeafPage* curPage, IndexEntry *&newEntry){
	if(curPage->AvailableSpace() >= GetKeyDataLength(key, LEAF_NODE)){
		RecordID dontcare;
		Status r = curPage ->Insert(key, rid, dontcare);
		CHECK(r);
		newEntry->value=INVALID_PAGE;
		return r;
	}
	//not enough space, time to split.
	PageID newRightLeafPID;
	BTLeafPage * newRightLeafPage;
	NEWPAGE(newRightLeafPID, newRightLeafPage);
	newRightLeafPage->Init(newRightLeafPID);
	newRightLeafPage->SetType(LEAF_NODE);
	Status s = RebalanceLeaf(curPage, newRightLeafPage);
	CHECK(s);
	newEntry->value = newRightLeafPID;
	RecordID dontcare;
	KeyType smallestKey;
	RecordID dontcare2;
	s = newRightLeafPage->GetFirst(dontcare, smallestKey, dontcare2);
	CHECK(s);
	/*for(int i=0; i<MAX_KEY_SIZE; i++){
	newEntry->key[i] = smallestKey[i];
	}*/
	memcpy(newEntry->key, smallestKey, strlen(smallestKey)+1);
	if(KeyCmp(key, newEntry->key) <0){
		s = curPage->Insert(key, rid, dontcare);
	}else{
		s = newRightLeafPage->Insert(key, rid, dontcare);
	}
	UNPIN(newRightLeafPID, true);
	return s;
}

//-------------------------------------------------------------------
// BTreeFile::InsertRootIsLeaf
//
// Input   : key - pointer to the value of the key to be inserted.
//           rid - RecordID of the record to be inserted.
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Insert an index entry with this rid and key.  
//-------------------------------------------------------------------
Status BTreeFile::InsertRootIsLeaf (const char * key, const RecordID rid, BTLeafPage *& root){
	if (root->AvailableSpace() >= GetKeyDataLength(key, LEAF_NODE)){
		//this means we have enough space in the leaf
		RecordID newEntry;
		Status r = ((BTLeafPage *)root)->Insert(key, rid, newEntry);
		return r;
	}
	//we don't have enough space in the leaf. so its time to start indexing
	// we store our old root page since we'll need it later
	//only for naming ease.
	BTLeafPage * leftLeaf = root;
	PageID leftLeafPID = header->GetRootPageID();
	//first we make a new leaf page
	PageID newRightLeafPID;
	BTLeafPage *newRightLeafPage;
	NEWPAGE(newRightLeafPID, newRightLeafPage);
	newRightLeafPage->Init(newRightLeafPID);
	newRightLeafPage->SetType(LEAF_NODE);
	//now time to start splitting
	//we move all records from old page to new page
	RebalanceLeaf(leftLeaf, newRightLeafPage);
	//now we have somewhat balanced leaf pages. we just need to add an index to the root.
	//first we get the smallest value in the right leaf
	KeyType smallestKey;
	RecordID movedVal, firstRid;
	Status s = newRightLeafPage->GetFirst(firstRid, smallestKey, movedVal);
	CHECK(s);
	//now we make a new root that is an index node not a leaf node
	PageID newRootPID;
	BTIndexPage *newRootPage;
	NEWPAGE(newRootPID, newRootPage);
	newRootPage->Init(newRootPID);
	newRootPage->SetType(INDEX_NODE);
	header->SetRootPageID(newRootPID);
	//now we set it up as an index
	newRootPage->SetLeftLink(leftLeafPID);
	RecordID dontcare;
	s = newRootPage->Insert(smallestKey, newRightLeafPID, dontcare); 
	CHECK(s);
	PageID firstChildPID;
	KeyType firstKey;
	s = newRootPage->GetFirst(dontcare, firstKey, firstChildPID);
	CHECK(s);
	if(KeyCmp(key,firstKey) <0) {
		//insert into firstChildpid
		s = leftLeaf->Insert(key, rid, dontcare);
		CHECK(s);
	}else{
		s = newRightLeafPage->Insert(key,rid,dontcare);
		CHECK(s);
	}
	//now unpin all these pages.
	UNPIN(newRightLeafPID, true);
	UNPIN(newRootPID, true);
	return OK;

}


//-------------------------------------------------------------------
// BTreeFile::RebalanceLeaf
//
// Input   : leftPage - pointer to left page
//           rightPage - empty page to rebalance too
// Output  : leftPage - rebalanced
//			 rightPage - rebalanced
// Return  : OK if successful, FAIL otherwise.
//-------------------------------------------------------------------
Status BTreeFile::RebalanceLeaf(BTLeafPage* leftPage, BTLeafPage* rightPage){
	while (true) {
		KeyType movedKey;
		RecordID movedVal, firstRid, insertedRid;
		Status s = leftPage->GetFirst(firstRid, movedKey, movedVal);
		if (s == DONE) break;
		s = rightPage->Insert(movedKey, movedVal, insertedRid);
		CHECK(s);
		s= leftPage->DeleteRecord(firstRid);
		CHECK(s);
	}
	while(leftPage->AvailableSpace() > rightPage->AvailableSpace()){
		KeyType movedKey;
		RecordID movedVal, firstRid, insertedRid;
		Status s = rightPage->GetFirst(firstRid, movedKey, movedVal);
		CHECK(s);
		s= leftPage->Insert(movedKey, movedVal, insertedRid);
		CHECK(s);
		s =rightPage->DeleteRecord(firstRid);
		CHECK(s);
	}
	rightPage->SetNextPage(leftPage->GetNextPage());
	leftPage->SetNextPage(rightPage->PageNo());
	rightPage->SetPrevPage(leftPage->PageNo());
	return OK;
}


//-------------------------------------------------------------------
// BTreeFile::Insert
//
// Input   : key - pointer to the value of the key to be inserted.
//           rid - RecordID of the record to be inserted.
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Insert an index entry with this rid and key.  
// Note    : If the root didn't exist, create it.
//-------------------------------------------------------------------
Status BTreeFile::Insert (const char *key, const RecordID rid)
{
	// there are several cases to consider here. 
	//first case is that this is the first insert
	if(header->GetRootPageID() == INVALID_PAGE){
		BTLeafPage *page;
		PageID pid;
		NEWPAGE(pid, page);
		page->Init(pid);
		page->SetType(LEAF_NODE);
		header->SetRootPageID(pid);
		RecordID drid;
		page->Insert(key, rid, drid);
		UNPIN(pid, true);
		return OK;
	}
	//otherwise we get the root. we don't know its type yet.
	SortedPage * oldRoot;
	PIN(header->GetRootPageID(), (Page *&) oldRoot);
	if(oldRoot->GetType() == LEAF_NODE){
		Status s = InsertRootIsLeaf(key, rid, (BTLeafPage *&)oldRoot);
		UNPIN(oldRoot->PageNo(), true);
		return s;
	}else if (oldRoot->GetType()==INDEX_NODE){
		//root is index. so we must traverse index.
		//figure out where to insert
		Status s = InsertRootIsIndex(key, rid, (BTIndexPage*)oldRoot);
		UNPIN(oldRoot->PageNo(), true);
		return s;
	}
	cout << "root is not index or leaf?" << endl;
	return FAIL;
}
//note it is always responsibility of calling function to pin and unpin
Status BTreeFile::InsertRootIsIndex(const char * key, const RecordID rid, BTIndexPage * root){
	RecordID currRid;
	PageID prevPointerToChild;
	KeyType currKey;
	Status s = root->GetFirst(currRid, currKey, prevPointerToChild);
	CHECK(s);
	SortedPage * childPage;
	if(KeyCmp(key, currKey) <0){
		prevPointerToChild= root->GetLeftLink();
		PIN(prevPointerToChild, childPage);
	}else{
		PageID nextPointerToChild;
		while(root->GetNext(currRid, currKey, nextPointerToChild)!=DONE){
			if(KeyCmp(key, currKey) <0) break;
			prevPointerToChild = nextPointerToChild;
		}
		PIN(prevPointerToChild, childPage);

	}
	IndexEntry *newEntry = new IndexEntry;
	newEntry->value=INVALID_PAGE;
	if(childPage->GetType() ==INDEX_NODE){
		s = InsertIntoIndex(key, rid, ((BTIndexPage *)childPage), newEntry); 
	}else if(childPage->GetType()==LEAF_NODE){
		s = InsertIntoLeaf(key, rid, ((BTLeafPage *)childPage), newEntry);
	}else {
		cout << "page of invalid type in InsertIntoIndex" << endl;
	}
	UNPIN(prevPointerToChild, true);
	CHECK(s);
	if(newEntry->value != INVALID_PAGE){
		RecordID dontcare;
		if(root->AvailableSpace() >= GetKeyDataLength(newEntry->key, INDEX_NODE)){
			s = root->Insert(newEntry->key, newEntry->value, dontcare);
			delete newEntry;
			return s;
		}
		//time to create new root
		PageID newRootPID;
		BTIndexPage *newRootPage;
		NEWPAGE(newRootPID, newRootPage);
		newRootPage->Init(newRootPID);
		newRootPage->SetType(INDEX_NODE);
		header->SetRootPageID(newRootPID);
		newRootPage->SetPrevPage(root->PageNo());
		PageID newRightIndexPID;
		BTIndexPage *newRightIndexPage;
		NEWPAGE(newRightIndexPID, newRightIndexPage);
		newRightIndexPage->Init(newRightIndexPID);
		newRightIndexPage->SetType(INDEX_NODE);
		IndexEntry *newKey = new IndexEntry;
		newKey->value=INVALID_PAGE;
		RebalanceIndex(root, newRightIndexPage, newKey);
		s = newRootPage->Insert(newKey->key, newKey->value, dontcare);
		UNPIN(newRightIndexPID, true);
		UNPIN(newRootPID, true);
		delete newKey;
	}
	delete newEntry;
	return s;
}



//-------------------------------------------------------------------
// BTreeFile::Delete
//
// Input   : key - pointer to the value of the key to be deleted.
//           rid - RecordID of the record to be deleted.
// Output  : None
// Return  : OK if successful, FAIL otherwise. 
// Purpose : Delete an entry with this rid and key.  
// Note    : If the root becomes empty, delete it.
//-------------------------------------------------------------------

Status BTreeFile::Delete (const char *key, const RecordID rid)
{
	if(header->GetRootPageID() == INVALID_PAGE) return FAIL;
	SortedPage * root;
	PIN(header->GetRootPageID(), (Page *&)root);
	if(root->GetType() == LEAF_NODE){
		Status  r = ((BTLeafPage *)root)->Delete(key, rid);
		UNPIN(header->GetRootPageID(), true);
		return r;
	}else{
		Status s= DeleteIsIndex(key, rid, (BTIndexPage *)root);
		UNPIN(header->GetRootPageID(),true);
		return s;
	}
}


Status BTreeFile::DeleteIsIndex(const char * key, const RecordID rid, BTIndexPage * index){
	RecordID currRid;
	PageID prevPointerToChild;
	KeyType currKey;
	Status s = index->GetFirst(currRid, currKey, prevPointerToChild);
	CHECK(s);
	SortedPage * childPage;
	if(KeyCmp(key, currKey) <0){
		prevPointerToChild= index->GetLeftLink();
		PIN(prevPointerToChild, childPage);
	}else{
		PageID nextPointerToChild;
		while(index->GetNext(currRid, currKey, nextPointerToChild)!=DONE){
			if(KeyCmp(key, currKey) <0) break;
			prevPointerToChild = nextPointerToChild;
		}
		PIN(prevPointerToChild, childPage);
	}
	Status r;
	if(childPage->GetType()==LEAF_NODE){
		r= ((BTLeafPage*)childPage)->Delete(key, rid);
	}else{
		r = DeleteIsIndex(key, rid, (BTIndexPage*)childPage);
	}
	UNPIN(childPage->PageNo(), true);
	return r;
}

//-------------------------------------------------------------------
// BTreeFile::OpenScan
//
// Input   : lowKey, highKey - pointer to keys, indicate the range
//                             to scan.
// Output  : None
// Return  : A pointer to IndexFileScan class.
// Purpose : Initialize a scan.  
// Note    : Usage of lowKey and highKey :
//
//           lowKey   highKey   range
//			 value	  value	
//           --------------------------------------------------
//           NULL     NULL      whole index
//           NULL     !NULL     minimum to highKey
//           !NULL    NULL      lowKey to maximum
//           !NULL    =lowKey   exact match (may not be unique)
//           !NULL    >lowKey   lowKey to highKey
//-------------------------------------------------------------------

IndexFileScan *BTreeFile::OpenScan (const char *lowKey, const char *highKey)
{	
	char* searchTerm = (lowKey == NULL) ? "" : lowKey; //assume "" is lowest string
	PageID firstGuy;
	BTLeafPage* lowPage;
	if (header->GetRootPageID() == INVALID_PAGE || Search(searchTerm, firstGuy) != OK){
		firstGuy = INVALID_PAGE;
		lowPage = NULL;
	}
	else {
		MINIBASE_BM->PinPage(firstGuy, (Page*&)lowPage);
	}
	//If There is a page where this exists, find the recordid it exists at
	RecordID dataRid;
	RecordID rid;
	char firstKey[MAX_KEY_SIZE];
	rid.pageNo = INVALID_PAGE;
	bool overTop = false;
	if (lowPage != NULL) {
		if(lowPage->_Search(rid, searchTerm, dataRid, firstKey) !=OK){
			PageID nextPage = lowPage->GetNextPage();
			MINIBASE_BM->UnpinPage(firstGuy, false);
			while(nextPage != INVALID_PAGE){
				MINIBASE_BM->PinPage(nextPage, (Page *&)lowPage);
				if(lowPage->_Search(rid, searchTerm, dataRid, firstKey)==OK && KeyCmp(firstKey, highKey) <= 0){
					break;
				}
				nextPage = lowPage->GetNextPage();
				MINIBASE_BM->UnpinPage(lowPage->PageNo(), false);
			}
			if(nextPage == INVALID_PAGE) {
				overTop = true;
				lowPage = NULL;
			}
		}
	}
	if(!overTop && (highKey != NULL && KeyCmp(firstKey,highKey)>0)) {
			MINIBASE_BM->UnpinPage(firstGuy, false);
			firstGuy = INVALID_PAGE;
			lowPage = NULL;
	}
	IndexFileScan* tbr = new BTreeFileScan(lowPage, rid, dataRid, firstKey, highKey, (highKey != NULL));

	return tbr;
}



// Dump Following Statistics:
// 1. Total # of leafnodes, and Indexnodes.
// 2. Total # of dataEntries.
// 3. Total # of index Entries.
// 4. Fill factor of leaf nodes. avg. min. max.
Status BTreeFile::DumpStatistics() {	
	ostream& os = std::cout;
	float avgDataFillFactor, avgIndexFillFactor;

	// initialization 
	hight = totalDataPages = totalIndexPages = totalNumIndex = totalNumData = 0;
	maxDataFillFactor = maxIndexFillFactor = 0; minDataFillFactor = minIndexFillFactor =1;
	totalFillData = totalFillIndex = 0;

	if(_DumpStatistics(header->GetRootPageID())== OK)
	{		// output result
		if (totalNumData == 0)
			maxDataFillFactor = minDataFillFactor = avgDataFillFactor = 0;
		else
			avgDataFillFactor = totalFillData/totalDataPages;
		if ( totalNumIndex == 0)
			maxIndexFillFactor = minIndexFillFactor = avgIndexFillFactor = 0;
		else 
			avgIndexFillFactor = totalFillIndex/totalIndexPages;
		os << "\n------------ Now dumping statistics of current B+ Tree!---------------" << endl;
		os << "  Total nodes are        : " << totalDataPages + totalIndexPages << " ( " << totalDataPages << " Data";
		os << "  , " << totalIndexPages <<" indexpages )" << endl;
		os << "  Total data entries are : " << totalNumData << endl;
		os << "  Total index entries are: " << totalNumIndex << endl;
		os << "  Hight of the tree is   : " << hight << endl;
		os << "  Average fill factors for leaf is : " << avgDataFillFactor<< endl;
		os << "  Maximum fill factors for leaf is : " << maxDataFillFactor;
		os << "	  Minumum fill factors for leaf is : " << minDataFillFactor << endl;
		os << "  Average fill factors for index is : " << 	avgIndexFillFactor << endl;
		os << "  Maximum fill factors for index is : " << maxIndexFillFactor;
		os << "	  Minumum fill factors for index is : " << minIndexFillFactor << endl;
		os << "  That's the end of dumping statistics." << endl;

		return OK;
	}
	return FAIL;
}

Status BTreeFile::_DumpStatistics(PageID pageID) { 
	__DumpStatistics(pageID);

	SortedPage *page;
	BTIndexPage *index;

	Status s;
	PageID curPageID;

	RecordID curRid;
	KeyType key;

	PIN (pageID, page);
	NodeType type = page->GetType ();

	switch (type) {
	case INDEX_NODE:
		index = (BTIndexPage *)page;
		curPageID = index->GetLeftLink();
		_DumpStatistics(curPageID);
		s=index->GetFirst(curRid, key, curPageID);
		if ( s == OK) {	
			_DumpStatistics(curPageID);
			s = index->GetNext(curRid, key, curPageID);
			while ( s != DONE) {	
				_DumpStatistics(curPageID);
				s = index->GetNext(curRid, key, curPageID);
			}
		}
		UNPIN(pageID, CLEAN);
		break;

	case LEAF_NODE:
		UNPIN(pageID, CLEAN);
		break;
	default:		
		assert (0);
	}

	return OK;
}

Status BTreeFile::__DumpStatistics (PageID pageID) {
	SortedPage *page;
	BTIndexPage *index;
	BTLeafPage *leaf;
	int i;
	Status s;
	PageID curPageID;
	float	curFillFactor;
	RecordID curRid;

	KeyType  key;
	RecordID dataRid;

	PIN (pageID, page);
	NodeType type = page->GetType ();
	i = 0;
	switch (type) {
	case INDEX_NODE:
		// add totalIndexPages
		totalIndexPages++;
		if ( hight <= 0) // still not reach the bottom
			hight--;
		index = (BTIndexPage *)page;
		curPageID = index->GetLeftLink();
		s=index->GetFirst (curRid , key, curPageID); 
		if ( s == OK) {	
			i++;
			s = index->GetNext(curRid, key, curPageID);
			while (s != DONE) {	
				i++;
				s = index->GetNext(curRid, key, curPageID);
			}
		}
		totalNumIndex  += i;
		curFillFactor = (float)(1.0 - 1.0*(index->AvailableSpace())/MAX_SPACE);
		if ( maxIndexFillFactor < curFillFactor)
			maxIndexFillFactor = curFillFactor;
		if ( minIndexFillFactor > curFillFactor)
			minIndexFillFactor = curFillFactor;
		totalFillIndex += curFillFactor;
		UNPIN(pageID, CLEAN);
		break;

	case LEAF_NODE:
		// when the first time reach a leaf, set hight
		if ( hight < 0)
			hight = -hight;
		totalDataPages++;

		leaf = (BTLeafPage *)page;
		s = leaf->GetFirst (curRid, key, dataRid);
		if (s == OK) {	
			s = leaf->GetNext(curRid, key, dataRid);
			i++;
			while ( s != DONE) {	
				i++;	
				s = leaf->GetNext(curRid, key, dataRid);
			}
		}
		totalNumData += i;
		curFillFactor = (float)(1.0 - 1.0*leaf->AvailableSpace()/MAX_SPACE);
		if ( maxDataFillFactor < curFillFactor)
			maxDataFillFactor = curFillFactor;
		if ( minDataFillFactor > curFillFactor)
			minDataFillFactor = curFillFactor;
		totalFillData += curFillFactor;
		UNPIN(pageID, CLEAN);
		break;
	default:		
		assert (0);
	}

	return OK;	
}

// function  BTreeFile::_SearchIndex
// PURPOSE	: given a IndexNode and key, find the PageID with the key in it
// INPUT	: key, a pointer to key;
//			: currIndexID, 
//			: curIndex, pointer to current BTIndexPage
// OUTPUT	: found PageID
Status BTreeFile::_SearchIndex (const char *key,  PageID currIndexID, BTIndexPage *currIndex, PageID& foundID)
{
	PageID nextPageID;

	Status s = currIndex->GetPageID (key, nextPageID);
	if (s != OK)
		return FAIL;

	// Now unpin the page, recurse and then pin it again

	UNPIN (currIndexID, CLEAN);
	s = _Search (key, nextPageID, foundID);
	if (s != OK)
		return FAIL;
	return OK;
}

// Function: 
// Input	: key, rid
// Output	: found Pid, where Key >= key
// Purpose	: find the leftmost leaf page contain the key, or bigger than the key
Status BTreeFile::_Search( const char *key,  PageID currID, PageID& foundID)
{

	SortedPage *page;
	Status s;

	PIN (currID, page);
	NodeType type = page->GetType ();

	// TWO CASES:
	// - pageType == INDEX:
	//   search the index
	// - pageType == LEAF_NODE:
	//   set the output page ID

	switch (type) 
	{
	case INDEX_NODE:
		s =	_SearchIndex(key,  currID, (BTIndexPage*)page, foundID);
		break;

	case LEAF_NODE:
		foundID =  page->PageNo();
		UNPIN(currID,CLEAN);
		break;
	default:		
		assert (0);
	}


	return OK;
}

// BTreeeFile:: Search
// PURPOSE	: find the PageNo of a give key
// INPUT	: key, pointer to a key
// OUTPUT	: foundPid

Status BTreeFile:: Search(const char *key,  PageID& foundPid)
{
	if (header->GetRootPageID() == INVALID_PAGE)
	{
		foundPid = INVALID_PAGE;
		return DONE;

	}

	Status s;

	s = _Search(key,  header->GetRootPageID(), foundPid);
	if (s != OK)
	{
		cerr << "Search FAIL in BTreeFile::Search\n";
		return FAIL;
	}

	return OK;
}

Status BTreeFile::_PrintTree ( PageID pageID)
{
	SortedPage *page;
	BTIndexPage *index;
	BTLeafPage *leaf;
	int i;
	Status s;
	PageID curPageID;

	RecordID curRid;

	KeyType  key;
	RecordID dataRid;

	ostream& os = cout;

	PIN (pageID, page);
	NodeType type = page->GetType ();
	i = 0;
	switch (type) 
	{
	case INDEX_NODE:
		index = (BTIndexPage *)page;
		curPageID = index->GetLeftLink();
		os << "\n---------------- Content of Index_Node-----   " << pageID <<endl;
		os << "\n Left most PageID:  "  << curPageID << endl;
		s=index->GetFirst (curRid , key, curPageID); 
		if ( s == OK)
		{	i++;
		os <<"Key: "<< key<< "	PageID: " 
			<< curPageID  << endl;
		s = index->GetNext(curRid, key, curPageID);
		while ( s != DONE)
		{	
			os <<"Key: "<< key<< "	PageID: " 
				<< curPageID  << endl;
			i++;
			s = index->GetNext(curRid, key, curPageID);

		}
		}
		os << "\n This page contains  " << i <<"  Entries!" << endl;
		UNPIN(pageID, CLEAN);
		break;

	case LEAF_NODE:
		leaf = (BTLeafPage *)page;
		s = leaf->GetFirst (curRid, key, dataRid);
		if ( s == OK)
		{	os << "\n Content of Leaf_Node"  << pageID << endl;
		os <<   "Key: "<< key<< "	DataRecordID: " 
			<< dataRid  << endl;
		s = leaf->GetNext(curRid, key, dataRid);
		i++;
		while ( s != DONE)
		{	
			os <<   "Key: "<< key<< "	DataRecordID: " 
				<< dataRid  << endl;
			i++;	
			s = leaf->GetNext(curRid, key, dataRid);
		}
		}
		os << "\n This page contains  " << i <<"  entries!" << endl;
		UNPIN(pageID, CLEAN);
		break;
	default:		
		assert (0);
	}
	//os.close();

	return OK;	
}

Status BTreeFile::PrintTree ( PageID pageID, PrintOption option)
{ 
	_PrintTree(pageID);
	if (option == SINGLE) return OK;

	SortedPage *page;
	BTIndexPage *index;

	Status s;
	PageID curPageID;

	RecordID curRid;
	KeyType  key;

	PIN (pageID, page);
	NodeType type = page->GetType ();

	switch (type) {
	case INDEX_NODE:
		index = (BTIndexPage *)page;
		curPageID = index->GetLeftLink();
		PrintTree(curPageID, RECURSIVE);
		s=index->GetFirst (curRid , key, curPageID);
		if ( s == OK)
		{	
			PrintTree(curPageID, RECURSIVE);
			s = index->GetNext(curRid, key, curPageID);
			while ( s != DONE)
			{	
				PrintTree(curPageID, RECURSIVE);
				s = index->GetNext(curRid, key, curPageID);
			}
		}
		UNPIN(pageID, CLEAN);
		break;

	case LEAF_NODE:
		UNPIN(pageID, CLEAN);
		break;
	default:		
		assert (0);
	}

	return OK;
}

Status BTreeFile::PrintWhole() {	
	ostream& os = cout;

	os << "\n\n------------------ Now Begin Printing a new whole B+ Tree -----------"<< endl;

	if(PrintTree(header->GetRootPageID(), RECURSIVE)== OK)
		return OK;
	return FAIL;
}


