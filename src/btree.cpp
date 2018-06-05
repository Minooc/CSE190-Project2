/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"

#include <queue>
#include <cmath>

//#define DEBUG
class Empty{};

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{

	// Construct index name
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	outIndexName = idxStr.str();

	// If index file already exists, remove it first
	try {
		File::remove(outIndexName);
	}
	catch (FileNotFoundException &e) { }

	// Create/Open blobfile
	BlobFile *bfile;
	if(bfile->exists(outIndexName)) {
		bfile = new BlobFile(outIndexName, false);
		this->file = bfile;
	}
	else {
		bfile = new BlobFile(outIndexName, true);
		this->file = bfile;
	}

	// Set values for BTreeIndex
	this->bufMgr = bufMgrIn;
	this->attributeType = attrType;
	this->attrByteOffset = attrByteOffset;
	this->numOfNodes = 0;
	this->startScanIndex = -1;
	this->scanExecuting = false;

	// Construct metadata page
	Page * metaPage = new Page();
	this->bufMgr->allocPage(this->file, this->headerPageNum, metaPage);
	IndexMetaInfo * metadata = new IndexMetaInfo();
	metadata = (IndexMetaInfo*)metaPage;
	metadata->attrByteOffset = attrByteOffset;
	metadata->attrType = attrType;
	metadata->rootPageNo = 0;
	strcpy(metadata->relationName, relationName.c_str());
	this->bufMgr->unPinPage(this->file, this->headerPageNum, 1);

	// Insert all entries in relation into index
	FileScan fscan(relationName, this->bufMgr);

	// Specify leaf and nonleaf types
	LeafNodeInt* LEAFINTEGER = new LeafNodeInt();
	NonLeafNodeInt * NONLEAFINTEGER = new NonLeafNodeInt();
	LeafNodeDouble* LEAFDOUBLE = new LeafNodeDouble();
	NonLeafNodeDouble * NONLEAFDOUBLE = new NonLeafNodeDouble();
	LeafNodeString* LEAFSTRING = new LeafNodeString();
	NonLeafNodeString * NONLEAFSTRING = new NonLeafNodeString();

	try {
		while (true) {
			// Get the record and call insert entry based on type
			RecordId rid;
			fscan.scanNext(rid);
			std::string record = fscan.getRecord();

			switch(this->attributeType) {

				case INTEGER:
					this->insertEntry(LEAFINTEGER, NONLEAFINTEGER, *((int*)record.c_str()), rid);
					break;
				case DOUBLE: 
					this->insertEntry(LEAFDOUBLE, NONLEAFDOUBLE, (double) (*((int*)record.c_str())), rid);
					break;
				case STRING:{
					char * tempRecd = (char*)(record.c_str() + attrByteOffset);		
					this->insertEntryString( tempRecd, rid);
					break;
				}
				default: break;
			}
		}
	}
	catch(EndOfFileException &e) {	}

	//Delete unused nodes
	delete(LEAFINTEGER);
	delete(NONLEAFINTEGER);
	delete(LEAFDOUBLE);
	delete(NONLEAFDOUBLE);
	delete(LEAFSTRING);
	delete(NONLEAFSTRING);;


}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	this->file->~File();
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
template<typename T, typename L, typename NL>
const void BTreeIndex::insertEntry(L* leafType, NL* nonLeafType, T keyValue, const RecordId rid) 
{
	// Set the leaf size based on type
	int leafSize;
	switch(this->attributeType) {
		case INTEGER: leafSize = INTARRAYLEAFSIZE; break;
		case DOUBLE: leafSize = DOUBLEARRAYLEAFSIZE; break;
		case STRING: leafSize = STRINGARRAYLEAFSIZE; break;
		default: break;
	}

	// Read root node
	Page * metadataPage = new Page();
	this->bufMgr->readPage(this->file, this->headerPageNum, metadataPage);
	IndexMetaInfo * metadata = new IndexMetaInfo();
	metadata = (IndexMetaInfo*)metadataPage;

	//  Tree is empty (first insertion) 
	if (metadata->rootPageNo == 0) {
		// Allocate root page
		Page * rootPage = new Page();
		this->bufMgr->allocPage(this->file, this->rootPageNum, rootPage);
		metadata->rootPageNo = this->rootPageNum;
		this->numOfNodes++;
		
		L * rootNode = new L();
		rootNode = (L*)rootPage;

		// Initialize root page based on type
		switch (this->attributeType) {
			case INTEGER: initializeInt((LeafNodeInt*)rootNode); break;
			case DOUBLE: initializeDouble((LeafNodeDouble*)rootNode); break;
			default: break;
		}
		// Assign record to node
		rootNode->keyArray[0] = keyValue;
		rootNode->ridArray[0] = rid;
		this->bufMgr->unPinPage(this->file, this->rootPageNum, 1);
	}

	// Tree is not empty (every cases other than first insertion) 
	else {

		/* This block handles the simple case where there is only one node.
 		 * What make this case differ from other cases are:
 		 * 1. Because there is only one node, root will be a leaf.
 		 *    So we cast the root to LeafNodeInt.
 		 * 2. We are not traversing through nodes. So parent node will be NULL.
 		 */
		if (this->numOfNodes == 1)  {
			Page * rootPage = new Page();
			this->bufMgr->readPage(this->file, this->rootPageNum, rootPage);
			L * rootNode = new L();
			rootNode = (L*)rootPage;


			// After the insertion, call fullNodeHandler if the node is full.
			switch(this->attributeType) {
				case INTEGER: insertToNodeNumber<int>(rootNode, keyValue, rid); break;
				case DOUBLE: insertToNodeNumber<double>(rootNode, keyValue, rid); break;
				default: break;
			}
	
			if (rootNode->keyArray[leafSize-1] != -1) { 
				NL* dummy = new NL();
				fullNodeHandlerNumber(leafType, nonLeafType, rootNode, dummy, this->rootPageNum,1, 1);
				this->numOfNodes++;
				delete(dummy);
			}
		}

		// This block is the general case where there is more than one node 
		else {	

			// Read root node
			Page * rootPage;
			bufMgr->readPage(file, metadata->rootPageNo, rootPage);  
			NL * rootNode = new NL();
			rootNode = (NL*)rootPage;

			// Find appropiate leaf node and insert. Call fullNodeHandler if the 
			// insertion causing the node to be fully filled up
			traverseAndInsertNumber(leafType, nonLeafType, rootNode, keyValue, rid);
			if (rootNode->keyArray[leafSize-1] != -1) {
				NL* dummy = new NL();
				fullNodeHandlerNumber(leafType, nonLeafType, rootNode, dummy, this->rootPageNum, 1, 1);
				this->numOfNodes++;
				delete(dummy);
			}
			
		}

	}

	// Set root page number and unpin page	
	metadata->rootPageNo = this->rootPageNum;
	this->bufMgr->unPinPage(this->file, this->headerPageNum, 0);
}



// -----------------------------------------------------------------------------
// BTreeIndex::insertEntryString - helper function of insertEntry for string
// -----------------------------------------------------------------------------
const void BTreeIndex::insertEntryString(const void* key, const RecordId rid) 
{

	char* keyValue = (char*)key;
	int leafSize;
	leafSize = STRINGARRAYLEAFSIZE; 

	Page * metadataPage = new Page();
	this->bufMgr->readPage(this->file, this->headerPageNum, metadataPage);
	IndexMetaInfo * metadata = new IndexMetaInfo();
	metadata = (IndexMetaInfo*)metadataPage;

	// Tree is empty (first insertion) 
	if (metadata->rootPageNo == 0) {
		// Allocate root page
		Page * rootPage = new Page();
		this->bufMgr->allocPage(this->file, this->rootPageNum, rootPage);
		metadata->rootPageNo = this->rootPageNum;
		this->numOfNodes++;

		
		LeafNodeString * rootNode = new LeafNodeString();
		rootNode = (LeafNodeString *) rootPage;

		// Initialze root node
		initializeString(rootNode);
		strncpy(rootNode->keyArray[0], (char *) keyValue, 10);
		rootNode->ridArray[0] = rid;
		this->bufMgr->unPinPage(this->file, this->rootPageNum, 1);
	}

	// Tree is not empty (every cases other than first insertion) 
	else {

		/* This block handles the simple case where there is only one node.
 		 * What make this case differ from other cases are:
 		 * 1. Because there is only one node, root will be a leaf.
 		 *    So we cast the root to LeafNodeInt.
 		 * 2. We are not traversing through nodes. So parent node will be NULL.
 		 */
		if (this->numOfNodes == 1)  {
			// Read root node
			Page * rootPage = new Page();
			this->bufMgr->readPage(this->file, this->rootPageNum, rootPage);
			LeafNodeString * rootNode = new LeafNodeString();
			rootNode = (LeafNodeString*)rootPage;

			// After the insertion, call fullNodeHandler if the node is full.
			insertToNodeString(rootNode,keyValue,rid);
			if(strcmp(rootNode->keyArray[leafSize-1], "\0\0\0\0\0\0\0\0\0\0") != 0){
				fullNodeHandlerString(rootNode, NULL, rootPageNum, 1);
				this->numOfNodes++;
			}
		}

		// This block is the general case where there is more than one node */
		else {	
			// Read root node
			Page * rootPage;
			bufMgr->readPage(file, metadata->rootPageNo, rootPage);  
			NonLeafNodeString * rootNode = new NonLeafNodeString();
			rootNode = (NonLeafNodeString *) rootPage;

			// Find appropriate leaf node and insert
			traverseAndInsertString(rootNode, key, rid);

			// After the insertion, call fullNodeHandler if the node is full.
			if(strcmp(rootNode->keyArray[leafSize-1], "\0\0\0\0\0\0\0\0\0\0") != 0){
				fullNodeHandlerString(rootNode, NULL, rootPageNum, 1);
				this->numOfNodes++;
			}
		}

	}
	
	// Set root page number and unpin page	
	metadata->rootPageNo = this->rootPageNum;
	this->bufMgr->unPinPage(this->file, this->headerPageNum, 0);
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertToNode - helper function of insertEntry for int and double
// -----------------------------------------------------------------------------
template <typename T, typename L>
const void BTreeIndex::insertToNodeNumber(L * node, T keyValue, const RecordId rid) {	

	int leafSize;
	switch(this->attributeType) {
		case INTEGER: leafSize = INTARRAYLEAFSIZE; break;
		case DOUBLE: leafSize = DOUBLEARRAYLEAFSIZE; break;
		default: break;
	}

	int i = leafSize;

	// Push every values to the right when it's greater than key
	while (i > 0 && (keyValue < node->keyArray[i-1] || node->keyArray[i-1] == -1)) {
		if (node->keyArray[i-1] != -1) {
			node->keyArray[i] = node->keyArray[i-1];
			node->ridArray[i] = node->ridArray[i-1];
		}
		i--;
	}

	// Assign the record to the node
	node->keyArray[i] = keyValue;
	node->ridArray[i] = rid;
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertToNodeString - helper function of insertEntry for string
// -----------------------------------------------------------------------------
const void BTreeIndex::insertToNodeString(LeafNodeString * node, const void * keyValue, const RecordId rid) {	

	int leafSize = STRINGARRAYLEAFSIZE;

	int i = leafSize;

	// Push every values to the right when it's greater than key
	while (i > 0 && ((strcmp(((const char *) keyValue), node->keyArray[i-1]) < 0)
		 || strcmp( node->keyArray[i-1], "\0\0\0\0\0\0\0\0\0\0") == 0)) {
		if (strcmp( node->keyArray[i-1], "\0\0\0\0\0\0\0\0\0\0")  != 0) {
			strncpy(node->keyArray[i], node->keyArray[i-1], 10);
			node->ridArray[i] = node->ridArray[i-1];
		}	
		i--;
	}

	// Assign the record to the node
	strncpy(node->keyArray[i], (char *) keyValue, 10);
	node->ridArray[i] = rid;
}




// --------------------------------------------------------------------------------------
// BTreeIndex::fullNodeHandlerNumber - helper function of insertEntry for int and double
// --------------------------------------------------------------------------------------
template<typename L, typename NL>
void BTreeIndex::fullNodeHandlerNumber(L* leafType, NL* nonLeafType, void* currNode, NL *parentNode, PageId currPageNo, bool isLeaf, bool isRoot) {

	// Set nonleaf node size
	int nonleafSize;
	switch(this->attributeType) {
		case INTEGER: nonleafSize = INTARRAYNONLEAFSIZE; break;
		case DOUBLE: nonleafSize = DOUBLEARRAYNONLEAFSIZE; break;
		case STRING: nonleafSize = STRINGARRAYNONLEAFSIZE; break;
		default: break;
	}

	// Check if there already exists the parent to push up
	if (isRoot) {
		Page * newParentPage = new Page();
		this->bufMgr->allocPage(this->file, this->rootPageNum, newParentPage);
		parentNode = (NL*)newParentPage;
		for (int i=0; i < nonleafSize; i++) {
			parentNode->keyArray[i] = -1;
		}
		parentNode->level = 1;
	}

	
	// split page
	PageId rightPageNum;
	double middleKey;
	if (isLeaf) {
		L* currNodeLeaf = (L*)currNode;
		splitLeafNode(currNodeLeaf, middleKey, rightPageNum); // splitNode called
	}
	else{
		NL* currNodeNonLeaf = (NL*)currNode;
		splitNonLeafNode(currNodeNonLeaf, middleKey, rightPageNum);
	}

	// Push every values to the right when it's greater than key
	int nonLeafIndex = nonleafSize;
	while (nonLeafIndex > 0 && (middleKey < parentNode->keyArray[nonLeafIndex-1] || parentNode->keyArray[nonLeafIndex-1] == -1)) {
		if (parentNode->keyArray[nonLeafIndex-1] != -1) {
			parentNode->keyArray[nonLeafIndex] = parentNode->keyArray[nonLeafIndex-1];
			parentNode->pageNoArray[nonLeafIndex+1] = parentNode->pageNoArray[nonLeafIndex];
			parentNode->pageNoArray[nonLeafIndex] = parentNode->pageNoArray[nonLeafIndex-1];

		}
		nonLeafIndex--;
	}
	
	// Put the middle key to the parent node and connect parent node to left and 
	// right node
	parentNode->keyArray[nonLeafIndex] = middleKey;
	parentNode->pageNoArray[nonLeafIndex] = currPageNo;
	parentNode->pageNoArray[nonLeafIndex+1] = rightPageNum;
	
}


// -----------------------------------------------------------------------------
// BTreeIndex::fullNodeHandlerString - helper function of insertEntry for string
// -----------------------------------------------------------------------------
void BTreeIndex::fullNodeHandlerString(void* currNode, NonLeafNodeString *parentNode, PageId currPageNo, bool isLeaf) {


	int nonleafSize = STRINGARRAYNONLEAFSIZE;

	// Check if there already exists the parent to push up
	if (parentNode == NULL) {
		Page * newParentPage = new Page();
		this->bufMgr->allocPage(this->file, this->rootPageNum, newParentPage);
		parentNode = (NonLeafNodeString*)newParentPage;
		for (int i=0; i < nonleafSize; i++) {
			strncpy(parentNode->keyArray[i], "\0\0\0\0\0\0\0\0\0\0", 10);
		}
		parentNode->level = 1;
	}

	
	// Split the full node depending on the node is leaf or nonleaf
	PageId rightPageNum;
	char middleKey[10];
	if (isLeaf) {
		LeafNodeString* currNodeLeaf = (LeafNodeString*)currNode;
		splitLeafNodeString(currNodeLeaf, middleKey, rightPageNum); // splitNode called
	}
	else{
		NonLeafNodeString * currNodeNonLeaf = (NonLeafNodeString*)currNode;
		splitNonLeafNodeString(currNodeNonLeaf, middleKey, rightPageNum);
	}


	// Push every values to the right when it's greater than key
	int nonLeafIndex = nonleafSize;
	while(nonLeafIndex > 0 && ((strcmp((const char *) middleKey,parentNode->keyArray[nonLeafIndex -1]) < 0) 
		|| strcmp(parentNode->keyArray[nonLeafIndex-1],"\0\0\0\0\0\0\0\0\0\0") == 0)) { 
		if (strcmp(parentNode->keyArray[nonLeafIndex-1], "\0\0\0\0\0\0\0\0\0\0") != 0) {
			strncpy(parentNode->keyArray[nonLeafIndex], parentNode->keyArray[nonLeafIndex-1], 10);	
			parentNode->pageNoArray[nonLeafIndex+1] = parentNode->pageNoArray[nonLeafIndex];
			parentNode->pageNoArray[nonLeafIndex] = parentNode->pageNoArray[nonLeafIndex-1];

		}
		nonLeafIndex--;
	}

	// Put the middle key to the parent node and connect parent node to left and 
	// right node
	strncpy(parentNode->keyArray[nonLeafIndex],middleKey, 10);
	parentNode->pageNoArray[nonLeafIndex] = currPageNo;
	parentNode->pageNoArray[nonLeafIndex+1] = rightPageNum;

}


// -----------------------------------------------------------------------------
// BTreeIndex::splitLeafNode - helper function of insertEntry for int and double
// -----------------------------------------------------------------------------
template <typename T, typename L>
void BTreeIndex::splitLeafNode(L *& leftNode, T& middleKey, PageId &pid) { 

	// Set the leaf size based on type
	int leafSize;
	switch(this->attributeType) {
		case INTEGER: leafSize = INTARRAYLEAFSIZE; break;
		case DOUBLE: leafSize = DOUBLEARRAYLEAFSIZE; break;
		case STRING: leafSize = STRINGARRAYLEAFSIZE; break;
		default: break;
	}

	// After splitNode, the original node will be in left, while returned node will be in right
	// Allocate new page to be right child of root
	Page * rightPage = new Page();
	this->bufMgr->allocPage(this->file, pid, rightPage);
	L * rightNode = new L();
	rightNode = (L*)rightPage;

	// Get the middle value
	int middlePoint = (leafSize/2);
	middleKey = leftNode->keyArray[middlePoint];

	// Move everything of 2nd half of the full node to the right node
	for (int i=middlePoint, j=0; i < leafSize; i++, j++) {
		rightNode->keyArray[j] = leftNode->keyArray[i];
		rightNode->ridArray[j] = leftNode->ridArray[i];
	}

	// Set 2nd half of the full node to be the default value
	for (int i=middlePoint; i < leafSize; i++) {
		if (leafSize % 2 == 1 && i == middlePoint) {}
		else { rightNode->keyArray[i] = -1; }
		leftNode->keyArray[i] = -1;
	}

	// Set leaf page's right sibling
	if (leftNode->rightSibPageNo != 0) rightNode->rightSibPageNo = leftNode->rightSibPageNo;
	leftNode->rightSibPageNo = pid;

	this->numOfNodes += 1;

}


// -----------------------------------------------------------------------------
// BTreeIndex::splitLeafNodeString - helper function of insertEntry for string
// -----------------------------------------------------------------------------
void BTreeIndex::splitLeafNodeString(LeafNodeString *& leftNode, char* middleKey, PageId &pid) { 

	int leafSize = STRINGARRAYLEAFSIZE;

	// After splitNode, the original node will be in left, while returned node will be in right
	// Allocate new page to be right child of root
	Page * rightPage = new Page();
	this->bufMgr->allocPage(this->file, pid, rightPage);
	LeafNodeString * rightNode = new LeafNodeString();
	rightNode = (LeafNodeString*)rightPage;

	// Get the middle value
	int middlePoint = (leafSize/2);
	strncpy(middleKey, leftNode->keyArray[middlePoint], 10);

	// Move everything of 2nd half of the full node to the right node
	for (int i=middlePoint, j=0; i < leafSize; i++, j++) {
		//rightNode->keyArray[j] = leftNode->keyArray[i];
		strncpy(rightNode->keyArray[j], leftNode->keyArray[i],10);
		rightNode->ridArray[j] = leftNode->ridArray[i];
	}

	// Set 2nd half of the full node to be the default value
	for (int i=middlePoint; i < leafSize; i++) {
		strncpy(rightNode->keyArray[i], "\0\0\0\0\0\0\0\0\0\0", 10);
		strncpy(leftNode->keyArray[i],"\0\0\0\0\0\0\0\0\0\0",10);
	}

	// Set leaf page's right sibling
	if (leftNode->rightSibPageNo != 0) rightNode->rightSibPageNo = leftNode->rightSibPageNo;
	leftNode->rightSibPageNo = pid;

	this->numOfNodes += 1;

}



// --------------------------------------------------------------------------------------------
// BTreeIndex::splitNonLeafNode - helper function of fullNodeHandlerNumber for int and double
// -------------------------------------------------------------------------------------------
template <typename T, typename NL>
void BTreeIndex::splitNonLeafNode(NL *& leftNode, T& middleKey, PageId &pid) { 

	// Set node size based on types
	int nonleafSize;
	switch(this->attributeType) {
		case INTEGER: nonleafSize = INTARRAYNONLEAFSIZE; break;
		case DOUBLE: nonleafSize = DOUBLEARRAYNONLEAFSIZE; break;
		default: break;
	}

	
	// After splitNode, the original node will be in left, while returned node will be in right
	// Allocate new page to be right child of root
	Page * rightPage = new Page();
	this->bufMgr->allocPage(this->file, pid, rightPage);
	NL * rightNode = new NL();
	rightNode = (NL*)rightPage;

	// Get the middle value
	int middlePoint = nonleafSize/2;
	middleKey = leftNode->keyArray[middlePoint];

	// Move right side of the full node to rightNode
	for (int i=middlePoint, j=0; i < nonleafSize; i++, j++) {
		rightNode->keyArray[j] = leftNode->keyArray[i];
		rightNode->pageNoArray[j] = leftNode->pageNoArray[i];
	}

	// Set 2nd half of the full node to be the default value
	for (int i=middlePoint; i < nonleafSize; i++) {
		rightNode->keyArray[i] = -1;
		leftNode->keyArray[i] = -1;
	}

	// Increment total number of nodes 
	this->numOfNodes += 1;
}


// -----------------------------------------------------------------------------------------
// BTreeIndex::splitNonLeafNodeString - helper function of fullNodeHandlerString for string
// ------------------------------------------------------------------------------------------
void BTreeIndex::splitNonLeafNodeString(NonLeafNodeString *& leftNode, char* middleKey, PageId &pid) { 

	int leafSize = STRINGARRAYNONLEAFSIZE;

	// Allocate new page to be right child of root
	Page * rightPage = new Page();
	this->bufMgr->allocPage(this->file, pid, rightPage);
	NonLeafNodeString * rightNode = new NonLeafNodeString();
	rightNode = (NonLeafNodeString*)rightPage;

	// Get the middle value
	int middlePoint = (leafSize/2);
	strncpy(middleKey,leftNode->keyArray[middlePoint],10);

	// Move right side of the full node to rightNode
	for (int i=middlePoint, j=0; i < leafSize; i++, j++) {
		strncpy(rightNode->keyArray[j], leftNode->keyArray[i],10);
		rightNode->pageNoArray[j] = leftNode->pageNoArray[i];
	}

	// Set 2nd half of the full node to be the default value
	for (int i=middlePoint; i < leafSize; i++) {
		strncpy(rightNode->keyArray[i], "\0\0\0\0\0\0\0\0\0\0", 10);
		strncpy(leftNode->keyArray[i],"\0\0\0\0\0\0\0\0\0\0",10);
	}
	// Increment total number of nodes
	this->numOfNodes += 1;

}


// ---------------------------------------------------------------------------------------
// BTreeIndex::traverseAndInsertNumber - helper function of insertEntry for int and double
// ----------------------------------------------------------------------------------------
template <typename T, typename L, typename NL>
void BTreeIndex::traverseAndInsertNumber(L* leafType, NL* nonLeafType, NL* currNode, T key, const RecordId rid) { 

	// Set leaf and non leaf size based on type
	int leafSize;
	int nonleafSize;
	switch(this->attributeType) {
		case INTEGER: leafSize = INTARRAYLEAFSIZE; nonleafSize = INTARRAYNONLEAFSIZE; break;
		case DOUBLE: leafSize = DOUBLEARRAYLEAFSIZE; nonleafSize = DOUBLEARRAYNONLEAFSIZE; break;
		default: break;
	}

	// Find appropriate child node and read
	int i = 0;
	for(i = 0; i < nonleafSize; i++){
		if(currNode->pageNoArray[i+1] == 0 || currNode->keyArray[i] > key){
			break;
		}
	}

	Page * childPage = new Page();
	this->bufMgr->readPage(this->file, currNode->pageNoArray[i], childPage);

	void * childNode;
	//Recursively call traverse if the child is not the leaf's parent (level is 1)
	if (currNode->level != 1) {		
		childNode = (NL*)childPage;
		traverseAndInsertNumber(leafType, nonLeafType, (NL*)childNode, key, rid);

		// Split node if the node is full before inserting
		if (currNode->keyArray[nonleafSize] != -1) { 
			fullNodeHandlerNumber(leafType, nonLeafType, childNode, currNode, currNode->pageNoArray[i], 1, 0);
			this->numOfNodes++;
		}
	}
	// Insert record to the leaf node
	else {		
		childNode = (L*)childPage;
		switch(this->attributeType) {
			case INTEGER: insertToNodeNumber<int>((L*)childNode, key, rid); break;
			case DOUBLE: insertToNodeNumber<double>((L*)childNode, key, rid); break;
			default: break;
		}

		// Split node if the node is full after inserting
		if (((L*)childNode)->keyArray[leafSize-1] != -1) { 

			fullNodeHandlerNumber(leafType, nonLeafType, childNode, currNode, currNode->pageNoArray[i],1, 0);
			this->numOfNodes++;
		}
	}

}



// -------------------------------------------------------------------------------
// BTreeIndex::traverseAndInsertString - helper function of insertEntry for string
// -------------------------------------------------------------------------------
void BTreeIndex::traverseAndInsertString(NonLeafNodeString* currNode, const void* key, const RecordId rid) { 

	char* keyValue = (char*)key;

	// Find appropriate child node and read
	int i = 0;
	for(i = 0; i < STRINGARRAYNONLEAFSIZE; i++){
		if(currNode->pageNoArray[i+1] == 0 || strcmp(currNode->keyArray[i], keyValue) > 0){
			break;
		}
	}	
	Page * childPage = new Page();
	this->bufMgr->readPage(this->file, currNode->pageNoArray[i], childPage);

	void * childNode;

	//Recursively call traverse if the child is not the leaf's parent (level is 1)
	if (currNode->level != 1) {		// child is non-leaf
		childNode = (NonLeafNodeString*)childPage;

		traverseAndInsertString((NonLeafNodeString*)childNode, key, rid);

		if(strcmp(currNode->keyArray[STRINGARRAYNONLEAFSIZE], "\0\0\0\0\0\0\0\0\0\0") != 0) {
			// Split node if the node is full before inserting
			fullNodeHandlerString(childNode, currNode, currNode->pageNoArray[i], 1);
			this->numOfNodes++;
		}
	}
	// Insert record to the leaf node
	else {	
		childNode = (LeafNodeString*)childPage;
		insertToNodeString((LeafNodeString*)childNode, key, rid);
		if(strcmp(((LeafNodeString*) childNode)->keyArray[STRINGARRAYLEAFSIZE-1], "\0\0\0\0\0\0\0\0\0\0") != 0){
	
			fullNodeHandlerString(childNode, currNode, currNode->pageNoArray[i],1);
			this->numOfNodes++;

		}
	}

}


// ----------------------------------------------------------------------
// BTreeIndex::initializeInt - helper function of insertEntry for int
// ----------------------------------------------------------------------
const void BTreeIndex::initializeInt(LeafNodeInt* rootNode)
{
	
	int leafSize = INTARRAYLEAFSIZE;

	for (int i=0; i < leafSize; i++) {
		rootNode->keyArray[i] = -1;
	}
}

// ----------------------------------------------------------------------
// BTreeIndex::initializeDouble - helper function of insertEntry for double
// ----------------------------------------------------------------------
const void BTreeIndex::initializeDouble(LeafNodeDouble* rootNode)
{
	int leafSize = DOUBLEARRAYLEAFSIZE;

	for (int i=0; i < leafSize; i++) {
		rootNode->keyArray[i] = -1;
	}
}

// ----------------------------------------------------------------------
// BTreeIndex::initializeString - helper function of insertEntry for string
// ----------------------------------------------------------------------
const void BTreeIndex::initializeString(LeafNodeString* rootNode)
{
	int leafSize = STRINGARRAYLEAFSIZE;

	for (int i=0; i < leafSize; i++) {
		strncpy(rootNode->keyArray[i], "\0\0\0\0\0\0\0\0\0\0", 10);
	}
}

// --------------------------------------------------------------------------------
// BTreeIndex::startScanNumber - helper function of startScan for int and double
// --------------------------------------------------------------------------------
template <typename T, typename L, typename NL>
const void BTreeIndex::startScanNumber (L* leafType, NL* nonLeafType, T lowVal, T highVal)
{
	// Get the size of the node
	int nonleafSize;
	switch(this->attributeType) {
		case INTEGER:
			nonleafSize = INTARRAYNONLEAFSIZE; 
			break;
		case DOUBLE:
			nonleafSize = DOUBLEARRAYNONLEAFSIZE;
			break;
		default: break;
	}

	// Read root node
	Page* metaPage; 
	bufMgr->readPage(file, headerPageNum, metaPage); 

	IndexMetaInfo * metadata = (IndexMetaInfo *) metaPage;

	// Check if the lowval and highval are valid	
	if(lowVal > highVal) {
		throw BadScanrangeException();
	}

	scanExecuting = true;


	// Find the appropriate node to start scanning from 
	if(numOfNodes == 1){
		currentPageNum = metadata->rootPageNo;
		nextEntry = 0;
		return;
	} 
	else {
		// Get the root node.
		bufMgr->readPage(file, metadata->rootPageNo, currentPageData); 
		NL * currNode  = (NL *) currentPageData;
		currentPageNum = metadata->rootPageNo;	
	
		int index = 0;
		//Traverse down the tree
		while(currNode->level != 1){

			for(index = 0; index < nonleafSize; index++){ 	
				if(lowVal <= currNode->keyArray[index] && currNode->keyArray[index] != -1){
					//index++;
					break;
				}
			}

			bufMgr->readPage(file, currNode->pageNoArray[index], currentPageData);
			bufMgr->unPinPage(file, currentPageNum, false);
			currNode = (NL *) currentPageData;
			currentPageNum = currNode->pageNoArray[index];
		}

		// At the 1st level node 
		for(index = 0; index < nonleafSize; index++){ 	
			if(lowVal <= currNode->keyArray[index] && currNode->keyArray[index] != -1){
				break;
			}
		}

		// Read the correct leaf node that contains the start of the record
		bufMgr->readPage(file, currNode->pageNoArray[index], currentPageData);
		bufMgr->unPinPage(file, currentPageNum, false);
		currentPageNum = currNode->pageNoArray[index];
		nextEntry = 0;
	}
}





// -----------------------------------------------------------------------------
// BTreeIndex::startScanString: helper function of startScan for string	
// ----------------------------------------------------------------------------
const void BTreeIndex::startScanString(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
	// Check if the operations are valid
	if(lowOpParm == LT || lowOpParm == LTE || highOpParm == GT || highOpParm == GTE){
		throw BadOpcodesException();
	} 
	
	// Read root page
	Page* metaPage; 
	bufMgr->readPage(file, headerPageNum, metaPage); 

	IndexMetaInfo * metadata = (IndexMetaInfo *) metaPage;

	// Set scanner variables
	lowValString = (std::string) ((char*) lowValParm);
	highValString = (std::string) ((char*) highValParm);

	lowOp = lowOpParm;
	highOp = highOpParm;

	// Check if the lowval and highval are valid	
	if(lowValString > highValString){
		throw BadScanrangeException();
	}

	scanExecuting = true;

	// Find the appropriate node to start scanning from 
	if(numOfNodes == 1){
		currentPageNum = metadata->rootPageNo;
		nextEntry = 0;
		return;
	} 
	else {
		// Get the root node.
		bufMgr->readPage(file, metadata->rootPageNo, currentPageData); 
		NonLeafNodeString * currNode  = (NonLeafNodeString *) currentPageData;
		currentPageNum = metadata->rootPageNo;	
	
		int index = 0;

		//Traverse down the tree
		while(currNode->level != 1){

			for(index = 0; index < STRINGARRAYNONLEAFSIZE; index++){ 	
				if(lowValString <= (std::string) currNode->keyArray[index] &&	
					(strcmp(currNode->keyArray[index], "\0\0\0\0\0\0\0\0\0\0") != 0)){
					break;
				}
			}

			bufMgr->readPage(file, currNode->pageNoArray[index], currentPageData);
			bufMgr->unPinPage(file, currentPageNum, false);
			currNode = (NonLeafNodeString *) currentPageData;
			currentPageNum = currNode->pageNoArray[index];
		}

		// At the 1st level node 
		for(index = 0; index < STRINGARRAYNONLEAFSIZE; index++){ 	
			if(lowValString <= (std::string) currNode->keyArray[index] &&	
				(strcmp(currNode->keyArray[index], "\0\0\0\0\0\0\0\0\0\0")) != 0){
				//index++;
				break;
			}
		}

		// Read the leaf node that contains the first record to be scanned
		bufMgr->readPage(file, currNode->pageNoArray[index], currentPageData);
		bufMgr->unPinPage(file, currentPageNum, false);
		currentPageNum = currNode->pageNoArray[index];
		nextEntry = 0;
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------
const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

	// Check if the operations are valid
	if(lowOpParm == LT || lowOpParm == LTE || highOpParm == GT || highOpParm == GTE){
		throw BadOpcodesException();
	} 
	
	lowOp = lowOpParm;
	highOp = highOpParm;

	// Specify leaf and nonleaf types
	LeafNodeInt* LEAFINTEGER = new LeafNodeInt();
	NonLeafNodeInt * NONLEAFINTEGER = new NonLeafNodeInt();
	LeafNodeDouble* LEAFDOUBLE = new LeafNodeDouble();
	NonLeafNodeDouble * NONLEAFDOUBLE = new NonLeafNodeDouble();
	LeafNodeString* LEAFSTRING = new LeafNodeString();
	NonLeafNodeString * NONLEAFSTRING = new NonLeafNodeString();

	// Call start scan based on the data type
	switch(this->attributeType) {
		case INTEGER:
			lowValInt = *((int *) lowValParm);
			highValInt = *((int*) highValParm);
			startScanNumber(LEAFINTEGER, NONLEAFINTEGER, lowValInt, highValInt);
			break;
		case DOUBLE:
			lowValDouble = *((double *) lowValParm);
			highValDouble = *((double*) highValParm);
			startScanNumber(LEAFDOUBLE, NONLEAFDOUBLE, lowValDouble, highValDouble);
			break;
		case STRING:
			startScanString(lowValParm,lowOpParm,highValParm,highOpParm);
			break;	
		default: break;
	}

	// Delete unecessary nodes
	delete(LEAFINTEGER);
	delete(NONLEAFINTEGER);
	delete(LEAFDOUBLE);
	delete(NONLEAFDOUBLE);
	delete(LEAFSTRING);
	delete(NONLEAFSTRING);;

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNextNumber - helper function of scanNext for int and double
// -----------------------------------------------------------------------------
template <typename T, typename L>
const void BTreeIndex::scanNextNumber(L* leafType, RecordId& outRid, T lowVal, T highVal)
{

	// Set leaf size based on type
	int leafSize;
	switch(this->attributeType) {
		case INTEGER: leafSize = INTARRAYLEAFSIZE; break;
		case DOUBLE: leafSize = DOUBLEARRAYLEAFSIZE; break;
		default: break;
	}
			
	if(!scanExecuting){
		throw ScanNotInitializedException();
	}

	// Read root node
	bufMgr->readPage(file, currentPageNum, currentPageData); 
	L * currNode = (L *) currentPageData;

	// Find the very first value's index that needs to be scanned from
	if(startScanIndex == -1){
		int i = 0;
		for(i = nextEntry; i < leafSize; i++){

			if(lowVal <= currNode->keyArray[i]){
				
				if((lowOp == GTE) && (lowVal == currNode->keyArray[i])){
					startScanIndex = i;
					break;	
				}
				else if((lowOp == GT) && (lowVal == currNode->keyArray[i])) {
					startScanIndex = i+1;
					break;
				}
				else{
					startScanIndex = i;
					break;
				}
			} 
		}
		nextEntry = startScanIndex;
	}
	
	bool notFound = true;
	// Find the next value that needs to be scanned
	while(notFound){

		//Case if we're at the end of the leaf node
		if(currNode->keyArray[nextEntry] == -1){
			PageId siblingNode = currNode->rightSibPageNo;
			bufMgr->unPinPage(file, currentPageNum, false);	
		
			nextEntry = 0;
			
			// When we're at the very right of the node, end the scan
			if(siblingNode == 0){
				throw IndexScanCompletedException();
			}
			
			// Set current node 
			currentPageNum = siblingNode;
			bufMgr->readPage(file, currentPageNum, currentPageData);
			currNode = (L*)currentPageData;
		}
		
		//If the value of the element is passing the high value, end the scan
		else if (currNode->keyArray[nextEntry] > highVal){
			throw IndexScanCompletedException();
		}

		// Case if the value of the node element still less than the high value 
		else if(highVal >= currNode->keyArray[nextEntry]){
			if((highOp == LTE) && (highVal == currNode->keyArray[nextEntry])){
				//throw IndexScanCompletedException();
				outRid = currNode->ridArray[nextEntry];
				notFound = false;
				nextEntry++;
			}
			else if((highOp == LT) && (highVal == currNode->keyArray[nextEntry])){
				throw IndexScanCompletedException();
			}
			else{ 
				outRid = currNode->ridArray[nextEntry];
				notFound = false;
				nextEntry++;
			}	
		}
	}

}


// -----------------------------------------------------------------------------
// BTreeIndex::scanNextString - Helper function of scanNext for string
// -----------------------------------------------------------------------------
const void BTreeIndex::scanNextString(RecordId& outRid) 
{
			
	if(!scanExecuting){
		throw ScanNotInitializedException();
	}

	// Read root node
	bufMgr->readPage(file, currentPageNum, currentPageData); 
	LeafNodeString * currNode = (LeafNodeString *) currentPageData;

	// Find appropriate starting element value of the node
	if(startScanIndex == -1){
		int i = 0;
		for(i = nextEntry; i < INTARRAYLEAFSIZE; i++){

			if(strncmp(lowValString.c_str(), currNode->keyArray[i],10) <= 0){	
				if((lowOp == GTE) && (strncmp(currNode->keyArray[i],lowValString.c_str(),10) == 0)){
					startScanIndex = i;
					break;	
				}
				else if ((lowOp == GT) && (strncmp(lowValString.c_str(),currNode->keyArray[i],10) == 0)){
					startScanIndex = i+1;
					break;
				}
				else{
					startScanIndex = i;
					break;
				}
			} 
		}
		nextEntry = startScanIndex;
	}
	
	bool notFound = true;

	
	// Find the next value that needs to be scanned
	while(notFound){

		//Case if we're at the end of the leaf node
		if(strcmp(currNode->keyArray[nextEntry],"\0\0\0\0\0\0\0\0\0\0") == 0){
			PageId siblingNode = currNode->rightSibPageNo;
			bufMgr->unPinPage(file, currentPageNum, false);	
		
			nextEntry = 0;
	
			// When we're at the very right of the node, end the scan
			if(siblingNode == 0){
				throw IndexScanCompletedException();
			}
			// Set current node
			currentPageNum = siblingNode;
			bufMgr->readPage(file, currentPageNum, currentPageData);
			currNode = (LeafNodeString*)currentPageData;
		}

		//If the value of the element is passing the high value, end the scan
		else if ((std::string)currNode->keyArray[nextEntry] > highValString){
			throw IndexScanCompletedException();
		}

		// Case if the value of the node element still less than the high value 
		else if(strncmp(highValString.c_str(),currNode->keyArray[nextEntry],10) >= 0){
			if((highOp == LTE) && (strncmp(currNode->keyArray[nextEntry],highValString.c_str(),10) == 0)){
				outRid = currNode->ridArray[nextEntry];
				notFound = false;
				nextEntry++;
			}
			else if((highOp == LT) && (strncmp(currNode->keyArray[nextEntry],highValString.c_str(),10) == 0)){	
				throw IndexScanCompletedException();
			}
			else{
				outRid = currNode->ridArray[nextEntry];
				notFound = false;
				nextEntry++;
			}	
		}
	}

}




// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
	LeafNodeInt * LEAFINTEGER = new LeafNodeInt();
	LeafNodeDouble * LEAFDOUBLE = new LeafNodeDouble();
	
	switch(this->attributeType) {
		case INTEGER:
			scanNextNumber(LEAFINTEGER, outRid, lowValInt, highValInt);
			break;
		case DOUBLE:
			scanNextNumber(LEAFDOUBLE, outRid, lowValDouble, highValDouble);
			break;
		case STRING:
			scanNextString(outRid);
			break;
		default: break;
	}

	delete(LEAFINTEGER);
	delete(LEAFDOUBLE);

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
	if(scanExecuting == false){
		throw ScanNotInitializedException();
	}
	scanExecuting = false;
	startScanIndex = -1;
}


// -----------------------------------------------------------------------------
// BTreeIndex::printTree (for debugging)
// -----------------------------------------------------------------------------
//
void BTreeIndex::printTree(){

	std::queue<PageId> q;

	Page* metaPage; 
	bufMgr->readPage(file, headerPageNum, metaPage); 
	IndexMetaInfo * metadata = (IndexMetaInfo *) metaPage;


	PageId currPageNum;
	LeafNodeString * currLeafNode;
	Page* currPageData;
	if(numOfNodes == 1){
		bufMgr->readPage(file, metadata->rootPageNo, currPageData); 
		currLeafNode  = (LeafNodeString *) currPageData;
		int i = 0;
		for(i = 0; i < STRINGARRAYNONLEAFSIZE; i++){
			if(strcmp(currLeafNode->keyArray[i], "\0\0\0\0\0\0\0\0\0\0") != 0){
				std::cout << currLeafNode->keyArray[i] << ", ";
			}else{
				break;
			}
		}
		std::cout << "\n";
		return;
	}


	bufMgr->readPage(file, metadata->rootPageNo, currPageData); 
	NonLeafNodeString * currNode  = (NonLeafNodeString *) currPageData;
		
	//Only works for one level tree as for now	
	while(currNode->level >=1){

		int i = 0;
		for(i = 0; i < STRINGARRAYNONLEAFSIZE; i++){
			if(strcmp(currNode->keyArray[i], "\0\0\0\0\0\0\0\0\0\0") != 0){
				std::cout << currNode->keyArray[i] << ", ";
				q.push(currNode->pageNoArray[i]);
			}else{
				break;
			}
		}

		while(!q.empty()){
			currPageNum = q.front();
			q.pop();
			bufMgr->readPage(file, currPageNum, currPageData);
			currLeafNode = (LeafNodeString *) currPageData;
		
			for(i = 0; i < STRINGARRAYLEAFSIZE; i++){
				if(strcmp(currLeafNode->keyArray[i], "\0\0\0\0\0\0\0\0\0\0") != 0){
					std::cout << (std::string) currLeafNode->keyArray[i] << ", ";		
				}
				else{
					break;
				}
			}	

		}
		
	}

}
}
