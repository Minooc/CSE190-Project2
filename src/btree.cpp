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

//#define DEBUG

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

	std::cout << "CONST: scanexecuting is " << scanExecuting << std::endl;
	// Insert all entries in relation into index
	FileScan fscan(relationName, this->bufMgr);
	try {
		while (true) {
			RecordId rid;
			fscan.scanNext(rid);
			std::string record = fscan.getRecord();
			int * key = ((int*)record.c_str() + attrByteOffset);
			this->insertEntry(key, rid);
		}
	}
	catch(EndOfFileException &e) {	}


}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	std::cout << "destructor called" << std::endl;
	this->file->~File();
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	int keyValue = *(int*)key;

	Page * metadataPage = new Page();
	this->bufMgr->readPage(this->file, this->headerPageNum, metadataPage);
	IndexMetaInfo * metadata = new IndexMetaInfo();
	metadata = (IndexMetaInfo*)metadataPage;

	/*  Tree is empty (first insertion) */
	if (metadata->rootPageNo == 0) {
		// Allocate root page
		Page * rootPage = new Page();
		this->bufMgr->allocPage(this->file, this->rootPageNum, rootPage);
		metadata->rootPageNo = this->rootPageNum;
		this->numOfNodes++;

		LeafNodeInt * rootNode = new LeafNodeInt();
		rootNode = (LeafNodeInt*)rootPage;
		for (int i=0; i < INTARRAYLEAFSIZE; i++) {
			rootNode->keyArray[i] = -1;
		}
		rootNode->keyArray[0] = keyValue;
		rootNode->ridArray[0] = rid;
		this->bufMgr->unPinPage(this->file, this->rootPageNum, 1);
	}

	/* Tree is not empty (every cases other than first insertion) */
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
			LeafNodeInt * rootNode = new LeafNodeInt();
			rootNode = (LeafNodeInt*)rootPage;


			// After the insertion, call fullNodeHandler if the node is full.
			insertToNode(rootNode,key,rid);
			if (rootNode->keyArray[INTARRAYLEAFSIZE-1] != -1) { 
				fullNodeHandler(rootNode, NULL, this->rootPageNum,1);
				this->numOfNodes++;
			}
		}

		/* This block is the general case where there is more than one node */
		else {	// more than one node

	//std::cout << "inserting " << keyValue << std::endl;
			Page * rootPage;
			bufMgr->readPage(file, metadata->rootPageNo, rootPage);  
			NonLeafNodeInt * rootNode = new NonLeafNodeInt();
			rootNode = (NonLeafNodeInt*)rootPage;

			traverse(rootNode, key, rid);
			if (rootNode->keyArray[INTARRAYNONLEAFSIZE-1] != -1) {
				std::cout << "Full when inserting " << keyValue << std::endl;
				fullNodeHandler(rootNode, NULL, this->rootPageNum, 1);
				this->numOfNodes++;
			}
/*
 *			traverse from root: traverse(currNode)
 *			currNode is initially root.
 *
 *			In traverse(), find the child to traverse. traverse(childNode)
 *			traverse() has two instances: currNode and childNode. In the next call, currNode will be the parent of childNode.
 *
 *			traverse() will be implemented as following:
 *			Get childNode
 *			If no childNode, return
 *			traverse(childNode)
 *			check if childNode is full (if exists)
 *				if so, call fullNodeHandler(currNode, childNode, ... )
 */
			
		}

	}
	
	metadata->rootPageNo = this->rootPageNum;
	this->bufMgr->unPinPage(this->file, this->headerPageNum, 0);
//	if (keyValue == 0)	testPrint();
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertToNode - helpfer function of insertEntry
// -----------------------------------------------------------------------------
const void BTreeIndex::insertToNode(LeafNodeInt * node, const void *key, const RecordId rid) {
	
	int keyValue = *(int*)key;
	int i = INTARRAYLEAFSIZE;

	// Push every values to the right when it's greater than key
	while (i > 0 && (keyValue < node->keyArray[i-1] || node->keyArray[i-1] == -1)) {
		if (node->keyArray[i-1] != -1) {
			node->keyArray[i] = node->keyArray[i-1];
			node->ridArray[i] = node->ridArray[i-1];
		}
		i--;
	}

	node->keyArray[i] = keyValue;
	node->ridArray[i] = rid;
//	std::cout << rid.page_number;
}

// -----------------------------------------------------------------------------
// BTreeIndex::fullNodeHandler - helper function of insertEntry
// -----------------------------------------------------------------------------
void BTreeIndex::fullNodeHandler(void* currNode, NonLeafNodeInt *parentNode, PageId currPageNo, bool isLeaf) {

	// Check if there already exists the parent to push up
	if (parentNode == NULL) { 
		Page * newParentPage = new Page();
		this->bufMgr->allocPage(this->file, this->rootPageNum, newParentPage);
		parentNode = (NonLeafNodeInt *)newParentPage;
		for (int i=0; i < INTARRAYNONLEAFSIZE; i++) {
			parentNode->keyArray[i] = -1;
		}
		parentNode->level = 1;
	}

	
	// split page
	PageId rightPageNum;
	int middleKey;
	if (isLeaf) {
		LeafNodeInt* currNodeLeaf = (LeafNodeInt*)currNode;
		splitLeafNode(currNodeLeaf, middleKey, rightPageNum); // splitNode called
//		parentNode->level ++;
	}
	else{
		NonLeafNodeInt* currNodeNonLeaf = (NonLeafNodeInt*)currNode;
		splitNonLeafNode(currNodeNonLeaf, middleKey, rightPageNum);
//		parentNode->level ++;
	}
	std::cout << "middle key is " << middleKey << std::endl;

	// Set attribute for new root
	// Push every values to the right when it's greater than key
	int nonLeafIndex = INTARRAYNONLEAFSIZE;
	while (nonLeafIndex > 0 && (middleKey < parentNode->keyArray[nonLeafIndex-1] || parentNode->keyArray[nonLeafIndex-1] == -1)) {
		if (parentNode->keyArray[nonLeafIndex-1] != -1) {
			parentNode->keyArray[nonLeafIndex] = parentNode->keyArray[nonLeafIndex-1];
			parentNode->pageNoArray[nonLeafIndex+1] = parentNode->pageNoArray[nonLeafIndex];
			parentNode->pageNoArray[nonLeafIndex] = parentNode->pageNoArray[nonLeafIndex-1];

		}
		nonLeafIndex--;
	}
	std::cout << "leaf index is " << nonLeafIndex << std::endl;
	parentNode->keyArray[nonLeafIndex] = middleKey;
	parentNode->pageNoArray[nonLeafIndex] = currPageNo;
	parentNode->pageNoArray[nonLeafIndex+1] = rightPageNum;
	

	std::cout << "rightsibno is " << ((LeafNodeInt*)currNode)->rightSibPageNo << std::endl;

	for (int i=0; i <11; i++) std::cout << parentNode->pageNoArray[i] << ' ';
//	testPrint();

}

// Just for test. Print whatever you want
void BTreeIndex::testPrint() {

	std::cout << "testPrint caled" << std::endl;
				/* TEST */

				Page * testRoot = new Page();
				this->bufMgr->readPage(this->file, this->rootPageNum, testRoot);
				NonLeafNodeInt * testRootNode = new NonLeafNodeInt();
				testRootNode = (NonLeafNodeInt*)testRoot;

				// get left page
				Page * testLeft = new Page();
				this->bufMgr->readPage(this->file, testRootNode->pageNoArray[0], testLeft);
				LeafNodeInt * testLeftNode = new LeafNodeInt();
				testLeftNode = (LeafNodeInt*)testLeft;
				
				Page * testRight = new Page();
				this->bufMgr->readPage(this->file, testRootNode->pageNoArray[1], testRight);
				LeafNodeInt * testRightNode = new LeafNodeInt();
				testRightNode = (LeafNodeInt*)testRight;

				std::cout << "MIDDLE KEY IS " << testRootNode->keyArray[0] << std::endl;
				std::cout << "Level of root is " << testRootNode->level << std::endl;
				std::cout << "Number of nodes is " << numOfNodes << std::endl;

				std::cout << "PRINTING ROOT" << std::endl;
				for (int i=0; i < INTARRAYNONLEAFSIZE; i++) {
					if (testRootNode->keyArray[i] != -1) std::cout << testRootNode->keyArray[i] << ' ';
				}
				std::cout << std::endl;

			
				std::cout << "PRINTING LEFTEST (index 0)" << std::endl;
				for (int i=0; i < INTARRAYLEAFSIZE; i++) {
					if (testLeftNode->keyArray[i] != -1) {
						std::cout << testLeftNode->keyArray[i] << ' ';
					}	
			}
			std::cout << "rightsib no is  " << testLeftNode->rightSibPageNo << std::endl;
				std::cout << std::endl;
				std::cout << "PRINTING Page with index 1" << std::endl;
				for (int i=0; i < INTARRAYLEAFSIZE; i++) {
					if (testRightNode->keyArray[i] != -1) {
						std::cout << testRightNode->keyArray[i] << ' ';
						std::cout << "page_num " << testRightNode->ridArray[i].page_number << ' ';
					}	
				}
			std::cout << "rightsib no is  " << testRightNode->rightSibPageNo << std::endl;
		for (int j=2; j < 100; j++) {
			if (testRootNode->pageNoArray[j] == 0) { std::cout << "end of root " << std::endl; break; }
 Page * test3Right = new Page();
			std::cout << "now reading the page with pageno " << testRootNode->pageNoArray[j] << std::endl;
this->bufMgr->readPage(this->file, testRootNode->pageNoArray[j], test3Right);
LeafNodeInt * test3RightNode = new LeafNodeInt();
test3RightNode = (LeafNodeInt*)test3Right;
				std::cout << std::endl;
				std::cout << "PRINTING Page with index " << j << std::endl;
				for (int i=0; i < INTARRAYLEAFSIZE; i++) {
					if (test3RightNode->keyArray[i] != -1) {
						std::cout << test3RightNode->keyArray[i] << ' ';
					}	

				}
				std::cout << "right sib no is " << test3RightNode->rightSibPageNo << std::endl;
		}

		for (int i=0; i < 10; i++ ) std::cout << testRootNode->pageNoArray[i] << ' ';
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitLeafNode - helper function of insertEntry
// -----------------------------------------------------------------------------
void BTreeIndex::splitLeafNode(LeafNodeInt *& leftNode, int& middleKey, PageId &pid) { 
	// After splitNode, the original node will be in left, while returned node will be in right
/*
	for (int i=0; i < INTARRAYLEAFSIZE; i++) {
		std::cout << "left node pid is " << leftNode->ridArray[i].page_number;
	}
*/
	// Allocate new page to be right child of root
	Page * rightPage = new Page();
	this->bufMgr->allocPage(this->file, pid, rightPage);
	LeafNodeInt * rightNode = new LeafNodeInt();
	rightNode = (LeafNodeInt*)rightPage;

	// get the middle value
	int middlePoint = INTARRAYLEAFSIZE/2;
	middleKey = leftNode->keyArray[middlePoint];
	std::cout << "Splitting with midpoint " << middleKey <<std::endl;
	// move right side of original node to rightNode
	for (int i=middlePoint, j=0; i < INTARRAYLEAFSIZE; i++, j++) {
		rightNode->keyArray[j] = leftNode->keyArray[i];
		rightNode->ridArray[j] = leftNode->ridArray[i];
	}
	for (int i=middlePoint; i < INTARRAYLEAFSIZE; i++) {
		rightNode->keyArray[i] = -1;
		leftNode->keyArray[i] = -1;
	}

	if (leftNode->rightSibPageNo != 0) rightNode->rightSibPageNo = leftNode->rightSibPageNo;
	leftNode->rightSibPageNo = pid;

/*
	std::cout << "LEFTNODE PRINTING\n";
	for (int i=0; i <middlePoint; i++) std::cout << leftNode->ridArray[i].page_number << ' '; 
	std::cout << "RIGHTNODE PRINTING\n";
	for (int i=0; i <middlePoint; i++) std::cout << rightNode->ridArray[i].page_number << ' '; 
*/	// Increment total number of nodes 
	this->numOfNodes += 1;


}

// -----------------------------------------------------------------------------
// BTreeIndex::splitNonLeafNode - helper function of insertEntry
// -----------------------------------------------------------------------------
void BTreeIndex::splitNonLeafNode(NonLeafNodeInt *& leftNode, int& middleKey, PageId &pid) { // TO DO: Add implementation 

	Page * rightPage = new Page();
	this->bufMgr->allocPage(this->file, pid, rightPage);
	NonLeafNodeInt * rightNode = new NonLeafNodeInt();
	rightNode = (NonLeafNodeInt*)rightPage;

	// get the middle value
	int middlePoint = INTARRAYLEAFSIZE/2;
	middleKey = leftNode->keyArray[middlePoint];

	// move right side of original node to rightNode
	for (int i=middlePoint, j=0; i < INTARRAYNONLEAFSIZE; i++, j++) {
		rightNode->keyArray[j] = leftNode->keyArray[i];
		rightNode->pageNoArray[j] = leftNode->pageNoArray[i];
	}
	for (int i=middlePoint; i < INTARRAYNONLEAFSIZE; i++) {
		rightNode->keyArray[i] = -1;
		leftNode->keyArray[i] = -1;
	}

	// Increment total number of nodes 
	this->numOfNodes += 1;
}

// -----------------------------------------------------------------------------
// BTreeIndex::traverse - helper function of insertEntry
// -----------------------------------------------------------------------------
void BTreeIndex::traverse(NonLeafNodeInt* currNode, const void* key, const RecordId rid) { 

	int keyValue = *(int*)key;

	int i = 0;
	for(i = 0; i < INTARRAYNONLEAFSIZE; i++){
		if(currNode->pageNoArray[i+1] == 0 || currNode->keyArray[i] > keyValue){
			break;
		}
	}

	Page * childPage = new Page();
//	std::cout << "Seeking " << i << "th index with pag no" << currNode->pageNoArray[i] << std::endl;
	this->bufMgr->readPage(this->file, currNode->pageNoArray[i], childPage);

	void * childNode;
	if (currNode->level != 1) {		// child is non-leaf
		childNode = (NonLeafNodeInt*)childPage;

		traverse((NonLeafNodeInt*)childNode, key, rid);

		if (currNode->keyArray[INTARRAYNONLEAFSIZE] != -1) { 
			fullNodeHandler(childNode, currNode, currNode->pageNoArray[i], 1);
			this->numOfNodes++;
		}
	}
	else {		// child is leaf
		childNode = (LeafNodeInt*)childPage;
		insertToNode((LeafNodeInt*)childNode, key, rid);
		if (((LeafNodeInt*)childNode)->keyArray[INTARRAYLEAFSIZE-1] != -1) { 

/*			for (int i=0; i< INTARRAYLEAFSIZE; i++) {
				std::cout << "At index " << i << ": " << ((LeafNodeInt*)childNode)->keyArray[i] << std::endl;
			}
*/
//			testPrint();
//			std::cout << "Full when inserting " << keyValue << std::endl;
			fullNodeHandler(childNode, currNode, currNode->pageNoArray[i],1);
			this->numOfNodes++;

			//testPrint();

		}
	}





/*
	if(currNode->level == 1){

		bufMgr->unPinPage(file, currNodeId, false);
		Page * leafPage;
		bufMgr->readPage(file, currNode->pageNoArray[i],leafPage);
		return (LeafNodeInt *) leafPage; 
	}
	else{	
		// Start scanning the node index
		int i = 0;
		for(i = 0; i < INTARRAYNONLEAFSIZE; i++){
			if(currNode->keyArray[i] != 0 && currNode->keyArray[i] > key){
				break;
			}
		}
		//PageId childPageNo = parentNode->pageNoArray[i];
		bufMgr->unPinPage(file, currNodeId, false);
		Page * parent;
		bufMgr->readPage(file, currNode->pageNoArray[i], parent);
		return traverse(key, (NonLeafNodeInt *) parent, currNode->pageNoArray[i]);
*/
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{

	//std::cout << "In startScan\n";

	// Operator: LT, LTE, GTE, GT
	if(lowOpParm == LT || lowOpParm == LTE || highOpParm == GT || highOpParm == GTE){
		throw BadOpcodesException();
	} 
	
	Page* metaPage; 
	bufMgr->readPage(file, headerPageNum, metaPage); 

	IndexMetaInfo * metadata = (IndexMetaInfo *) metaPage;

	lowValInt = *((int *) lowValParm);
	highValInt = *((int*) highValParm);

	lowOp = lowOpParm;
	highOp = highOpParm;
	
	if(lowValInt > highValInt) {
		throw BadScanrangeException();
	}

	scanExecuting = true;

	std::cout << "lowValParm: " << lowValInt << ", highValParm: " << highValInt << "\n";

	if(numOfNodes == 1){
		std::cout << "numOfNodes = 1\n";
		currentPageNum = metadata->rootPageNo;
		nextEntry = 0;
		return;
	} 


	else {
		std::cout << "num of nodes = " << numOfNodes << "\n";
		// Get the root node.
		bufMgr->readPage(file, metadata->rootPageNo, currentPageData); 
		NonLeafNodeInt * currNode  = (NonLeafNodeInt *) currentPageData;
		currentPageNum = metadata->rootPageNo;	
		std::cout <<"curNode->level: " <<  currNode->level << "\n";		
	
		int index = 0;
		while(currNode->level != 1){

			std::cout <<"curNode->level: " <<  currNode->level << "\n";		
			for(index = 0; index < INTARRAYNONLEAFSIZE; index++){ 	
				if(lowValInt <= currNode->keyArray[index] && currNode->keyArray[index] != -1){
					//index++;
					break;
				}
			}

			bufMgr->readPage(file, currNode->pageNoArray[index], currentPageData);
			bufMgr->unPinPage(file, currentPageNum, false);
			currNode = (NonLeafNodeInt *) currentPageData;
			currentPageNum = currNode->pageNoArray[index];
		}

		// At the 1st level node 
		for(index = 0; index < INTARRAYNONLEAFSIZE; index++){ 	
			std::cout << currNode->keyArray[index] << ", ";
			if(lowValInt <= currNode->keyArray[index] && currNode->keyArray[index] != -1){
				//index++;
				break;
			}
		}

		std::cout << "\n";

		std::cout << "Index: " << index << "\n";	
		bufMgr->readPage(file, currNode->pageNoArray[index], currentPageData);
		bufMgr->unPinPage(file, currentPageNum, false);
		//currNode = (NonLeafNodeInt *) currPage;
		currentPageNum = currNode->pageNoArray[index];
		nextEntry = 0;

		
		std::cout << "Printing leaf nodes\n";
		LeafNodeInt * currNode2 = (LeafNodeInt *) currentPageData;
		for(index = 0; index < INTARRAYLEAFSIZE; index++){
			if(currNode2->keyArray[index] != -1)
				std::cout << currNode2->keyArray[index] << ", ";
		}	
	}

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
			
	if(!scanExecuting){
		throw ScanNotInitializedException();
	}

	bufMgr->readPage(file, currentPageNum, currentPageData); 
	LeafNodeInt * currNode = (LeafNodeInt *) currentPageData;

	/*
	int index;	
	for(index = 0; index < INTARRAYLEAFSIZE; index++){
		if(currNode->keyArray[index] != -1)
			std::cout << currNode->keyArray[index] << ", ";
	}*/

	if(startScanIndex == -1){
		int i = 0;
		for(i = nextEntry; i < INTARRAYLEAFSIZE; i++){

			if(lowValInt <= currNode->keyArray[i]){
				
				if((lowOp == GTE) && (lowValInt == currNode->keyArray[i])){
					startScanIndex = i;
					break;	
				}
				else if((lowOp == GT) && (lowValInt == currNode->keyArray[i])) {
					startScanIndex = i+1;
					break;
				}
				else{
					startScanIndex = i;
					break;
				}
			} 
		}
		//std::cout << "Broke out of the loop at i= " << i << "\n";
		nextEntry = startScanIndex;
	}
	
//	std::cout << "nextEntry: " << nextEntry << "\n";

	bool notFound = true;
	while(notFound){

	
/*	
		if(currNode->ridArray[nextEntry].page_number == 0){
			std::cout << "Scan completed page_number = 0\n";
			std::cout << "at index: " << nextEntry << "\n";
			throw IndexScanCompletedException();	
		}
*/
		if(currNode->keyArray[nextEntry] == -1){
			std::cout << "calling rightsibpagno\n";
			PageId siblingNode = currNode->rightSibPageNo;
			bufMgr->unPinPage(file, currentPageNum, false);	
		
			nextEntry = 0;
			if(siblingNode == 0){
				//std::cout << "Scan completed siblingNode = 0\n";
				throw IndexScanCompletedException();
			}

			currentPageNum = siblingNode;
			std::cout << "rigth sib no is " << currentPageNum << std::endl;
			bufMgr->readPage(file, currentPageNum, currentPageData);
			currNode = (LeafNodeInt*)currentPageData;
		}

		else if (currNode->keyArray[nextEntry] > highValInt){
			//std::cout << "Scan completed keyArray[nextEntry] > highValInt\n";
			//std::cout << "at index: " << nextEntry << "\n";
			throw IndexScanCompletedException();
		}

		else if(highValInt >= currNode->keyArray[nextEntry]){
			//std::cout << "highValInt: " << highValInt << ", keyArray: " << currNode->keyArray[nextEntry];
			//std::cout << "\n";
			if((highOp == LTE) && (highValInt == currNode->keyArray[nextEntry])){
				//throw IndexScanCompletedException();
				outRid = currNode->ridArray[nextEntry];
				notFound = false;
				nextEntry++;
			}
			else if((highOp == LT) && (highValInt == currNode->keyArray[nextEntry])){
				//std::cout << "Scan completed highValInt = keyArray[nextEntry]\n";
				//std::cout << "at index: " << nextEntry << "\n";
				throw IndexScanCompletedException();
			}
			else{ 
				outRid = currNode->ridArray[nextEntry];
//				std::cout << "Found: " << currNode->keyArray[nextEntry] << "\n";
				//std::cout << "page_number: " << outRid.page_number; 
				notFound = false;
				nextEntry++;
			}	
		}
	}
	//std::cout << "\nOut of scanNext()\n";

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


void BTreeIndex::printTree(){

	std::cout << "PRINTING TREE..\n";	
	std::queue<PageId> q;

	Page* metaPage; 
	bufMgr->readPage(file, headerPageNum, metaPage); 
	IndexMetaInfo * metadata = (IndexMetaInfo *) metaPage;


	PageId currPageNum;
	LeafNodeInt * currLeafNode;
	Page* currPageData;
	if(numOfNodes == 1){
		bufMgr->readPage(file, metadata->rootPageNo, currPageData); 
		currLeafNode  = (LeafNodeInt *) currPageData;
		int i = 0;
		for(i = 0; i < INTARRAYNONLEAFSIZE; i++){
		 	if(currLeafNode->keyArray[i] != -1){
				std::cout << currLeafNode->keyArray[i] << ", ";
			}else{
				break;
			}
		}
		std::cout << "\n";
		return;
	}


	bufMgr->readPage(file, metadata->rootPageNo, currPageData); 
	NonLeafNodeInt * currNode  = (NonLeafNodeInt *) currPageData;
		
	//Only works for one level tree as for now	
	while(currNode->level >=1){

		int i = 0;
		for(i = 0; i < INTARRAYNONLEAFSIZE; i++){
		 	if(currNode->keyArray[i] != -1){
				std::cout << currNode->keyArray[i] << ", ";
				q.push(currNode->pageNoArray[i]);
			}else{
				break;
			}
		}
		std::cout << "\n";

		std::cout << "Leaf Nodes: \n";

		while(!q.empty()){
			currPageNum = q.front();
			q.pop();
			bufMgr->readPage(file, currPageNum, currPageData);
			currLeafNode = (LeafNodeInt *) currPageData;
		
			for(i = 0; i < INTARRAYLEAFSIZE; i++){
				if(currLeafNode->keyArray[i] != -1){
					std::cout << currLeafNode->keyArray[i] << ", ";		
				}
				else{
					break;
				}
			}	

			std::cout << "\n";
		}
		
	}

}
}
