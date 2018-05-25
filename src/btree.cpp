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

//	std::cout << "inserting " << keyValue << std::endl;

			// After the insertion, call fullNodeHandler if the node is full.
			insertToNode(rootNode,key,rid);
			if (rootNode->keyArray[INTARRAYLEAFSIZE-1] != -1) { 
				fullNodeHandler(rootNode, NULL, 1);
				this->numOfNodes++;
			}
		}

		/* This block is the general case where there is more than one node */
		else {	// more than one node
			Page * parentPage;
			bufMgr->readPage(file, metadata->rootPageNo, parentPage);  
			LeafNodeInt * leafNode = traverse(keyValue, (NonLeafNodeInt *) parentPage, metadata->rootPageNo);

/*
			Page * rootPage = new Page();
			this->bufMgr->readPage(this->file, this->rootPageNum, rootPage);
			NonLeafNodeInt * rootNode = new NonLeafNodeInt();
			rootNode = (NonLeafNodeInt*)rootPage;
*/

//			const LeafNodeInt* leafNode = traverse();
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
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertToNode - helpfer function of insertEntry
// -----------------------------------------------------------------------------
const void BTreeIndex::insertToNode(LeafNodeInt * node, const void *key, const RecordId rid) {
	
	int keyValue = *(int*)key;
	int i = INTARRAYLEAFSIZE;

	// Push every values to the right when it's greater than key
	while (i > 0 && (keyValue < node->keyArray[i-1] || node->keyArray[i-1] == -1)) {
		if (node->keyArray[i-1] != -1) node->keyArray[i] = node->keyArray[i-1];
		i--;
	}

//	printf("inserting %d into position %d\n", keyValue, i);
	node->keyArray[i] = keyValue;


}

// -----------------------------------------------------------------------------
// BTreeIndex::fullNodeHandler - helper function of insertEntry
// -----------------------------------------------------------------------------
void BTreeIndex::fullNodeHandler(void* currNode, NonLeafNodeInt* parentNode, bool isLeaf) {


	int currRootPageNo = this->rootPageNum;

	// Check if there already exists the parent to push up
	if (parentNode == NULL) { 
		Page * newParentPage = new Page();
		this->bufMgr->allocPage(this->file, this->rootPageNum, newParentPage);
		parentNode = (NonLeafNodeInt *)newParentPage;
		for (int i=0; i < INTARRAYLEAFSIZE; i++) {
			parentNode->keyArray[i] = -1;
		}
	}

	
	// split page
	PageId rightPageNum;
	int middleKey;
	if (isLeaf) {
		LeafNodeInt* currNodeLeaf = (LeafNodeInt*)currNode;
		splitLeafNode(currNodeLeaf, middleKey, rightPageNum); // splitNode called
	}
	else{
		/* Haven't implemented splitNonLeafNode yet */
//		NonLeafNodeInt* currNodeNonLeaf = (NonLeafNodeInt*)currNode;
//		splitNonLeafNode(currNodeNonLeaf, middleKey, rightPageNum);
	}

	// Set attribute for new root
	// Push every values to the right when it's greater than key
	int nonLeafIndex = INTARRAYLEAFSIZE;
	while (nonLeafIndex > 0 && (middleKey < parentNode->keyArray[nonLeafIndex-1] || parentNode->keyArray[nonLeafIndex-1] == -1)) {
		if (parentNode->keyArray[nonLeafIndex-1] != -1) parentNode->keyArray[nonLeafIndex] = parentNode->keyArray[nonLeafIndex-1];
		nonLeafIndex--;
	}
	parentNode->keyArray[nonLeafIndex] = middleKey;
	parentNode->pageNoArray[nonLeafIndex] = currRootPageNo;
	parentNode->pageNoArray[nonLeafIndex+1] = rightPageNum;


	testPrint();


}

// Just for test. Print whatever you want
void BTreeIndex::testPrint() {

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

				std::cout << "PRINTING LEFT" << std::endl;
				for (int i=0; i < INTARRAYLEAFSIZE; i++) {
					if (testLeftNode->keyArray[i] != -1) std::cout << testLeftNode->keyArray[i] << ' ';
				}
				std::cout << std::endl;
				std::cout << "PRINTING RIGHT" << std::endl;
				for (int i=0; i < INTARRAYLEAFSIZE; i++) {
					if (testRightNode->keyArray[i] != -1) std::cout << testRightNode->keyArray[i] << ' ';
				}
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitLeafNode - helper function of insertEntry
// -----------------------------------------------------------------------------
void BTreeIndex::splitLeafNode(LeafNodeInt *& leftNode, int& middleKey, PageId &pid) { 
	// After splitNode, the original node will be in left, while returned node will be in right

	// Allocate new page to be right child of root
	Page * rightPage = new Page();
	this->bufMgr->allocPage(this->file, pid, rightPage);
	LeafNodeInt * rightNode = new LeafNodeInt();
	rightNode = (LeafNodeInt*)rightPage;

	// get the middle value
	int middlePoint = INTARRAYLEAFSIZE/2;
	middleKey = leftNode->keyArray[middlePoint];

	// move right side of original node to rightNode
	for (int i=middlePoint, j=0; i < INTARRAYLEAFSIZE; i++, j++) {
		rightNode->keyArray[j] = leftNode->keyArray[i];
		rightNode->ridArray[j] = leftNode->ridArray[i];
	}
	for (int i=middlePoint; i < INTARRAYLEAFSIZE; i++) {
		rightNode->keyArray[i] = -1;
		leftNode->keyArray[i] = -1;
	}

	// Increment total number of nodes 
	this->numOfNodes += 1;


}

// -----------------------------------------------------------------------------
// BTreeIndex::splitNonLeafNode - helper function of insertEntry
// -----------------------------------------------------------------------------
void BTreeIndex::splitNonLeafNode(int key, NonLeafNodeInt *& leftNode) { // TO DO: Add implementation 

}

// -----------------------------------------------------------------------------
// BTreeIndex::traverse - helper function of insertEntry
// -----------------------------------------------------------------------------
LeafNodeInt * BTreeIndex::traverse(int key, NonLeafNodeInt * currNode, PageId currNodeId) { // TO DO: Add parameter & implementation


	if(currNode->level == 1){
		int i = 0;
		for(i = 0; i < INTARRAYNONLEAFSIZE; i++){
			if(currNode->keyArray[i] != 0 && currNode->keyArray[i] > key){
				break;
			}
		}

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

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{

}

}
