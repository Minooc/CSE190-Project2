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

		LeafNodeInt * rootNode = new LeafNodeInt();
		rootNode = (LeafNodeInt*)rootPage;
		printf("root page inserting %d w/ %d\n", *(int*)key, rid);
		rootNode->keyArray[0] = *(int*)key;
		rootNode->ridArray[0] = rid;
		rootNode->occupancy = 1;
		this->bufMgr->unPinPage(this->file, this->rootPageNum, 1);
	}
	else {	// Tree is not empty (every cases other than first insertion)

		Page * rootPage = new Page();
		this->bufMgr->readPage(this->file, this->rootPageNum, rootPage);
		LeafNodeInt * rootNode = new LeafNodeInt();
		rootNode = (LeafNodeInt*)rootPage;

		// Check if root is full
		if (rootNode->occupancy == INTARRAYLEAFSIZE) {
			/* Split */

			// insert
			insertToNode(key, rid, 0);

		}
		else {	// root is not full

			insertToNode(key, rid, 1);
		}


	}
	
	this->bufMgr->unPinPage(this->file, this->headerPageNum, 0);
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertToNode - helpfer function of insertEntry
// -----------------------------------------------------------------------------
const void BTreeIndex::insertToNode(const void *key, const RecordId rid, bool isLeaf) {
	

	if (isLeaf) {	// inserting to leaf node

		Page * rootPage = new Page();
		this->bufMgr->readPage(this->file, this->rootPageNum, rootPage);
		LeafNodeInt * rootNode = new LeafNodeInt();
		rootNode = (LeafNodeInt*)rootPage;

		int keyValue = *(int*)key;
		int i = rootNode->occupancy;

		
		// Push every values to the right when it's greater than key
		while (i > 0 && (keyValue < rootNode->keyArray[i-1])) {
			rootNode->keyArray[i] = rootNode->keyArray[i-1];
			i--;
		}

		printf("inserting %d into position %d\n", keyValue, i);
		rootNode->keyArray[i] = keyValue;
		rootNode->occupancy = rootNode->occupancy + 1;

	}

	else {	// Inserting to non-leaf node


	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitNode - helper function of insertEntry
// -----------------------------------------------------------------------------
const void BTreeIndex::splitNode() { // TO DO: Add parameter & implementation

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
