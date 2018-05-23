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
//	printf("inserting key %d (rid %d)\n", *(int*)key, rid);
	Page * metadataPage = new Page();
	this->bufMgr->readPage(this->file, this->headerPageNum, metadataPage);
//	printf("read page with num %d\n", (int)headerPageNum);

	IndexMetaInfo * metadata = new IndexMetaInfo();
	metadata = (IndexMetaInfo*)metadataPage;
	std::cout << metadata->attrByteOffset << ' ' << metadata->attrType << ' ' << metadata->relationName << std::endl;

	this->bufMgr->unPinPage(this->file, headerPageNum, 0);

		
	
/*
	std::cout << "inserting leaf" << std::endl;
	LeafNodeInt leaf;
	leaf.keyArray[0] = *(int*)key;
	leaf.ridArray[0] = rid;

	Page * page = new Page();
	page = (Page*)&leaf;
	this->file->writePage(1, *page);
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
