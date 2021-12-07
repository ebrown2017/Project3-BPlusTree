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
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	outIndexName = idxStr.str();

	bufMgr = bufMgrIn;
	leafOccupancy = INTARRAYLEAFSIZE;
	nodeOccupancy = INTARRAYNONLEAFSIZE;
	attributeType = attrType;
	this->attrByteOffset = attrByteOffset;

	try
	{
		file = new BlobFile(outIndexName, false);

		headerPageNum = file->getFirstPageNo();

		
	}
	catch (FileNotFoundException e)
	{
		file = new BlobFile(outIndexName, true);

		Page* headerPage;
		bufMgr->allocPage(file, headerPageNum, headerPage);
		
		Page* rootPage;
		bufMgr->allocPage(file, rootPageNum, rootPage);

		IndexMetaInfo* metaInfo = (IndexMetaInfo*) headerPage;
		metaInfo->attrByteOffset = attrByteOffset;
		metaInfo->attrType = attrType;
		metaInfo->rootPageNo = rootPageNum;
		strncpy(metaInfo->relationName, relationName.c_str(), relationName.length());

		LeafNodeInt* root = (LeafNodeInt*) rootPageNum;

		bufMgr->unPinPage(file, headerPageNum, false);
		bufMgr->unPinPage(file, rootPageNum, false);

		FileScan fscan(relationName, bufMgr);
		
		while(true)
		{
			try
			{
				RecordId rid;
				scanNext(rid);
				//insertEntry()
			
			}
			catch(EndOfFileException e)
			{
				break;
			}
			
		}


	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	scanExecuting = false;

	bufMgr->flushFile(file);
	delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	/*
	If the tree is empty insert the root node
	Else determine if the value should go to left or right of the root 
	*/
	/*
	1) Search for appropriate location to add an entry
	2) Check if the node has the space for a new entry
		a) if there is no space split the entry into two entries. If there is space then insert and terminate
		b) Is this entry a leaf node?
			i) If yes then copy the middle node into the parent entry
			ii) If no then move the middle node to the parent entry
		c) insert index entry pointing towards the new half of the split entry into the parent entry
	
	 */


	if (rootIsLeaf)
	{
		LeafNodeInt* root = (LeafNodeInt*) rootPageNum;
		if (root->keyArray[leafOccupancy - 1] == INT_MAX)
		{
			int index = 0;
			while (index < leafOccupancy && root->keyArray[index] < (int) key)
			{
				index++;
			}
			//split node
			rootIsLeaf = false;

			Page* page;
			
			bufMgr->allocPage(file, rootPageNum, page);
			NonLeafNodeInt* node = (NonLeafNodeInt*) page;
			node->level = 1;
			
			Page* split;
			PageId splitID;
			bufMgr->allocPage(file, splitID, split);
			LeafNodeInt* splitNode = (LeafNodeInt*) split;
			node->level = 1;
			
			int mid = (leafOccupancy + 1) / 2;
			for (int i = mid + 1; i < leafOccupancy; i++)
			{

			}
			

		}
		else
		{
			int index = 0;
			while (index < leafOccupancy && root->keyArray[index] < (int) key)
			{
				index++;
			}
			for (int i = leafOccupancy - 2; i >= index; i--)
			{
				root->keyArray[index + 1] = root->keyArray[index];
				root->ridArray[index + 1] =  root->ridArray[index];
			}
			root->keyArray[index] = (int) key;
			root->ridArray[index] = rid;
		}
	}

	FileScan fscan(file->filename(), bufMgr);
	
	while(true)
		{
			try
			{
				RecordId rid;
				scanNext(rid);
				//insertEntry()
				if (rid == )
			
			}
			catch(EndOfFileException e)
			{
				break;
			}
			
		}


}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
	if (lowValParm > highValParm)
	{
		throw new BadScanrangeException();
	}

	if (lowOpParm != GT && lowOpParm != GTE)
	{
		throw new BadOpcodesException();
	}

	if (highOpParm != LT && highOpParm != LTE)
	{
		throw new BadOpcodesException();
	}

	lowValInt = (int) lowValParm;
	highValInt = (int) highValParm;
	lowOp = lowOpParm;
	highOp = highOpParm;

	if (rootIsLeaf)
	{
		
	}
	scanExecuting = true;
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) 
{
	if (!scanExecuting)
	{
		throw new ScanNotInitializedException();
	}

	// use lowValInt and highValInt

	
	

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan() 
{
	if (!scanExecuting)
	{
		throw new ScanNotInitializedException();
	}

	scanExecuting = false;
}

}
