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

/*

To create a disk image of the index file, you simply use the BlobFile constructor
with the name of the index file.

The file that you create is a “raw” file, i.e., it has no page
structure on top of it.

You will need to implement a structure on top of the pages that you
get from the I/O Layer to implement the nodes of the B+ Tree.

Note the PageFile class
that we provide superimposes a page structure on the “raw” page. Just as the File class
uses the first page as a header page to store the metadata for that file, you will dedicate a
header page for the B+ Tree file too for storing metadata of the index.
*/
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
        scanExecuting = false;

        try
        {
            file = new BlobFile(outIndexName, false);

            headerPageNum = file->getFirstPageNo();
            Page* headerPage;
            bufMgr->readPage(file, headerPageNum, headerPage);
            IndexMetaInfo* metaInfo = (IndexMetaInfo*) headerPage;
            rootPageNum = metaInfo->rootPageNo;
            bufMgr->unPinPage(file, headerPageNum, false);

            rootIsLeaf = metaInfo->rootIsLeaf;

        }
        catch (FileNotFoundException& e)
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

            LeafNodeInt* root = (LeafNodeInt*) rootPage;
            root->rightSibPageNo = MAX_INT;
            for (int i = 0; i < leafOccupancy; i++)
            {
                root->keyArray[i] = MAX_INT;
            }

            rootIsLeaf = true;
            metaInfo->rootIsLeaf = true;

            bufMgr->unPinPage(file, headerPageNum, true);
            bufMgr->unPinPage(file, rootPageNum, true);

            FileScan fscan(relationName, bufMgr);

            while(true)
            {
                try
                {
                    RecordId rid;
                    fscan.scanNext(rid);
                    std::string recordStr = fscan.getRecord();
                    const char *record = recordStr.c_str();
                    insertEntry(record + attrByteOffset, rid);

                }
                catch(EndOfFileException& e)
                {
                    break;
                }
            }
        }
    }

    BTreeIndex::BTreeIndex(const std::string & relationName,
                           std::string & outIndexName,
                           BufMgr *bufMgrIn,
                           const int attrByteOffset,
                           const Datatype attrType, const int nodeOccupancy, const int leafOccupancy)
    {
        std::ostringstream idxStr;
        idxStr << relationName << '.' << attrByteOffset;
        outIndexName = idxStr.str();

        bufMgr = bufMgrIn;
        this->leafOccupancy = leafOccupancy;
        this->nodeOccupancy = nodeOccupancy;
        attributeType = attrType;
        this->attrByteOffset = attrByteOffset;
        scanExecuting = false;

        try
        {
            file = new BlobFile(outIndexName, false);

            headerPageNum = file->getFirstPageNo();
            Page* headerPage;
            bufMgr->readPage(file, headerPageNum, headerPage);
            IndexMetaInfo* metaInfo = (IndexMetaInfo*) headerPage;
            rootPageNum = metaInfo->rootPageNo;
            bufMgr->unPinPage(file, headerPageNum, false);

            rootIsLeaf = metaInfo->rootIsLeaf;

        }
        catch (FileNotFoundException& e)
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

            LeafNodeInt* root = (LeafNodeInt*) rootPage;
            root->rightSibPageNo = MAX_INT;
            for (int i = 0; i < leafOccupancy; i++)
            {
                root->keyArray[i] = MAX_INT;
            }

            rootIsLeaf = true;
            metaInfo->rootIsLeaf = true;

            bufMgr->unPinPage(file, headerPageNum, true);
            bufMgr->unPinPage(file, rootPageNum, true);

            FileScan fscan(relationName, bufMgr);

            while(true)
            {
                try
                {
                    RecordId rid;
                    fscan.scanNext(rid);
                    std::string recordStr = fscan.getRecord();
                    const char *record = recordStr.c_str();
                    insertEntry(record + attrByteOffset, rid);

                }
                catch(EndOfFileException& e)
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


        PageKeyPair<int> split;
        if (rootIsLeaf)
        {
            split = insertLeaf(rootPageNum, key, rid);
        }
        else
        {
            split = insertNode(rootPageNum, key, rid);
        }

		// check if the root is to be split
        if (split.key != MAX_INT)
        {
			// create new root node
            Page* page;
            PageId pageNum;

            bufMgr->allocPage(file, pageNum, page);

            NonLeafNodeInt* node = (NonLeafNodeInt*) page;
            node->keyArray[0] = split.key;
            node->pageNoArray[0] = rootPageNum;
            node->pageNoArray[1] = split.pageNo;

            rootPageNum = pageNum;

            for (int i = 1; i < nodeOccupancy; i++)
            {
                node->keyArray[i] = MAX_INT;
            }

            if (rootIsLeaf)
            {
                node->level = 1;
                rootIsLeaf = false;
            }
            else
            {
                node->level = 0;
            }

            Page* headerPage;
            bufMgr->readPage(file, headerPageNum, headerPage);
            IndexMetaInfo* metaPage = (IndexMetaInfo*) headerPage;
            metaPage -> rootPageNo = rootPageNum;
            metaPage -> rootIsLeaf = false;

            bufMgr->unPinPage(file, pageNum, true);
            bufMgr->unPinPage(file, headerPageNum, true);
        }

        return;
    }

    PageKeyPair<int> BTreeIndex::insertNode(PageId pageNum, const void *key, const RecordId rid)
    {
        Page* page;
        bufMgr->readPage(file, pageNum, page);
        NonLeafNodeInt* node = (NonLeafNodeInt*) page;

		// find the smallest entry in the node with a key >= the element we are inserting
        int index = 0;
        int k = *((int*) key);
        while (index < nodeOccupancy && node->keyArray[index] < k)
        {
            index++;
        }

        PageKeyPair<int> split;
        if (node-> level == 1)
        {
            split = insertLeaf(node->pageNoArray[index], key, rid);
        }
        else
        {
            split = insertNode(node->pageNoArray[index], key, rid);
        }

        PageKeyPair<int> pair;
        if (split.key != MAX_INT)
        {
            if (node->keyArray[nodeOccupancy - 1] != MAX_INT)
            {
                //split node

                Page* splitPage;
                PageId splitID;
                bufMgr->allocPage(file, splitID, splitPage);
                
                NonLeafNodeInt* splitNode = (NonLeafNodeInt*) splitPage;
                splitNode->level = node->level;

                for (int i = 0; i < nodeOccupancy; i++)
                {
                    splitNode->keyArray[i] = MAX_INT;
                }

                int mid = (nodeOccupancy) / 2;

                if (index == mid)
                {
                    for (int i = mid; i < nodeOccupancy; i++)
                    {
                        splitNode->keyArray[i - mid] = node->keyArray[i];
                        splitNode->pageNoArray[i - mid + 1] = node->pageNoArray[i + 1];
                    }
                    splitNode->keyArray[0] = split.pageNo;
                    
                    for (int i = mid; i < nodeOccupancy; i++)
                    {
                        node->keyArray[i] = MAX_INT;
                    }

                    pair.set(splitID, split.key);
                }
                else if (index < mid)
                {
                    for (int i = mid; i < nodeOccupancy; i++)
                    {
                        splitNode->keyArray[i - mid] = node->keyArray[i];
                        splitNode->pageNoArray[i - mid + 1] = node->pageNoArray[i + 1];
                    }

                    splitNode->pageNoArray[0] = node->pageNoArray[mid];

                    pair.set(splitID, node->keyArray[mid - 1]);

                    for (int i = mid - 2; i >= index; i--)
                    {
                        node->keyArray[i + 1] = node->keyArray[i];
                        node->pageNoArray[i + 2] = node->pageNoArray[i + 1];
                    }

                    for (int i = mid; i < nodeOccupancy; i++)
                    {
                        node->keyArray[i] = MAX_INT;
                    }

                    node->keyArray[index] = split.key;
                    node->pageNoArray[index + 1] = split.pageNo;

                }
                else
                {
                    pair.set(splitID, node->keyArray[mid]);
                    mid++;
                    splitNode->pageNoArray[0] = node->pageNoArray[mid];
                    for (int i = mid; i < index; i++)
                    {
                        splitNode->keyArray[i - mid] = node->keyArray[i];
                        splitNode->pageNoArray[i - mid + 1] = node->pageNoArray[i + 1];
                    }
                    splitNode->keyArray[index - mid] = split.key;
                    splitNode->pageNoArray[index - mid + 1] = split.pageNo;
                    for (int i = index; i < nodeOccupancy; i++)
                    {
                        splitNode->keyArray[i - mid + 1] = node->keyArray[i];
                        splitNode->pageNoArray[i - mid + 2] = node->pageNoArray[i + 1];
                    }

                    for (int i = mid - 1; i < nodeOccupancy; i++)
                    {
                        node->keyArray[i] = MAX_INT;
                    }
                }
                bufMgr->unPinPage(file, splitID, true);
            }
            else
            {
                for (int i = nodeOccupancy - 2; i >= index; i--)
                {
                    node->keyArray[i + 1] = node->keyArray[i];
                    node->pageNoArray[i + 2] = node->pageNoArray[i + 1];
                }
                node->keyArray[index] = split.key;
                node->pageNoArray[index + 1] = split.pageNo;

                pair.set(MAX_INT, MAX_INT);
            }
            bufMgr->unPinPage(file, pageNum, true);
        }
        else
        {
            pair.set(MAX_INT, MAX_INT);
            bufMgr->unPinPage(file, pageNum, false);
        }
        return pair;
    }

    PageKeyPair<int> BTreeIndex::insertLeaf(PageId pageNum, const void *key, const RecordId rid)
    {
        Page* page;
        bufMgr->readPage(file, pageNum, page);
        
        LeafNodeInt* leaf = (LeafNodeInt*) page;
        int index = 0;
        int k = *((int*) key);
        while(index < leafOccupancy && leaf->keyArray[index] < k)
        {
            index++;
        }

		// if the leaf is full, split into two leaves
        if (leaf->keyArray[leafOccupancy - 1] != MAX_INT)
        {
            //split node

			// create the new leaf
            Page* split;
            PageId splitID;
            bufMgr->allocPage(file, splitID, split);
            LeafNodeInt* splitNode = (LeafNodeInt*) split;
            splitNode->rightSibPageNo = leaf -> rightSibPageNo;
            leaf->rightSibPageNo = splitID;

            for (int i = 0; i < leafOccupancy; i++)
            {
                splitNode-> keyArray[i] = MAX_INT;
            }

            int mid = (leafOccupancy - 1) / 2;

            if (index < mid)
            {
				// copies the second half of the leaf to the new leaf
                for (int i = mid; i < leafOccupancy; i++)
                {
                    splitNode->keyArray[i - mid] = leaf->keyArray[i];
                    splitNode->ridArray[i - mid] = leaf->ridArray[i];
                }

				// shifts the elements greater than the element we are inserting right
                for (int i = mid - 1; i >= index; i--)
                {
                    leaf->keyArray[i + 1] = leaf->keyArray[i];
                    leaf->ridArray[i + 1] = leaf->ridArray[i];
                }

                for (int i = mid + 1; i < leafOccupancy; i++)
                {
                    leaf->keyArray[i] = MAX_INT;
                }

                leaf->keyArray[index] = k;
                leaf->ridArray[index] = rid;
            }
            else
            {
                mid++;
                for (int i = mid; i < index; i++)
                {
                    splitNode->keyArray[i - mid] = leaf->keyArray[i];
                    splitNode->ridArray[i - mid] = leaf->ridArray[i];
                }
                splitNode->keyArray[index - mid] = k;
                splitNode->ridArray[index - mid] = rid;
                for (int i = index; i < leafOccupancy; i++)
                {
                    splitNode->keyArray[i - mid + 1] = leaf->keyArray[i];
                    splitNode->ridArray[i - mid + 1] = leaf->ridArray[i];
                }

                for (int i = mid; i < leafOccupancy; i++)
                {
                    leaf->keyArray[i] = MAX_INT;
                }
            }

            PageKeyPair<int> pair;
            pair.set(splitID, splitNode->keyArray[0]);
            bufMgr->unPinPage(file, pageNum, true);
            bufMgr->unPinPage(file, splitID, true);
            return pair;
        }
        else
        {
            for (int i = leafOccupancy - 2; i >= index; i--)
            {
                leaf->keyArray[i + 1] = leaf->keyArray[i];
                leaf->ridArray[i + 1] = leaf->ridArray[i];
            }
            leaf->keyArray[index] = k;
            leaf->ridArray[index] = rid;

            PageKeyPair<int> pair;
            pair.set(MAX_INT, MAX_INT);
            bufMgr->unPinPage(file, pageNum, true);
            return pair;
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
		// check if operators are valid
        if (lowOpParm != GT && lowOpParm != GTE)
        {
            throw BadOpcodesException();
        }

        if (highOpParm != LT && highOpParm != LTE)
        {
            throw BadOpcodesException();
        }

        lowValInt = *((int*) lowValParm);
        highValInt = *((int*) highValParm);

        if (lowValInt > highValInt)
        {
            throw BadScanrangeException();
        }
        lowOp = lowOpParm;
        highOp = highOpParm;

        Page* page;
        PageId pageNum = rootPageNum;
        bufMgr->readPage(file, pageNum, page);
        

		// if the root isn't a leaf node, traverse the B+ tree until we reach a leaf
        if (!rootIsLeaf)
        {
            NonLeafNodeInt* node = (NonLeafNodeInt*) page;
            while(true)
            {
                int index = 0;
                while (index < nodeOccupancy && node->keyArray[index] <= lowValInt)
                {
                    index++;
                }
                bufMgr->unPinPage(file, pageNum, false);
                pageNum = node->pageNoArray[index];
                bufMgr->readPage(file, pageNum, page);

                if (node-> level == 1)
                {
                    break;
                }
                node = (NonLeafNodeInt*) page;
            }
        }

        LeafNodeInt* leaf = (LeafNodeInt*) page;
        int index = 0;
		// find the first node >= the lower bound of our range
        while(index < leafOccupancy && leaf->keyArray[index] < lowValInt)
        {
            index++;
        }

		// If the range is exclusive, find the first node > the lower bound of our range
        if (lowOp == GT)
        {
            while(index < leafOccupancy && leaf->keyArray[index] <= lowValInt)
            {
                index++;
            }
        }

		// every element in our B+ tree is below the range we are searching for
        if (index >= leafOccupancy)
        {
            bufMgr->unPinPage(file, pageNum, false);
            throw NoSuchKeyFoundException();
        }

		//  No elements within the range
        if (highOp == LT && leaf->keyArray[index] >= highValInt)
        {
            bufMgr->unPinPage(file, pageNum, false);
            throw NoSuchKeyFoundException();
        }

        if (highOp == LTE && leaf->keyArray[index] > highValInt)
        {
            bufMgr->unPinPage(file, pageNum, false);
            throw NoSuchKeyFoundException();
        }

		// mark which entry is the first in range
        nextEntry = index;
        currentPageNum = pageNum;
        currentPageData = page;
        scanExecuting = true;
    }

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

    void BTreeIndex::scanNext(RecordId& outRid)
    {
        if (!scanExecuting)
        {
            throw ScanNotInitializedException();
        }

        LeafNodeInt* leaf = (LeafNodeInt*) currentPageData;

        if (nextEntry >= leafOccupancy || leaf->keyArray[nextEntry] == MAX_INT)
        {
            if (leaf->rightSibPageNo == (PageId) MAX_INT)
            {
                throw IndexScanCompletedException();
            }
            bufMgr->unPinPage(file, currentPageNum, false);
            currentPageNum = leaf->rightSibPageNo;
            bufMgr->readPage(file, currentPageNum, currentPageData);
            
            leaf = (LeafNodeInt*) currentPageData;
            nextEntry = 0;
        }

        if (highOp == LT && leaf->keyArray[nextEntry] >= highValInt)
        {
            throw IndexScanCompletedException();
        }

        if (highOp == LTE && leaf->keyArray[nextEntry] > highValInt)
        {
            throw IndexScanCompletedException();
        }

        outRid = leaf->ridArray[nextEntry];

        nextEntry++;
    }

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
    void BTreeIndex::endScan()
    {
        if (!scanExecuting)
        {
            throw ScanNotInitializedException();
        }

        bufMgr->unPinPage(file, currentPageNum, false);
        scanExecuting = false;
    }

    bool BTreeIndex::getNodeStatus()
    {
        return rootIsLeaf;
    }
}