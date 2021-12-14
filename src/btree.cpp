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
        scanExecuting = false;

        // Check to see if file exists
        try
        {
            // file exists; open file
            file = new BlobFile(outIndexName, false);

            // get root info from the meta page
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
            // File does not exist, create one
            file = new BlobFile(outIndexName, true);

            // allocate pages for meta page and root node
            Page* headerPage;
            bufMgr->allocPage(file, headerPageNum, headerPage);

            Page* rootPage;
            bufMgr->allocPage(file, rootPageNum, rootPage);

            // set data in meta page
            IndexMetaInfo* metaInfo = (IndexMetaInfo*) headerPage;
            metaInfo->attrByteOffset = attrByteOffset;
            metaInfo->attrType = attrType;
            metaInfo->rootPageNo = rootPageNum;
            strncpy(metaInfo->relationName, relationName.c_str(), relationName.length());
            metaInfo->rootIsLeaf = true;

            // initialize root node
            LeafNodeInt* root = (LeafNodeInt*) rootPage;
            // set right sibling page number to max int value to denote no right sibling
            root->rightSibPageNo = MAX_INT;
            // set entries of key array to max int value to denote no key in that index
            for (int i = 0; i < leafOccupancy; i++)
            {
                root->keyArray[i] = MAX_INT;
            }

            rootIsLeaf = true;

            bufMgr->unPinPage(file, headerPageNum, true);
            bufMgr->unPinPage(file, rootPageNum, true);

            FileScan fscan(relationName, bufMgr);

            // Insert into B+ tree
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
// BTreeIndex::BTreeIndex -- Constructor with specified node/leaf capacities
// -----------------------------------------------------------------------------
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

        // Check to see if file exist
        try
        {
            // file exists, open file
            file = new BlobFile(outIndexName, false);

            // get root info from meta page
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
            // File does not exist, create it
            file = new BlobFile(outIndexName, true);

            // allocate pages for meta info and root noe
            Page* headerPage;
            bufMgr->allocPage(file, headerPageNum, headerPage);

            Page* rootPage;
            bufMgr->allocPage(file, rootPageNum, rootPage);

            // set info in meta page
            IndexMetaInfo* metaInfo = (IndexMetaInfo*) headerPage;
            metaInfo->attrByteOffset = attrByteOffset;
            metaInfo->attrType = attrType;
            metaInfo->rootPageNo = rootPageNum;
            strncpy(metaInfo->relationName, relationName.c_str(), relationName.length());

            // create root node
            LeafNodeInt* root = (LeafNodeInt*) rootPage;
            // set right sibling page number to max int value to denote no right sibling
            root->rightSibPageNo = MAX_INT;
            // set entries of key array to max int value to denote no key in that index
            for (int i = 0; i < leafOccupancy; i++)
            {
                root->keyArray[i] = MAX_INT;
            }

            rootIsLeaf = true;
            metaInfo->rootIsLeaf = true;

            bufMgr->unPinPage(file, headerPageNum, true);
            bufMgr->unPinPage(file, rootPageNum, true);

            FileScan fscan(relationName, bufMgr);

            // Insert into B+ tree
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
        // Splitting logic
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

            //  update root page number in the header
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

    // -----------------------------------------------------------------------------
    // BTreeIndex::insertNode
    // -----------------------------------------------------------------------------

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

        // Determine if the current node is at the level above the leaf nodes
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
        // determine if children of node were split
        if (split.key != MAX_INT)
        {
            // determine if this node needs to split
            if (node->keyArray[nodeOccupancy - 1] != MAX_INT)
            {
                //split node using logic described in insertEntry()

                Page* splitPage;
                PageId splitID;
                bufMgr->allocPage(file, splitID, splitPage);
                
                // create new node for splitting
                NonLeafNodeInt* splitNode = (NonLeafNodeInt*) splitPage;
                splitNode->level = node->level;

                for (int i = 0; i < nodeOccupancy; i++)
                {
                    splitNode->keyArray[i] = MAX_INT;
                }

                int mid = (nodeOccupancy) / 2;

                // determine how to split and which key to push up 
                // based on location of node inserted
                if (index == mid)
                {
                    // copies right half of entries to new leaf
                    for (int i = mid; i < nodeOccupancy; i++)
                    {
                        splitNode->keyArray[i - mid] = node->keyArray[i];
                        splitNode->pageNoArray[i - mid + 1] = node->pageNoArray[i + 1];
                    }
                    splitNode->pageNoArray[0] = split.pageNo;
                    
                    for (int i = mid; i < nodeOccupancy; i++)
                    {
                        node->keyArray[i] = MAX_INT;
                    }

                    pair.set(splitID, split.key);
                }
                else if (index < mid)
                {
                    // copies right half of entries to new leaf
                    for (int i = mid; i < nodeOccupancy; i++)
                    {
                        splitNode->keyArray[i - mid] = node->keyArray[i];
                        splitNode->pageNoArray[i - mid + 1] = node->pageNoArray[i + 1];
                    }

                    splitNode->pageNoArray[0] = node->pageNoArray[mid];

                    pair.set(splitID, node->keyArray[mid - 1]);

                    // shift elements over in current leaf that are greater than the one we are inserting
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
                    // copies right half of entries to new leaf
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
                // shift keys right
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

    /*
    Helper method for the insertNode, manages adding a leaf to the b+ tree.
    */
    PageKeyPair<int> BTreeIndex::insertLeaf(PageId pageNum, const void *key, const RecordId rid)
    {
        Page* page;
        bufMgr->readPage(file, pageNum, page);
        
        // figure out where to insert the new record
        LeafNodeInt* leaf = (LeafNodeInt*) page;
        int index = 0;
        int k = *((int*) key);
        while(index < leafOccupancy && leaf->keyArray[index] < k)
        {
            index++;
        }

		// if the leaf is full, split into two leaves
        PageKeyPair<int> pair;
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

            if (index <= mid)
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
                // copy the second half of the current leaf into the new leaf
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

                // reset keys in current leaf
                for (int i = mid; i < leafOccupancy; i++)
                {
                    leaf->keyArray[i] = MAX_INT;
                }
            }

            pair.set(splitID, splitNode->keyArray[0]);
            bufMgr->unPinPage(file, splitID, true);
        }
        else
        {
            // Leaf isn't full, shift over elements to the right and add to leaf
            for (int i = leafOccupancy - 2; i >= index; i--)
            {
                leaf->keyArray[i + 1] = leaf->keyArray[i];
                leaf->ridArray[i + 1] = leaf->ridArray[i];
            }
            leaf->keyArray[index] = k;
            leaf->ridArray[index] = rid;

            pair.set(MAX_INT, MAX_INT);    
        }

        // unpin pages
        bufMgr->unPinPage(file, pageNum, true);
        return pair;
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

        // check for bad scan range
        if (lowValInt > highValInt)
        {
            throw BadScanrangeException();
        }
        lowOp = lowOpParm;
        highOp = highOpParm;

        // treat all GT parameters as GTE parameters
        if (lowOp == GT)
        {
            lowValInt++;
        }

        Page* page;
        PageId pageNum = rootPageNum;
        bufMgr->readPage(file, pageNum, page);
        
		// if the root isn't a leaf node, traverse the B+ tree until we reach a leaf
        if (!rootIsLeaf)
        {
            NonLeafNodeInt* node = (NonLeafNodeInt*) page;
            while(true)
            {
                // figure out which child to traverse to
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
        // Check to ensure we have active scan
        if (!scanExecuting)
        {
            throw ScanNotInitializedException();
        }

        LeafNodeInt* leaf = (LeafNodeInt*) currentPageData;

        // If at the end of a leaf, go to next page
        if (nextEntry >= leafOccupancy || leaf->keyArray[nextEntry] == MAX_INT)
        {
            // No more pages in tree, scan is completed
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

        // check if scan is completed
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
    
    // -----------------------------------------------------------------------------
    // BTreeIndex::getNodesStatus
    // -----------------------------------------------------------------------------
    //
    bool BTreeIndex::getNodeStatus()
    {
        // return if node is leaf or not, useful in testing
        return rootIsLeaf;
    }
}