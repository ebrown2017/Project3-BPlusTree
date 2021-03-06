/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <vector>
#include "btree.h"
#include "page.h"
#include "filescan.h"
#include "page_iterator.h"
#include "file_iterator.h"
#include "exceptions/insufficient_space_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/end_of_file_exception.h"
#include <random>
#include <chrono>

#define checkPassFail(a, b) 																				\
{																																		\
	if(a == b)																												\
		std::cout << "\nTest passed at line no:" << __LINE__ << "\n";		\
	else																															\
	{																																	\
		std::cout << "\nTest FAILS at line no:" << __LINE__;						\
		std::cout << "\nExpected no of records:" << b;									\
		std::cout << "\nActual no of records found:" << a;							\
		std::cout << std::endl;																					\
		exit(1);																												\
	}																																	\
}

using namespace badgerdb;

// -----------------------------------------------------------------------------
// Globals
// -----------------------------------------------------------------------------
const std::string relationName = "relA";
//If the relation size is changed then the second parameter 2 chechPassFail may need to be changed to number of record that are expected to be found during the scan, else tests will erroneously be reported to have failed.
const int	relationSize = 5000;
std::string intIndexName, doubleIndexName, stringIndexName;

// This is the structure for tuples in the base relation

typedef struct tuple {
	int i;
	double d;
	char s[64];
} RECORD;

PageFile* file1;
RecordId rid;
RECORD record1;
std::string dbRecord1;

BufMgr * bufMgr = new BufMgr(100);

// -----------------------------------------------------------------------------
// Forward declarations
// -----------------------------------------------------------------------------

// default tests
void createRelationForward();
void createRelationBackward();
void createRelationRandom();
void createRelationRandomSize(int relSize);
void intTests();
int intScan(BTreeIndex *index, int lowVal, Operator lowOp, int highVal, Operator highOp);
void indexTests();
void largeTests(BTreeIndex *index);
void emptyTests();
void smallTests(BTreeIndex *index);
void allTests(BTreeIndex* index, int relSize);

// Given tests
void test1();
void test2();
void test3();
void test4();
void test5();
void test6();
void test7();
void test8();
void test9();
void test10();
void test11();
void test12();
void test13();
void test14();
void test15();
void test16();
void test17();

void errorTests();
void deleteRelation();

int main(int argc, char **argv)
{

  // Clean up from any previous runs that crashed.
  try
	{
    File::remove(relationName);
  }
	catch(const FileNotFoundException &)
	{
  }

	{
		// Create a new database file.
		PageFile new_file = PageFile::create(relationName);

		// Allocate some pages and put data on them.
		for (int i = 0; i < 20; ++i)
		{
			PageId new_page_number;
			Page new_page = new_file.allocatePage(new_page_number);

    	sprintf(record1.s, "%05d string record", i);
    	record1.i = i;
    	record1.d = (double)i;
    	std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

			new_page.insertRecord(new_data);
			new_file.writePage(new_page_number, new_page);
		}
	}
	// new_file goes out of scope here, so file is automatically closed.

	{
		FileScan fscan(relationName, bufMgr);

		try
		{
			RecordId scanRid;
			while(1)
			{
				fscan.scanNext(scanRid);
				//Assuming RECORD.i is our key, lets extract the key, which we know is INTEGER and whose byte offset is also know inside the record. 
				std::string recordStr = fscan.getRecord();
				const char *record = recordStr.c_str();
				int key = *((int *)(record + offsetof (RECORD, i)));
				std::cout << "Extracted : " << key << std::endl;
			}
		}
		catch(const EndOfFileException &e)
		{
			std::cout << "Read all records" << std::endl;
		}
	}
	// filescan goes out of scope here, so relation file gets closed.

	File::remove(relationName);

	test1();
	test2();
	test3();
	test4();
	test5();
	test6();
	test7();
	test8();
	test9();
	test10();
	test11();
	test12();
	test13();
	test14();
	test15();
	test16();
	test17();
	
	errorTests();

	delete bufMgr;

  return 1;
}

void test1()
{
	// Create a relation with tuples valued 0 to relationSize and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "---------------------" << std::endl;
	std::cout << "createRelationForward" << std::endl;
	createRelationForward();
	indexTests();
	deleteRelation();
}

void test2()
{
	// Create a relation with tuples valued 0 to relationSize in reverse order and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "----------------------" << std::endl;
	std::cout << "createRelationBackward" << std::endl;
	createRelationBackward();
	indexTests();
	deleteRelation();
}

void test3()
{
	// Create a relation with tuples valued 0 to relationSize in random order and perform index tests 
	// on attributes of all three types (int, double, string)
	std::cout << "--------------------" << std::endl;
	std::cout << "createRelationRandom" << std::endl;
	createRelationRandom();
	indexTests();
	deleteRelation();
}

void test4()
{
	// Test creates a relation and checks to see if root isnt a leaf, this indicates that a split occurred
	std::cout << "--------------------" << std::endl;
	std::cout << "Test 4: checking split functionality" << std::endl;
	try
	{
		createRelationRandom();
		BTreeIndex index = BTreeIndex(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
		if (index.getNodeStatus())
		{
			std::cout << "Test 4 failed, no split occurred" << std::endl;;
		}
		else
		{
			std::cout << "Test 4 passed, a split occurred" << std::endl;;
		}
	}
	catch(BadOpcodesException& e)
	{
		std::cout << "Test 4 Passed" << std::endl;;
	}

	try
	{
		File::remove(intIndexName);
	}
  	catch(const FileNotFoundException &e)
	{

	}
	deleteRelation();
}


void test5()
{
	// creates a random relation, tries to start a scan with invalid opcodes
	try
	{
		createRelationRandom();
		BTreeIndex index = BTreeIndex(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
		int lowval = 10;
		int highval = 100;
		index.startScan(&lowval, LT, &highval, GT);
		index.endScan();
		std::cout << "Test 5 failed, no BadOpcodesException thrown" << std::endl;
	}
	catch(BadOpcodesException& e)
	{
		std::cout << "Test 5 Passed" << std::endl;
		
	}
	try
	{
		File::remove(intIndexName);
	}
  catch(const FileNotFoundException &e)
  {
  }
	deleteRelation();
}

void test6()
{
	// Test tries to start a scan with an invalid scan range
	try
	{
		createRelationRandom();
		BTreeIndex index = BTreeIndex(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
		int lowval = 100;
		int highval = 10;
		index.startScan(&lowval, GT, &highval, LT);
		index.endScan();
		std::cout << "Test 6 failed, no BadScanrangeException thrown" << std::endl;
	}
	catch(BadScanrangeException& e)
	{
		std::cout << "Test 6 Passed" << std::endl;		
	}

	try
	{
		File::remove(intIndexName);
	}
	catch(const FileNotFoundException &e)
	{

	}
	deleteRelation();
}

void test7()
{
	// Test tries to end a scan before starting one
	try
	{
		createRelationRandom();
		BTreeIndex index = BTreeIndex(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
		index.endScan();
		std::cout << "Test 7 failed, endScan() ran without a Scan running" << std::endl;
	}
	catch(ScanNotInitializedException& e )
	{
		std::cout << "Test 7 Passed" << std::endl;
	}

	try
	{
		File::remove(intIndexName);
	}
	catch(const FileNotFoundException &e)
	{

	}
	deleteRelation();
}

void test8()
{
	// Test tries to scan next when the scan is complete
	try
	{
		createRelationRandom();
		BTreeIndex index = BTreeIndex(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
		int lowval = 10;
		int highval = 100;
		index.startScan(&lowval, GT, &highval, LT);
		try
		{
			RecordId outRid;
			for (int i = 0; i < 5001; i++)
			{
				index.scanNext(outRid);
			}
			std::cout << "Test 8 failed, no IndexScanCompletedException was thrown" << std::endl;
		}
		catch(IndexScanCompletedException& e )
		{
			std::cout << "Test 8 passed" << std::endl;
		}
		index.endScan();
	}
	catch(std::exception& e)
	{
		std::cout << "Test 8 failed, exception was thrown"<< std::endl;
	}
	try
	{
		File::remove(intIndexName);
	}
	catch(const FileNotFoundException &e)
	{

	}
	deleteRelation();
}

void test9()
{
	// Test creates a forward relation and checks to see if root isnt a leaf, this indicates that a split occurred
	std::cout << "--------------------" << std::endl;
	std::cout << "Test 9: checking split functionality" << std::endl;
	try
	{
		createRelationForward();
		BTreeIndex index = BTreeIndex(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
		if (index.getNodeStatus())
		{
			std::cout << "Test 9 failed, no split occurred" << std::endl;
		}
		else
		{
			std::cout << "Test 9 passed, a split occurred" << std::endl;
		}
	}
	catch(BadOpcodesException& e)
	{
		std::cout << "Test 9 Passed" << std::endl;
	}
	try
	{
		File::remove(intIndexName);
	}
	catch(const FileNotFoundException &e)
	{

	}
	deleteRelation();
}

void test10()
{
	// Test creates a backward relation and checks to see if root isnt a leaf, this indicates that a split occurred
	std::cout << "--------------------" << std::endl;
	std::cout << "Test 10: checking split functionality" << std::endl;
	try
	{
		createRelationBackward();
		BTreeIndex index = BTreeIndex(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
		if (index.getNodeStatus())
		{
			std::cout << "Test 10 failed, no split occurred" << std::endl;
		}
		else
		{
			std::cout << "Test 10 passed, a split occurred" << std::endl;
		}
	}
	catch(BadOpcodesException& e)
	{
		std::cout << "Test 10 Passed" << std::endl;
	}
	try
	{
		File::remove(intIndexName);
	}
  catch(const FileNotFoundException &e)
  {
  }
	deleteRelation();
}

void test11()
{
	// Create a relation with 100,000 tuples in random order and perform index tests 
	std::cout << "Test 11: relation with 100000 tuples" << std::endl;
	createRelationRandomSize(100000);
	try
	{
		// time duration to insert 100,000 records
		auto begin = std::chrono::high_resolution_clock::now();
		BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
		auto end = std::chrono::high_resolution_clock::now();
		auto durMs = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
		std::cout << "Inserting 100,000 tuples took: " << durMs << " milliseconds" << std::endl;
		largeTests(&index);
	}
	catch(const std::exception& e)
	{
		
	}
	
	try
	{
		File::remove(intIndexName);
	}
	catch(const FileNotFoundException &e)
	{

	}
	deleteRelation();
}

void largeTests(BTreeIndex* index)
{
	// tests for B+ trees with 100,000 entries
	checkPassFail(intScan(index,25,GT,40,LT), 14)
	checkPassFail(intScan(index,20,GTE,35,LTE), 16)
	checkPassFail(intScan(index,-3,GT,3,LT), 3)
	checkPassFail(intScan(index,996,GT,1001,LT), 4)
	checkPassFail(intScan(index,0,GT,1,LT), 0)
	checkPassFail(intScan(index,300,GT,400,LT), 99)
	checkPassFail(intScan(index,3000,GTE,4000,LT), 1000)
	checkPassFail(intScan(index,8000,GT, 30000, LTE), 22000)
	checkPassFail(intScan(index,42000,GTE, 60000, LTE), 18001)
	checkPassFail(intScan(index,28000,GT, 28002, LT), 1)
	checkPassFail(intScan(index,0, GTE, 50000, LT), 50000)
	checkPassFail(intScan(index,0,GTE,50000,LT), 50000)
	checkPassFail(intScan(index,50000,GTE,100000,LT), 50000)
	checkPassFail(intScan(index, 69000, GT, 96000, LTE), 27000)
	checkPassFail(intScan(index, 12345, GT, 54321, LTE), 41976)
	checkPassFail(intScan(index, 22222, GT, 88888, LTE), 66666)
	checkPassFail(intScan(index, 1, GTE, 69696, LTE), 69696)
	checkPassFail(intScan(index, 99990, GT, 200000, LTE), 9)
}

void test12()
{
	// tests with tree with no entries added
	std::cout << "Test 12: relation with no tuples" << std::endl;
	createRelationRandomSize(0);
	
	emptyTests();
	
	try
	{
		File::remove(intIndexName);
	}
	catch(const FileNotFoundException &e)
	{

	}
	deleteRelation();
}

void test13()
{
	// test with a relation with 100000 tuples and a B+ tree with different capacities
	std::cout << "Test 13: relation with 100000 tuples and specified capacities" << std::endl;
	createRelationRandomSize(100000);
	try
	{
		BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER, 100, 60);
		largeTests(&index);
	}
	catch(std::exception &e)
	{
		std::cout << "test failed" << std::endl;
	}
	
	try
	{
		File::remove(intIndexName);
	}
	catch(const FileNotFoundException &e)
	{

	}
	deleteRelation();
}

void test14()
{
	std::cout << "Test 14: root is leaf" << std::endl;
	createRelationRandomSize(100);
	try
	{
		BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
		smallTests(&index);
	}
	catch(std::exception &e)
	{
		std::cout << "Test failed" << std::endl;
	}
	
	try
	{
		File::remove(intIndexName);
	}
	catch(const FileNotFoundException &e)
	{

	}
	deleteRelation();

}

void test15()
{
	std::cout << "Test 15: relation with 100 tuples and specified capacities" << std::endl;
	createRelationRandomSize(100);
	try
	{
		BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER, 6, 4);
		smallTests(&index);
	}
	catch(std::exception &e)
	{
		std::cout << "Test failed" << std::endl;
	}
	
	try
	{
		File::remove(intIndexName);
	}
	catch(const FileNotFoundException &e)
	{

	}
	deleteRelation();

}

void test16()
{
	std::cout << "Test 16: test for when the file already exists" << std::endl;
	createRelationRandomSize(100);
	try
	{
		BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
		smallTests(&index);
	}
	catch(std::exception &e)
	{
		std::cout << "Test failed" << std::endl;
	}

	try
	{
		BTreeIndex index2(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
		smallTests(&index2);
	}
	catch(std::exception &e)
	{
		std::cout << "Test failed" << std::endl;
	}
	
	try
	{
		File::remove(intIndexName);
	}
	catch(const FileNotFoundException &e)
	{

	}
	deleteRelation();

}

void smallTests(BTreeIndex* index)
{
	// tests for tree with 100 values
	checkPassFail(intScan(index, 0, GTE, 100, LT), 100)
	checkPassFail(intScan(index,25,GT,40,LT), 14)
	checkPassFail(intScan(index,20,GTE,35,LTE), 16)
	checkPassFail(intScan(index,-3,GT,3,LT), 3)
	checkPassFail(intScan(index,996,GT,1001,LT), 0)
	checkPassFail(intScan(index,0,GT,1,LT), 0)
	checkPassFail(intScan(index,59,GT,81,LT), 21)
}

void emptyTests()
{
	// test for tree with no values
	BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
	checkPassFail(intScan(&index,2,GT,10,LT), 0)
	checkPassFail(intScan( &index, -2, GTE, 2, LTE), 0)
}

void test17()
{
	std::cout << "Test 17: Tests that each individual entry exists in B+ tree with 100,000 entries and specified capacity" << std::endl;
	createRelationRandomSize(100000);
	try
	{
		BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER, 100, 60);
		allTests(&index, 100000);
	}
	catch(std::exception &e)
	{
		std::cout << "Test 17 failed" << std::endl;
	}
	
	try
	{
		File::remove(intIndexName);
	}
	catch(const FileNotFoundException &e)
	{

	}
	deleteRelation();
}

void allTests(BTreeIndex* index, int relSize)
{
	// checks that all entries from [0, relSize)) are in the B+ tree in one search
	checkPassFail(intScan(index, 0,  GTE, relSize, LT), relSize)
	// checks that all entries from [0, relSize)) are in the B+ tree individually
	for (int i = 0; i < relSize; i++)
	{
		checkPassFail(intScan(index,i, GTE, i + 1, LT), 1)
		checkPassFail(intScan(index,i - 1, GT, i , LTE), 1)
	}
}

// -----------------------------------------------------------------------------
// createRelationForward
// -----------------------------------------------------------------------------

void createRelationForward()
{
	std::vector<RecordId> ridVec;
  // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(const FileNotFoundException &e)
	{
	}

  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  // Insert a bunch of tuples into the relation.
  for(int i = 0; i < relationSize; i++ )
	{
    sprintf(record1.s, "%05d string record", i);
    record1.i = i;
    record1.d = (double)i;
    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
			catch(const InsufficientSpaceException &e)
			{
				file1->writePage(new_page_number, new_page);
  			new_page = file1->allocatePage(new_page_number);
			}
		}
  }

	file1->writePage(new_page_number, new_page);
}

// -----------------------------------------------------------------------------
// createRelationBackward
// -----------------------------------------------------------------------------

void createRelationBackward()
{
  // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(const FileNotFoundException &e)
	{
	}
  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  // Insert a bunch of tuples into the relation.
  for(int i = relationSize - 1; i >= 0; i-- )
	{
    sprintf(record1.s, "%05d string record", i);
    record1.i = i;
    record1.d = i;

    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(RECORD));

		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
			catch(const InsufficientSpaceException &e)
			{
				file1->writePage(new_page_number, new_page);
  			new_page = file1->allocatePage(new_page_number);
			}
		}
  }

	// write to page
	file1->writePage(new_page_number, new_page);
}

// -----------------------------------------------------------------------------
// createRelationRandom
// -----------------------------------------------------------------------------

void createRelationRandom()
{
  // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(const FileNotFoundException &e)
	{
	}
  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  // insert records in random order

  std::vector<int> intvec(relationSize);
  for( int i = 0; i < relationSize; i++ )
  {
    intvec[i] = i;
  }

  long pos;
  int val;
	int i = 0;
  while( i < relationSize )
  {
    pos = rand() % (relationSize-i);
    val = intvec[pos];
    sprintf(record1.s, "%05d string record", val);
    record1.i = val;
    record1.d = val;

    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(RECORD));

		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
			catch(const InsufficientSpaceException &e)
			{
      	file1->writePage(new_page_number, new_page);
  			new_page = file1->allocatePage(new_page_number);
			}
		}

		int temp = intvec[relationSize-1-i];
		intvec[relationSize-1-i] = intvec[pos];
		intvec[pos] = temp;
		i++;
  }
  
	file1->writePage(new_page_number, new_page);
}

// -----------------------------------------------------------------------------
// createRelationRandomSize
// -----------------------------------------------------------------------------
void createRelationRandomSize(int relSize)
{
	// inserts specified number of records in random order
  // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(const FileNotFoundException &e)
	{
	}
  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  // insert records in random order

  std::vector<int> intvec(relSize);
  for( int i = 0; i < relSize; i++ )
  {
    intvec[i] = i;
  }

  long pos;
  int val;
	int i = 0;
  while( i < relSize )
  {
    pos = rand() % (relSize-i);
    val = intvec[pos];
    sprintf(record1.s, "%05d string record", val);
    record1.i = val;
    record1.d = val;

    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(RECORD));

		// write records to page
		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
			catch(const InsufficientSpaceException &e)
			{
      	file1->writePage(new_page_number, new_page);
  			new_page = file1->allocatePage(new_page_number);
			}
		}

		int temp = intvec[relSize-1-i];
		intvec[relSize-1-i] = intvec[pos];
		intvec[pos] = temp;
		i++;
  }
  
	// write page
	file1->writePage(new_page_number, new_page);
}

// -----------------------------------------------------------------------------
// indexTests
// -----------------------------------------------------------------------------

void indexTests()
{
  intTests();
	try
	{
		File::remove(intIndexName);
	}
  catch(const FileNotFoundException &e)
  {
  }
}

// -----------------------------------------------------------------------------
// intTests
// -----------------------------------------------------------------------------

void intTests()
{
  std::cout << "Create a B+ Tree index on the integer field" << std::endl;
  BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);

	// run some tests
	checkPassFail(intScan(&index,25,GT,40,LT), 14)
	checkPassFail(intScan(&index,20,GTE,35,LTE), 16)
	checkPassFail(intScan(&index,-3,GT,3,LT), 3)
	checkPassFail(intScan(&index,996,GT,1001,LT), 4)
	checkPassFail(intScan(&index,0,GT,1,LT), 0)
	checkPassFail(intScan(&index,300,GT,400,LT), 99)
	checkPassFail(intScan(&index,3000,GTE,4000,LT), 1000)
}

int intScan(BTreeIndex * index, int lowVal, Operator lowOp, int highVal, Operator highOp)
{
  RecordId scanRid;
	Page *curPage;

  std::cout << "Scan for ";
  if( lowOp == GT ) { std::cout << "("; } else { std::cout << "["; }
  std::cout << lowVal << "," << highVal;
  if( highOp == LT ) { std::cout << ")"; } else { std::cout << "]"; }
  std::cout << std::endl;

  int numResults = 0;
	
	try
	{
  	index->startScan(&lowVal, lowOp, &highVal, highOp);
	}
	catch(const NoSuchKeyFoundException &e)
	{
    std::cout << "No Key Found satisfying the scan criteria." << std::endl;
		return 0;
	}

	while(1)
	{
		try
		{
			index->scanNext(scanRid);
			bufMgr->readPage(file1, scanRid.page_number, curPage);
			RECORD myRec = *(reinterpret_cast<const RECORD*>(curPage->getRecord(scanRid).data()));
			bufMgr->unPinPage(file1, scanRid.page_number, false);

			if( numResults < 5 )
			{
				std::cout << "at:" << scanRid.page_number << "," << scanRid.slot_number;
				std::cout << " -->:" << myRec.i << ":" << myRec.d << ":" << myRec.s << ":" <<std::endl;
			}
			else if( numResults == 5 )
			{
				std::cout << "..." << std::endl;
			}
		}
		catch(const IndexScanCompletedException &e)
		{
			break;
		}

		numResults++;
	}

  if( numResults >= 5 )
  {
    std::cout << "Number of results: " << numResults << std::endl;
  }
  index->endScan();
  std::cout << std::endl;

	return numResults;
}

// -----------------------------------------------------------------------------
// errorTests
// -----------------------------------------------------------------------------

void errorTests()
{
	{
		std::cout << "Error handling tests" << std::endl;
		std::cout << "--------------------" << std::endl;
		// Given error test

		try
		{
			File::remove(relationName);
		}
		catch(const FileNotFoundException &e)
		{
		}

		file1 = new PageFile(relationName, true);
		
		// initialize all of record1.s to keep purify happy
		memset(record1.s, ' ', sizeof(record1.s));
		PageId new_page_number;
		Page new_page = file1->allocatePage(new_page_number);

		// Insert a bunch of tuples into the relation.
		for(int i = 0; i <10; i++ ) 
		{
		  sprintf(record1.s, "%05d string record", i);
		  record1.i = i;
		  record1.d = (double)i;
		  std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

			while(1)
			{
				try
				{
		  		new_page.insertRecord(new_data);
					break;
				}
				catch(const InsufficientSpaceException &e)
				{
					file1->writePage(new_page_number, new_page);
					new_page = file1->allocatePage(new_page_number);
				}
			}
		}

		file1->writePage(new_page_number, new_page);

		BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
		
		int int2 = 2;
		int int5 = 5;

		// Scan Tests
		std::cout << "Call endScan before startScan" << std::endl;
		try
		{
			index.endScan();
			std::cout << "ScanNotInitialized Test 1 Failed." << std::endl;
		}
		catch(const ScanNotInitializedException &e)
		{
			std::cout << "ScanNotInitialized Test 1 Passed." << std::endl;
		}
		
		std::cout << "Call scanNext before startScan" << std::endl;
		try
		{
			RecordId foo;
			index.scanNext(foo);
			std::cout << "ScanNotInitialized Test 2 Failed." << std::endl;
		}
		catch(const ScanNotInitializedException &e)
		{
			std::cout << "ScanNotInitialized Test 2 Passed." << std::endl;
		}
		
		std::cout << "Scan with bad lowOp" << std::endl;
		try
		{
			index.startScan(&int2, LTE, &int5, LTE);
			std::cout << "BadOpcodesException Test 1 Failed." << std::endl;
		}
		catch(const BadOpcodesException &e)
		{
			std::cout << "BadOpcodesException Test 1 Passed." << std::endl;
		}
		
		std::cout << "Scan with bad highOp" << std::endl;
		try
		{
			index.startScan(&int2, GTE, &int5, GTE);
			std::cout << "BadOpcodesException Test 2 Failed." << std::endl;
		}
		catch(const BadOpcodesException &e)
		{
			std::cout << "BadOpcodesException Test 2 Passed." << std::endl;
		}


		std::cout << "Scan with bad range" << std::endl;
		try
		{
			index.startScan(&int5, GTE, &int2, LTE);
			std::cout << "BadScanrangeException Test 1 Failed." << std::endl;
		}
		catch(const BadScanrangeException &e)
		{
			std::cout << "BadScanrangeException Test 1 Passed." << std::endl;
		}

		deleteRelation();
	}

	try
	{
		File::remove(intIndexName);
	}
  catch(const FileNotFoundException &e)
  {
  }
}

void deleteRelation()
{
	if(file1)
	{
		bufMgr->flushFile(file1);
		delete file1;
		file1 = NULL;
	}
	try
	{
		File::remove(relationName);
	}
	catch(const FileNotFoundException &e)
	{
	}
}
