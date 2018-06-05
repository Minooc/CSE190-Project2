/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#pragma once

#include <iostream>
#include <string>
#include "string.h"
#include <sstream>

#include "types.h"
#include "page.h"
#include "file.h"
#include "buffer.h"

namespace badgerdb
{

/**
 * @brief Datatype enumeration type.
 */
enum Datatype
{
	INTEGER = 0,
	DOUBLE = 1,
	STRING = 2
};

/**
 * @brief Scan operations enumeration. Passed to BTreeIndex::startScan() method.
 */
enum Operator
{ 
	LT, 	/* Less Than */
	LTE,	/* Less Than or Equal to */
	GTE,	/* Greater Than or Equal to */
	GT		/* Greater Than */
};

/**
 * @brief Size of String key.
 */
const  int STRINGSIZE = 10;

/**
 * @brief Number of key slots in B+Tree leaf for INTEGER key.
 */
//                                                  sibling ptr             key               rid
const  int INTARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) ) / ( sizeof( int ) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree leaf for DOUBLE key.
 */
//                                                     sibling ptr               key               rid
const  int DOUBLEARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) ) / ( sizeof( double ) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree leaf for STRING key.
 */
//                                                    sibling ptr           key                      rid
const  int STRINGARRAYLEAFSIZE = ( Page::SIZE - sizeof( PageId ) ) / ( 10 * sizeof(char) + sizeof( RecordId ) );

/**
 * @brief Number of key slots in B+Tree non-leaf for INTEGER key.
 */
//                                                     level     extra pageNo                  key       pageNo
const  int INTARRAYNONLEAFSIZE = ( Page::SIZE - sizeof( int ) - sizeof( PageId ) ) / ( sizeof( int ) + sizeof( PageId ) );

/**
 * @brief Number of key slots in B+Tree leaf for DOUBLE key.
 */
//                                                        level        extra pageNo                 key            pageNo   -1 due to structure padding
const  int DOUBLEARRAYNONLEAFSIZE = (( Page::SIZE - sizeof( int ) - sizeof( PageId ) ) / ( sizeof( double ) + sizeof( PageId ) )) - 1;

/**
 * @brief Number of key slots in B+Tree leaf for STRING key.
 */
//                                                        level        extra pageNo             key                   pageNo
const  int STRINGARRAYNONLEAFSIZE = ( Page::SIZE - sizeof( int ) - sizeof( PageId ) ) / ( 10 * sizeof(char) + sizeof( PageId ) );

/**
 * @brief Structure to store a key-rid pair. It is used to pass the pair to functions that 
 * add to or make changes to the leaf node pages of the tree. Is templated for the key member.
 */
template <class T>
class RIDKeyPair{
public:
	RecordId rid;
	T key;
	void set( RecordId r, T k)
	{
		rid = r;
		key = k;
	}
};

/**
 * @brief Structure to store a key page pair which is used to pass the key and page to functions that make 
 * any modifications to the non leaf pages of the tree.
*/
template <class T>
class PageKeyPair{
public:
	PageId pageNo;
	T key;
	void set( int p, T k)
	{
		pageNo = p;
		key = k;
	}
};

/**
 * @brief Overloaded operator to compare the key values of two rid-key pairs
 * and if they are the same compares to see if the first pair has
 * a smaller rid.pageNo value.
*/
template <class T>
bool operator<( const RIDKeyPair<T>& r1, const RIDKeyPair<T>& r2 )
{
	if( r1.key != r2.key )
		return r1.key < r2.key;
	else
		return r1.rid.page_number < r2.rid.page_number;
}

/**
 * @brief The meta page, which holds metadata for Index file, is always first page of the btree index file and is cast
 * to the following structure to store or retrieve information from it.
 * Contains the relation name for which the index is created, the byte offset
 * of the key value on which the index is made, the type of the key and the page no
 * of the root page. Root page starts as page 2 but since a split can occur
 * at the root the root page may get moved up and get a new page no.
*/
struct IndexMetaInfo{
  /**
   * Name of base relation.
   */
	char relationName[20];

  /**
   * Offset of attribute, over which index is built, inside the record stored in pages.
   */
	int attrByteOffset;

  /**
   * Type of the attribute over which index is built.
   */
	Datatype attrType;

  /**
   * Page number of root page of the B+ Tree inside the file index file.
   */
	PageId rootPageNo;
};

/*
Each node is a page, so once we read the page in we just cast the pointer to the page to this struct and use it to access the parts
These structures basically are the format in which the information is stored in the pages for the index file depending on what kind of 
node they are. The level memeber of each non leaf structure seen below is set to 1 if the nodes 
at this level are just above the leaf nodes. Otherwise set to 0.
*/

/**
 * @brief Structure for all non-leaf nodes when the key is of INTEGER type.
*/
struct NonLeafNodeInt{
  /**
   * Level of the node in the tree.
   */
	int level;

  /**
   * Stores keys.
   */
	int keyArray[ INTARRAYNONLEAFSIZE ];

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf nodes in the tree.
   */
	PageId pageNoArray[ INTARRAYNONLEAFSIZE + 1 ];
};

/**
 * @brief Structure for all non-leaf nodes when the key is of DOUBLE type.
*/
struct NonLeafNodeDouble{
  /**
   * Level of the node in the tree.
   */
	int level;

  /**
   * Stores keys.
   */
	double keyArray[ DOUBLEARRAYNONLEAFSIZE ];

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf nodes in the tree.
   */
	PageId pageNoArray[ DOUBLEARRAYNONLEAFSIZE + 1 ];
};

/**
 * @brief Structure for all non-leaf nodes when the key is of STRING type.
*/
struct NonLeafNodeString{
  /**
   * Level of the node in the tree.
   */
	int level;

  /**
   * Stores keys.
   */
	char keyArray[ STRINGARRAYNONLEAFSIZE ][ STRINGSIZE ];

  /**
   * Stores page numbers of child pages which themselves are other non-leaf/leaf nodes in the tree.
   */
	PageId pageNoArray[ STRINGARRAYNONLEAFSIZE + 1 ];
};

/**
 * @brief Structure for all leaf nodes when the key is of INTEGER type.
*/
struct LeafNodeInt{
  /**
   * Stores keys.
   */
	int keyArray[ INTARRAYLEAFSIZE ];

  /**
   * Stores RecordIds.
   */
	RecordId ridArray[ INTARRAYLEAFSIZE ];

  /**
   * Page number of the leaf on the right side.
	 * This linking of leaves allows to easily move from one leaf to the next leaf during index scan.
   */
	PageId rightSibPageNo;
};

/**
 * @brief Structure for all leaf nodes when the key is of DOUBLE type.
*/
struct LeafNodeDouble{
  /**
   * Stores keys.
   */
	double keyArray[ DOUBLEARRAYLEAFSIZE ];

  /**
   * Stores RecordIds.
   */
	RecordId ridArray[ DOUBLEARRAYLEAFSIZE ];

  /**
   * Page number of the leaf on the right side.
	 * This linking of leaves allows to easily move from one leaf to the next leaf during index scan.
   */
	PageId rightSibPageNo;
};

/**
 * @brief Structure for all leaf nodes when the key is of STRING type.
*/
struct LeafNodeString{
  /**
   * Stores keys.
   */
	char keyArray[ STRINGARRAYLEAFSIZE ][ STRINGSIZE ];

  /**
   * Stores RecordIds.
   */
	RecordId ridArray[ STRINGARRAYLEAFSIZE ];

  /**
   * Page number of the leaf on the right side.
	 * This linking of leaves allows to easily move from one leaf to the next leaf during index scan.
   */
	PageId rightSibPageNo;
};

/**
 * @brief BTreeIndex class. It implements a B+ Tree index on a single attribute of a
 * relation. This index supports only one scan at a time.
*/
class BTreeIndex {

 private:

  /**
   * File object for the index file.
   */
	File		*file;

  /**
   * Buffer Manager Instance.
   */
	BufMgr	*bufMgr;

  /**
   * Page number of meta page.
   */
	PageId	headerPageNum;

  /**
   * page number of root page of B+ tree inside index file.
   */
	PageId	rootPageNum;

  /**
   * Datatype of attribute over which index is built.
   */
	Datatype	attributeType;

  /**
   * Offset of attribute, over which index is built, inside records. 
   */
	int 		attrByteOffset;

  /**
   * Number of nodes in the tree.
   */
	int			numOfNodes;

  /**
   * Number of keys in leaf node, depending upon the type of key.
   */
	int			leafOccupancy;

  /**
   * Number of keys in non-leaf node, depending upon the type of key.
   */
	int			nodeOccupancy;


	// MEMBERS SPECIFIC TO SCANNING

  /**
   * True if an index scan has been started.
   */
	bool		scanExecuting;

  /**
   * Index of next entry to be scanned in current leaf being scanned.
   */
	int			nextEntry;

  /**
   * Page number of current page being scanned.
   */
	PageId	currentPageNum;

  /**
   * Current Page being scanned.
   */
	Page		*currentPageData;

  /**
   * Low INTEGER value for scan.
   */
	int			lowValInt;

  /**
   * Low DOUBLE value for scan.
   */
	double	lowValDouble;

  /**
   * Low STRING value for scan.
   */
	std::string	lowValString;

  /**
   * High INTEGER value for scan.
   */
	int			highValInt;

  /**
   * High DOUBLE value for scan.
   */
	double	highValDouble;

  /**
   * High STRING value for scan.
   */
	std::string highValString;
	
  /**
   * Low Operator. Can only be GT(>) or GTE(>=).
   */
	Operator	lowOp;

  /**
   * High Operator. Can only be LT(<) or LTE(<=).
   */
	Operator	highOp;

  /**	
   * To keep track the startScanIndex for scanner functionalities
   */
	int startScanIndex;

  /**	
   * String default value for string insertion (\0\0\0\0\0\0\0\0\0\0)
   */
	char* STRINGDEFVAL;

 public:

	/**
   	* BTreeIndex Constructor. 
	* Check to see if the corresponding index file exists. If so, open the file.
	* If not, create it and insert entries for every tuple in the base relation using FileScan class.
	*
   	* @param relationName        Name of file.
   	* @param outIndexName        Return the name of index file.
   	* @param bufMgrIn		Buffer Manager Instance
   	* @param attrByteOffset	Offset of attribute, over which index is to be built, in the record
   	* @param attrType		Datatype of attribute over which index is built
   	* @throws  BadIndexInfoException     If the index file already exists for the corresponding attribute, but values in metapage(relationName, attribute byte offset, attribute type etc.) do not match with values received through constructor parameters.
   	*/
	BTreeIndex(const std::string & relationName, std::string & outIndexName,
						BufMgr *bufMgrIn,	const int attrByteOffset,	const Datatype attrType);
	

  	/**
   	* BTreeIndex Destructor. 
	 * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
	 * and delete file instance thereby closing the index file.
	 * Destructor should not throw any exceptions. All exceptions should be caught in here itself. 
	 * */
	~BTreeIndex();


	/**insertEntry and insertEntryString
	* Insert a new entry indicated by key and record.
	* These function is the main entry of inserting data into the tree and helpers functions will be called
	* from here to handle if the tree needs to be splitted.
	* insertEntry and insertEntryString: 
   	* @param key	Key to insert, pointer to integer/double/char string
   	* @param rid	Record ID of a record whose entry is getting inserted into the index.
   	* insertEntry:
   	*@param leafType	 The type of the leaf that is going to be inserted into
   	*@param nonLeafType 	The type of the leaf that is going to be inserted into 
	**/
	template <typename T, typename L, typename NL>
	const void insertEntry(L* leafType, NL* nonLeafType, T key, const RecordId rid);
	const void insertEntryString(const void* key, const RecordId rid);


	/*
 	*fullNodeHandlerNumber and fullNodeHandlerString
 	* These functions will be called when an element that is just inserted into a function
 	* makes the node full, and hence tree split would be neccessary.
 	* These functions also will restructure the tree once the tree has been splitted after insertion
	*fullHandlerNumber and fullHandlerString: 
	*@param: currNode	The current node that just got inserted into
 	*@param: parentNode	The parent node of the node that just got inserted into.
	*@param: currPageNo	the page number of the current node that just got inserted into
	*@param: isLeaf 	Flag to indicate leaf node is calling the function
	*fullNodeHandlerString:
	*@param: LeafType	Leaf node to indicate which type of leaf node we're inserting into
	*@param: nonleafType	Non leaf node to indicate which type of non-leaf node we're inserting into
	*@param: isRoot		Flag to indicate root node is calling the function
	*/
	template<typename L, typename NL>
	void fullNodeHandlerNumber(L* leafType, NL* nonLeafType, void* currNode, NL* parentNode, PageId currPageNo, bool isLeaf, bool isRoot);
	void fullNodeHandlerString(void * currNode, NonLeafNodeString *parentNode, PageId currPageNo, bool isLeaf); 


	/*
 	*insertToNodeNumber and insertToNodeString
	*These functions handle basic insertion to the leaf node and it's always guaranteed
 	*the node is never full before inserted
	*@param: node	 	The leaf node that is going to be inserted into
	*@param: keyValue	key that is going to be inserted into node
	*@param: recordId	record id that is going to be inserted into node 
	**/
	template<typename T, typename L>
	const void insertToNodeNumber(L * node, T keyValue, const RecordId rid);
	const void insertToNodeString(LeafNodeString * node, const void * key, const RecordId rid);

	/*
 	*splitLeafNode and splitLeafNodeString
 	*These functions handle the splitting of the leaf nodes into two parts: left and right
	* nodes while also assigning middleKey and newly created pageId by reference
	*@param: leftNode	The full leaf node that is going to be splitted
	*@param: middleKey	The new middle key that is going to be assigned by reference
	*@param: pid		Page id of the newly created node and assigned by reference
 	*/ 
	template<typename T, typename L>
	void splitLeafNode(L *& leftNode, T& middleKey, PageId &pid);
	void splitLeafNodeString(LeafNodeString *& leftNode, char* middleKey, PageId &pid);


	/*
 	*splitNonLeafNode and splitNonLeafNnodeString
	*Same concept as splitLeafNode and splitNonLeafNode, this will handle the node splitting
	* of a leafNode while alsoo assigning middle key and newly created node's page id by reffrence
	*@param: leftNode	The full leaf node that needs to be splitted
	*@param: middleKey	The new middle key that is going to be assigned by reference
	*@param: pid		Page id of the newly created node and assigned by reference
 	*/ 
	template<typename T, typename NL>
	void splitNonLeafNode(NL *& leftNode, T& middleKey, PageId &pid); 
	void splitNonLeafNodeString(NonLeafNodeString *& leftNode, char* middleKey, PageId &pid);

	/*
	 *traverseAndInsertNumber and traverseAndInsertString
	 *These functions will find the appropriate node for a record to be inserted into.
	 *Once it find the appropriate node, it will also directly insert into it. 
	 *It will also call fullNodeHandler if the node before of after insertion is full
	 *traverse and traverseString:
	 *@params: currNode	 the root node of the tree
	 *@params: key		 the key of the record that is going to be inserted
	 *@params: rid		 the record id of the record that is going to be inserted 
	 *traverse:
	 *@param: LeafType	Leaf node to indicate which type of root node of the tree
	 *@param: nonleafType	Non leaf node to indicate which type of non-leaf of the tree
	 * */ 
	template <typename T, typename L, typename NL>
	void traverseAndInsertNumber(L* leafType, NL* nonLeafType, NL * currNode, T key, const RecordId rid); 
	void traverseAndInsertString(NonLeafNodeString * currNode, const void* key, const RecordId rid);
	/*
	 *printTree
	 *Simply print tree for debugging purposes
	 */ 
	void printTree();


	/**
	*initializeInt/Double/String. Node intialzer based on the data type.
 	*This functions will populate the node that is passed into the function with the default values.
 	*Default values are: -1 for Int and Double, \0\0\0\0\0\0\0\0\0\0 for String.
 	*@param rootNode:	the node that is going to be initialized with default value.
 	*/
	const void initializeInt(LeafNodeInt* rootNode);
	const void initializeDouble(LeafNodeDouble* rootNode);
	const void initializeString(LeafNodeString* rootNode);
 
 	/**
 	 * startScan: The main entry of scanning the records. 	
	 * Begin a filtered scan of the index. This function will simply call another starScan
	 * functions based on the type of the tree 
   	* @param lowVal	Low value of range, pointer to integer / double / char string
   	* @param lowOp		Low operator (GT/GTE)
   	* @param highVal	High value of range, pointer to integer / double / char string
   	* @param highOp	High operator (LT/LTE)
   	* @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values 
   	* @throws  BadScanrangeException If lowVal > highval
	 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
	**/
	const void startScan(const void* lowVal, const Operator lowOp, const void* highVal, const Operator highOp);

	/*
 	* startScanNumber and startScanString
 	* These functions will find an appropriate node to where the searched recorded
 	* being laid out on the node
	* startScanGeneric and startScanString
   	* @param lowVal		Low value of range, pointer to integer / double / char string
   	* @param lowOp		Low operator (GT/GTE)
   	* @param highVal	High value of range, pointer to integer / double / char string
   	* @param highOp		High operator (LT/LTE)
	* startScanNumber:
	 *@param: LeafType	Leaf node to indicate which type of root node of the tree
	 *@param: nonleafType	Non leaf node to indicate which type of non-leaf of the tree
	*/
	template <typename T, typename L, typename NL>
	const void startScanNumber (L* leafType, NL* nonLeafType, T lowVal, T highVal); 
	const void startScanString (const void* lowVal, const Operator lowOp, const void * highVal, const Operator highOp);

  	/**
 	 * scanNext 	
 	 * The main entry to scanNext. This function will call the scanner helper function based on the data type.	
	 * Fetch the record id of the next index entry that matches the scan.
	 * Return the next record from current page being scanned. If current page has been scanned to its entirety,
	 * move on to the right sibling of current page, if any exists, to start scanning that page.
   	* @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
	* @throws ScanNotInitializedException If no scan has been initialized.
	**/
	const void scanNext(RecordId& outRid);  // returned record id

	/**
 	* scanNextNumber and scanNextString
	* These functions contains the algorithm that will find the appropriate the next appropriate
	* record in the node of the tree while keeping track of last scanned index.
	* scanNextGeneric and scanNextString: 
  	* @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
	* scanNextNumber:
   	* @param lowVal		Low value of range, pointer to integer / double / char string
   	* @param lowOp		Low operator (GT/GTE)
   	* @param highVal	High value of range, pointer to integer / double / char string
   	* @param highOp		High operator (LT/LTE)
	* @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
 	*/
	template <typename T, typename L>
	const void scanNextNumber(L* leafType, RecordId& outRid, T lowVal, T highVal);
	const void scanNextString(RecordId& outRid);	

  	/**
	 * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
	 * @throws ScanNotInitializedException If no scan has been initialized.
	**/
	const void endScan();
	
};

}
