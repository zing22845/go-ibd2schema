package ibd2schema

import "math"

type PageType uint16

// FIL file
const (
	// The number of bytes required to store the file space header
	FIL_PAGE_DATA = 38
	// address size is 6 bytes
	FIL_ADDR_SIZE = 6
	/** page offset inside space */
	FIL_PAGE_OFFSET                  = 4
	FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID = 34
	// The physical size of a list base node in bytes
	FLST_BASE_NODE_SIZE = 4 + 2*FIL_ADDR_SIZE
	FLST_NODE_SIZE      = 2 * FIL_ADDR_SIZE // The physical size of a list node in bytes
	// page type offset in page
	FIL_PAGE_TYPE = 24
	/** File page types (values of FIL_PAGE_TYPE) @{ */
	/** B-tree node */
	FIL_PAGE_INDEX = 17855

	/** R-tree node */
	FIL_PAGE_RTREE = 17854

	/** Tablespace SDI Index page */
	FIL_PAGE_SDI = 17853

	/** This page type is unused. */
	FIL_PAGE_TYPE_UNUSED = 1

	/** Undo log page */
	FIL_PAGE_UNDO_LOG = 2

	/** Index node */
	FIL_PAGE_INODE = 3

	/** Insert buffer free list */
	FIL_PAGE_IBUF_FREE_LIST = 4

	/* File page types introduced in MySQL/InnoDB 5.1.7 */
	/** Freshly allocated page */
	FIL_PAGE_TYPE_ALLOCATED = 0

	/** Insert buffer bitmap */
	FIL_PAGE_IBUF_BITMAP = 5

	/** System page */
	FIL_PAGE_TYPE_SYS = 6

	/** Transaction system data */
	FIL_PAGE_TYPE_TRX_SYS = 7

	/** File space header */
	FIL_PAGE_TYPE_FSP_HDR = 8

	/** Extent descriptor page */
	FIL_PAGE_TYPE_XDES = 9

	/** Uncompressed BLOB page */
	FIL_PAGE_TYPE_BLOB = 10

	/** First compressed BLOB page */
	FIL_PAGE_TYPE_ZBLOB = 11

	/** Subsequent compressed BLOB page */
	FIL_PAGE_TYPE_ZBLOB2 = 12

	/*
	   * In old tablespaces, garbage in FIL_PAGE_TYPE is replaced with
	   this value when flushing pages.
	*/
	FIL_PAGE_TYPE_UNKNOWN = 13

	/** Compressed page */
	FIL_PAGE_COMPRESSED = 14

	/** Encrypted page */
	FIL_PAGE_ENCRYPTED = 15

	/** Compressed and Encrypted page */
	FIL_PAGE_COMPRESSED_AND_ENCRYPTED = 16

	/** Encrypted R-tree page */
	FIL_PAGE_ENCRYPTED_RTREE = 17

	/** Uncompressed SDI BLOB page */
	FIL_PAGE_SDI_BLOB = 18

	/** Compressed SDI BLOB page */
	FIL_PAGE_SDI_ZBLOB = 19

	/** Legacy doublewrite buffer page. */
	FIL_PAGE_TYPE_LEGACY_DBLWR = 20

	/** Rollback Segment Array page */
	FIL_PAGE_TYPE_RSEG_ARRAY = 21

	/** Index pages of uncompressed LOB */
	FIL_PAGE_TYPE_LOB_INDEX = 22

	/** Data pages of uncompressed LOB */
	FIL_PAGE_TYPE_LOB_DATA = 23

	/** The first page of an uncompressed LOB */
	FIL_PAGE_TYPE_LOB_FIRST = 24

	/** The first page of a compressed LOB */
	FIL_PAGE_TYPE_ZLOB_FIRST = 25

	/** Data pages of compressed LOB */
	FIL_PAGE_TYPE_ZLOB_DATA = 26

	/*
	   * Index pages of compressed LOB. This page contains an array of
	   z_index_entry_t objects.
	*/
	FIL_PAGE_TYPE_ZLOB_INDEX = 27

	/** Fragment pages of compressed LOB. */
	FIL_PAGE_TYPE_ZLOB_FRAG = 28

	/** Index pages of fragment pages (compressed LOB). */
	FIL_PAGE_TYPE_ZLOB_FRAG_ENTRY = 29

	/** Note the highest valid non-index page_type_t. */
	FIL_PAGE_TYPE_LAST = FIL_PAGE_TYPE_ZLOB_FRAG_ENTRY

	FIL_PAGE_DATA_END = 8

	/** if there is a 'natural' successor of the page, its offset. Otherwise
	FIL_NULL. B-tree index pages(FIL_PAGE_TYPE contains FIL_PAGE_INDEX) on the
	same PAGE_LEVEL are maintained as a doubly linked list via FIL_PAGE_PREV and
	FIL_PAGE_NEXT in the collation order of the smallest user record on each
	page. */
	FIL_PAGE_NEXT = 12

	FIL_NULL = math.MaxUint32
)

