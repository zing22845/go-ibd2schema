package ibd2schema

const (
	/** 8 bytes containing the length of the externally stored part of the LOB.
	  The 2 highest bits are reserved to the flags below. */
	BTR_EXTERN_LEN = 12
	/** page number where stored */
	BTR_EXTERN_PAGE_NO = 4
	LOB_HDR_PART_LEN   = 0
	LOB_HDR_SIZE       = 10
	LOB_PAGE_DATA = FIL_PAGE_DATA + LOB_HDR_SIZE
)
