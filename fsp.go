package ibd2schema

import "encoding/binary"

// FSP file space
const (
	// Offset of the space header within a file page
	FSP_HEADER_OFFSET = FIL_PAGE_DATA
	// The number of bytes required to store SDI root page number(4) and SDI version(4) at Page 0
	FSP_SDI_HEADER_LEN = 8
	// fsp_space_t.flags, similar to dict_table_t::flags
	FSP_SPACE_FLAGS = 16
	// Number of flag bits used to indicate the tablespace page size
	FSP_FLAGS_WIDTH_PAGE_SSIZE uint32 = 4
	/** Zero relative shift position of the POST_ANTELOPE field */
	FSP_FLAGS_POS_POST_ANTELOPE uint32 = 0
	/** Width of the POST_ANTELOPE flag */
	FSP_FLAGS_WIDTH_POST_ANTELOPE uint32 = 1
	/** Number of flag bits used to indicate the tablespace zip page size */
	FSP_FLAGS_WIDTH_ZIP_SSIZE uint32 = 4
	/** Zero relative shift position of the ZIP_SSIZE field */
	FSP_FLAGS_POS_ZIP_SSIZE uint32 = FSP_FLAGS_POS_POST_ANTELOPE + FSP_FLAGS_WIDTH_POST_ANTELOPE
	// Zero relative shift position of the ATOMIC_BLOBS field
	FSP_FLAGS_POS_ATOMIC_BLOBS uint32 = FSP_FLAGS_POS_ZIP_SSIZE + FSP_FLAGS_WIDTH_ZIP_SSIZE
	/** Width of the SDI flag.  This flag indicates the presence of
	tablespace dictionary.*/
	FSP_FLAGS_WIDTH_SDI uint32 = 1
	/** Width of the encryption flag.  This flag indicates that the tablespace
	is a tablespace with encryption. */
	FSP_FLAGS_WIDTH_ENCRYPTION = 1
	/** Zero relative shift position of the start of the DATA_DIR bit */
	FSP_FLAGS_POS_DATA_DIR = FSP_FLAGS_POS_PAGE_SSIZE + FSP_FLAGS_WIDTH_PAGE_SSIZE
	/** Width of the DATA_DIR flag.  This flag indicates that the tablespace
	is found in a remote location, not the default data directory. */
	FSP_FLAGS_WIDTH_DATA_DIR = 1
	/** Zero relative shift position of the start of the SHARED bit */
	FSP_FLAGS_POS_SHARED = FSP_FLAGS_POS_DATA_DIR + FSP_FLAGS_WIDTH_DATA_DIR
	/** Width of the TEMPORARY flag.  This flag indicates that the tablespace
	is a temporary tablespace and everything in it is temporary, meaning that
	it is for a single client and should be deleted upon startup if it exists. */
	FSP_FLAGS_WIDTH_TEMPORARY = 1
	/** Width of the SHARED flag.  This flag indicates that the tablespace
	was created with CREATE TABLESPACE and can be shared by multiple tables. */
	FSP_FLAGS_WIDTH_SHARED = 1
	/** Zero relative shift position of the start of the TEMPORARY bit */
	FSP_FLAGS_POS_TEMPORARY = FSP_FLAGS_POS_SHARED + FSP_FLAGS_WIDTH_SHARED
	/** Zero relative shift position of the start of the ENCRYPTION bit */
	FSP_FLAGS_POS_ENCRYPTION = FSP_FLAGS_POS_TEMPORARY + FSP_FLAGS_WIDTH_TEMPORARY
	/** Zero relative shift position of the start of the SDI bits */
	FSP_FLAGS_POS_SDI = FSP_FLAGS_POS_ENCRYPTION + FSP_FLAGS_WIDTH_ENCRYPTION
	/** Width of the ATOMIC_BLOBS flag.  The ability to break up a long
	column into an in-record prefix and an externally stored part is available
	to ROW_FORMAT=REDUNDANT and ROW_FORMAT=COMPACT. */
	FSP_FLAGS_WIDTH_ATOMIC_BLOBS uint32 = 1
	// Zero relative shift position of the PAGE_SSIZE field
	FSP_FLAGS_POS_PAGE_SSIZE uint32 = FSP_FLAGS_POS_ATOMIC_BLOBS + FSP_FLAGS_WIDTH_ATOMIC_BLOBS
	// Bit mask of the PAGE_SSIZE field
	FSP_FLAGS_MASK_PAGE_SSIZE uint32 = (1<<FSP_FLAGS_WIDTH_PAGE_SSIZE - 1) << FSP_FLAGS_POS_PAGE_SSIZE
	/** Bit mask of the ZIP_SSIZE field */
	FSP_FLAGS_MASK_ZIP_SSIZE uint32 = (1<<FSP_FLAGS_WIDTH_ZIP_SSIZE - 1) << FSP_FLAGS_POS_ZIP_SSIZE
	/** Bit mask of the SDI field */
	FSP_FLAGS_MASK_SDI uint32 = (1<<FSP_FLAGS_WIDTH_SDI - 1) << FSP_FLAGS_POS_SDI
	/* File space header size */
	FSP_HEADER_SIZE = 32 + 5*FLST_BASE_NODE_SIZE

	/** On a page of any file segment, data
	may be put starting from this offset */
	FSEG_PAGE_DATA = FIL_PAGE_DATA
	/** Length of the file system  header, in bytes */
	FSEG_HEADER_SIZE = 10
) // end of const block

/*
* Read the flags from the tablespace header page.
@param[in]      page    first page of a tablespace
@return the contents of FSP_SPACE_FLAGS
*/
func FspHeaderGetFlags(page []byte) uint32 {
	return FspHeaderGetField(page, FSP_SPACE_FLAGS)
}

/*
* Read a tablespace header field.
@param[in]      page    first page of a tablespace
@param[in]      field   the header field
@return the contents of the header field
*/
func FspHeaderGetField(page []byte, field uint32) uint32 {
	offset := FSP_HEADER_OFFSET + field
	return binary.LittleEndian.Uint32(page[offset : offset+4])
}

// FspFlagsGetPageSsize returns the value of the PAGE_SSIZE field from the given flags.
func FspFlagsGetPageSsize(flags uint32) uint32 {
	return (flags & FSP_FLAGS_MASK_PAGE_SSIZE) >> FSP_FLAGS_POS_PAGE_SSIZE
}

/** FspFlagsGetZipSsize Return the value of the ZIP_SSIZE field */
func FspFlagsGetZipSsize(flags uint32) uint32 {
	return (flags & FSP_FLAGS_MASK_ZIP_SSIZE) >> FSP_FLAGS_POS_ZIP_SSIZE
}

/** Return the value of the SDI field */
func FspFlagsHasSDI(flags uint32) uint32 {
	return (flags & FSP_FLAGS_MASK_SDI) >> FSP_FLAGS_POS_SDI
}

func FspHeaderGetSDIOffset(pageSize *PageSize) (offset uint32) {
	offset = XDES_ARR_OFFSET +
		GetXdesSize(pageSize)*XdesArrSize(pageSize) +
		INFO_MAX_SIZE
	return offset
}

/** File space extent size in pages
page size | file space extent size
----------+-----------------------
   4 KiB  | 256 pages = 1 MiB
   8 KiB  | 128 pages = 1 MiB
  16 KiB  |  64 pages = 1 MiB
  32 KiB  |  64 pages = 2 MiB
  64 KiB  |  64 pages = 4 MiB
*/
// FSPExtentSize calculates the file space extent size in pages based on a given page size
func FspExtentSize(size uint32) uint32 {
	switch {
	case size <= 16*KiB:
		return MiB / size
	case size <= 32*KiB:
		return 2 * MiB / size
	default:
		return 4 * MiB / size
	}
}
