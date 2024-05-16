package ibd2schema

// universal page sizes
const (
	/* Define the Min, Max, Default page sizes. */
	/** Minimum Page Size Shift (power of 2) */
	UNIV_PAGE_SIZE_SHIFT_MIN = 12
	/** Maximum Page Size Shift (power of 2) */
	UNIV_PAGE_SIZE_SHIFT_MAX = 16
	/** Original 16k InnoDB Page Size Shift, in case the default changes */
	UNIV_PAGE_SIZE_SHIFT_ORIG = 14
	/** Minimum page size InnoDB currently supports. */
	UNIV_PAGE_SIZE_MIN = 1 << UNIV_PAGE_SIZE_SHIFT_MIN
	/** Maximum page size InnoDB currently supports. */
	UNIV_PAGE_SIZE_MAX = 1 << UNIV_PAGE_SIZE_SHIFT_MAX
	/** Original 16k page size for InnoDB tablespaces. */
	UNIV_PAGE_SIZE_ORIG = 1 << UNIV_PAGE_SIZE_SHIFT_ORIG
	/** log2 of smallest compressed page size (1<<10 == 1024 bytes)
	Note: This must never change! */
	UNIV_ZIP_SIZE_SHIFT_MIN = 10
	/** log2 of largest compressed page size (1<<14 == 16384 bytes).
	A compressed page directory entry reserves 14 bits for the start offset
	and 2 bits for flags. This limits the uncompressed page size to 16k.
	Even though a 16k uncompressed page can theoretically be compressed
	into a larger compressed page, it is not a useful feature so we will
	limit both with this same constant. */
	UNIV_ZIP_SIZE_SHIFT_MAX = 14
	/** Smallest compressed page size */
	UNIV_ZIP_SIZE_MIN = 1 << UNIV_ZIP_SIZE_SHIFT_MIN
	/** Largest compressed page size */
	UNIV_ZIP_SIZE_MAX = 1 << UNIV_ZIP_SIZE_SHIFT_MAX
	/** Original 16k InnoDB Page Size as an ssize (log2 - 9) */
	UNIV_PAGE_SSIZE_ORIG = UNIV_PAGE_SIZE_SHIFT_ORIG - 9
)

// Universal page size
var UNIV_PAGE_SIZE uint32

// set UNIV_PAGE_SIZE
func SetUnivPageSize(size uint32) {
	if size < UNIV_PAGE_SIZE_MIN || size > UNIV_PAGE_SIZE_MAX {
		panic("invalid page size")
	}
	UNIV_PAGE_SIZE = size
}
