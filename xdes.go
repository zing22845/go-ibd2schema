package ibd2schema

const (
	/*                      EXTENT DESCRIPTOR
	                        =================
	File extent descriptor data structure: contains bits to tell which pages in
	the extent are free and which contain old tuple version to clean. */
	/*-------------------------------------*/
	// The identifier of the segment to which this extent belongs
	XDES_ID = 0
	// The list node data structure for the descriptors
	XDES_FLST_NODE = 8
	// contains state information of the extent
	XDES_STATE = FLST_NODE_SIZE + 8
	// Descriptor bitmap of the pages in the extent
	XDES_BITMAP = FLST_NODE_SIZE + 12
	/** How many bits are there per page */
	XDES_BITS_PER_PAGE = 2
	/** Offset of the descriptor array on a descriptor page */
	XDES_ARR_OFFSET = FSP_HEADER_OFFSET + FSP_HEADER_SIZE
	/*-------------------------------------*/
) // end of const block

func GetXdesSize(pageSize *PageSize) uint32 {
	return XDES_BITMAP + UTBitsInBytes(FspExtentSize(pageSize.Logical)*XDES_BITS_PER_PAGE)
}

/*
* Calculates the descriptor array size.
@param[in]      page_size       page size
@return size of descriptor array
*/
func XdesArrSize(pageSize *PageSize) uint32 {
	return pageSize.Physical / FspExtentSize(pageSize.Logical)
}
