package ibd2schema

const (

	/* Number of extra bytes in a new-style record,
	in addition to the data and the offsets */
	REC_N_NEW_EXTRA_BYTES uint32 = 5
	// next record mask
	REC_NEXT       uint32 = 2
	REC_NEXT_MASK  uint32 = 0xFFFF
	REC_NEXT_SHIFT uint32 = 0
	/** Stored at rec origin minus 3rd byte. Only 3bits of 3rd byte are used for rec type. */
	REC_OFF_TYPE uint32 = 3
	/** Stored at rec_origin minus 2nd byte and length 2 bytes. */
	REC_OFF_NEXT = 2
	/** Length of TYPE field in record of SDI Index. */
	REC_DATA_TYPE_LEN = 4
	/** Length of ID field in record of SDI Index. */
	REC_DATA_ID_LEN = 8
	/* This is single byte bit-field */
	REC_OLD_INFO_BITS uint32 = 6 /* This is single byte bit-field */
	REC_NEW_INFO_BITS uint32 = 5 /* This is single byte bit-field */
	/** The deleted flag in info bits; when bit is set to 1, it means the record has
	been delete marked */
	REC_INFO_DELETED_FLAG uint32 = 0x20
	REC_INFO_BITS_SHIFT   uint32 = 0
	/* Number of extra bytes in an old-style record,
	in addition to the data and the offsets */
	REC_N_OLD_EXTRA_BYTES = 6
	/** SDI Index record Origin. */
	REC_ORIGIN uint32 = 0
	/** Offset of TYPE field in record (0). */
	REC_OFF_DATA_TYPE uint32 = REC_ORIGIN
	/** Offset of ID field in record (4). */
	REC_OFF_DATA_ID uint32 = REC_OFF_DATA_TYPE + REC_DATA_TYPE_LEN
	/** Offset of 6-byte trx id (12). */
	REC_OFF_DATA_TRX_ID uint32 = REC_OFF_DATA_ID + REC_DATA_ID_LEN
	/** 7-byte roll-ptr (18). */
	REC_OFF_DATA_ROLL_PTR uint32 = REC_OFF_DATA_TRX_ID + DATA_TRX_ID_LEN
	/** 4-byte un-compressed len (25) */
	REC_OFF_DATA_UNCOMP_LEN uint32 = REC_OFF_DATA_ROLL_PTR + DATA_ROLL_PTR_LEN
	/** Length of UNCOMPRESSED_LEN field in record of SDI Index. */
	REC_DATA_UNCOMP_LEN uint32 = 4
	/** 4-byte compressed len (29) */
	REC_OFF_DATA_COMP_LEN uint32 = REC_OFF_DATA_UNCOMP_LEN + REC_DATA_UNCOMP_LEN
	/** Length of SDI Index record header. */
	REC_MIN_HEADER_SIZE uint32 = REC_N_NEW_EXTRA_BYTES
	/** Length of COMPRESSED_LEN field in record of SDI Index. */
	REC_DATA_COMP_LEN uint32 = 4
	/** Variable length Data (33). */
	REC_OFF_DATA_VARCHAR uint32 = REC_OFF_DATA_COMP_LEN + REC_DATA_COMP_LEN
)

const (
	/* Record status values */
	REC_STATUS_ORDINARY = iota
	REC_STATUS_NODE_PTR
	REC_STATUS_INFIMUM
	REC_STATUS_SUPREMUM
)
