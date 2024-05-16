package ibd2schema

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

const (
	/** index page header starts at this offset */
	PAGE_HEADER = FSEG_PAGE_DATA
	/** start of data on the page */
	PAGE_DATA = PAGE_HEADER + 36 + 2*FSEG_HEADER_SIZE
	/** number of records in the heap, bit 15=flag: new-style compact page format */
	PAGE_N_HEAP = 4
	/** level of the node in an index tree; the leaf level is the level 0.
	This field should not be written to after page creation. */
	PAGE_LEVEL = 26
	/** index id where the page belongs. This field should not be written to after
	page creation. */
	PAGE_INDEX_ID = 28
	/** number of slots in page directory */
	PAGE_N_DIR_SLOTS = 0
	/** pointer to record heap top */
	PAGE_HEAP_TOP = 2
	/* Offset of the directory start down from the page end. We call the
	slot with the highest file address directory start, as it points to
	the first record in the list of records. */
	PAGE_DIR = FIL_PAGE_DATA_END
	/* We define a slot in the page directory as two bytes */
	PAGE_DIR_SLOT_SIZE = 2
	/** number of user records on the page */
	PAGE_N_RECS = 16
	/** First user record in creation (insertion) order, not necessarily collation
	  order; this record may have been deleted */
	PAGE_HEAP_NO_USER_LOW = 2
	/** Size of an compressed page directory entry */
	PAGE_ZIP_DIR_SLOT_SIZE = 2
	/** Mask of record offsets */
	PAGE_ZIP_DIR_SLOT_MASK uint32 = 0x3fff
	/** 'owned' flag */
	PAGE_ZIP_DIR_SLOT_OWNED = 0x4000
	/** offset of the page infimum record on a new-style compact page */
	PAGE_NEW_INFIMUM = (PAGE_DATA + REC_N_NEW_EXTRA_BYTES)
	/** offset of the page infimum record on an
	  old-style page */
	PAGE_OLD_INFIMUM = PAGE_DATA + 1 + REC_N_OLD_EXTRA_BYTES
	/** offset of the page supremum record on a new-style compact page */
	PAGE_NEW_SUPREMUM = PAGE_DATA + 2*REC_N_NEW_EXTRA_BYTES + 8
	/** offset of the page supremum record end on a new-style compact page */
	PAGE_NEW_SUPREMUM_END = PAGE_NEW_SUPREMUM + 8
	/** offset of the page supremum record on an
	old-style page */
	PAGE_OLD_SUPREMUM = PAGE_DATA + 2 + 2*REC_N_OLD_EXTRA_BYTES + 8
	/** Start offset of the area that will be compressed */
	PAGE_ZIP_START = PAGE_NEW_SUPREMUM_END
	/* The offset of the physically lower end of the directory, counted from
	page end, when the page is empty */
	PAGE_EMPTY_DIR_START = PAGE_DIR + 2*PAGE_DIR_SLOT_SIZE
)

/* The infimum and supremum records are omitted from the compressed page.
On compress, we compare that the records are there, and on uncompress we
restore the records. */
/** Extra bytes of an infimum record */
var INFIMUM_EXTRA = []byte{
	0x01,       /* info_bits=0, n_owned=1 */
	0x00, 0x02, /* heap_no=0, status=2 */
	/* ?, ?     */ /* next=(first user rec, or supremum) */
}

/** Data bytes of an infimum record */
var INFIMUM_DATA = []byte{
	0x69, 0x6e, 0x66, 0x69, 0x6d, 0x75, 0x6d, 0x00, /* "infimum\0" */
}

/** Extra bytes and data bytes of a supremum record */
var SUPREMUM_EXTRA_DATA = []byte{
	/* 0x0?, */ /* info_bits=0, n_owned=1..8 */
	0x00,
	0x0b, /* heap_no=1, status=3 */
	0x00,
	0x00, /* next=0 */
	0x73,
	0x75,
	0x70,
	0x72,
	0x65,
	0x6d,
	0x75,
	0x6d, /* "supremum" */
}

type Page struct {
	*PageSize
	PageNum          uint32
	NHeap            uint16
	NHeapBase        uint16
	NRecs            uint16
	NDense           uint16
	NSlots           uint16
	HeapTop          uint16
	OriginData       []byte
	UncompressedData []byte
	SlotOffset       uint32
	Slot             []byte
	Recs             []uint16
	PageLevel        uint16
	PageType         PageType
	NextPageNum      uint32
}

func NewPage(pageNum uint32, pageSize *PageSize, originData []byte) (p *Page, err error) {
	if pageSize == nil {
		return nil, fmt.Errorf("pageSize is nil")
	}
	p = &Page{
		PageNum:    pageNum,
		PageSize:   pageSize,
		OriginData: originData,
	}
	if pageSize.IsCompressed {
		p.UncompressedData = make([]byte, pageSize.Logical)
		p.Decompress()
		return p, nil
	}
	p.UncompressedData = originData
	return p, nil
}

func (p *Page) HeaderGetField(field int) uint16 {
	return binary.BigEndian.Uint16(p.OriginData[PAGE_HEADER+field:])
}

func (p *Page) GetNHeapBase() {
	p.NHeapBase = p.HeaderGetField(PAGE_N_HEAP)
}

func (p *Page) GetNHeap() {
	p.GetNHeapBase()
	p.NHeap = p.NHeapBase & 0x7fff
}

func (p *Page) GetHeapTop() {
	p.HeapTop = p.HeaderGetField(PAGE_HEAP_TOP)
}

func (p *Page) GetRecType(recOffset uint32) (recType byte) {
	recTypeByte := p.UncompressedData[recOffset-REC_OFF_TYPE]
	/* Retrieve the 3bits to determine the rec type */
	return recTypeByte & 0x7
}

func (p *Page) GetPageType() {
	p.PageType = PageType(binary.BigEndian.Uint16(p.OriginData[FIL_PAGE_TYPE:]))
}

/*
  - Gets the number of user records on page (infimum and supremum records
    are not user records).
*/
func (p *Page) GetNRecs() {
	p.NRecs = p.HeaderGetField(PAGE_N_RECS)
}

/*
  - Determine whether the page is in new-style compact format.
    @return nonzero if the page is in compact format, zero if it is in
    old-style format
*/
func (p *Page) GetIsCompact() {
	p.GetNHeapBase()
	p.IsCompact = (p.NHeapBase & 0x8000) != 0
}

/*
  - Gets the number of elements in the dense page directory,
    including deleted records (the free list).
    @return number of elements in the dense page directory
*/
func (p *Page) GetNDens() {
	/* Exclude the page infimum and supremum from the record count. */
	p.NDense = p.NHeap - PAGE_HEAP_NO_USER_LOW
}

/*
  - Gets the number of dir slots in directory.
    @return number of slots
*/
func (p *Page) GetNSlots() {
	p.NSlots = p.HeaderGetField(PAGE_N_DIR_SLOTS)
}

/*
  - Gets pointer to nth directory slot.
    @return pointer to dir slot
*/
func (p *Page) GetNSlotOffset() (offset uint32) {
	p.GetNSlots()
	offset = p.Logical - PAGE_DIR - uint32(p.NSlots)*PAGE_DIR_SLOT_SIZE
	return offset
}

/*
* Read a given slot in the dense page directory.
@return record offset on the uncompressed page, possibly ORed with
PAGE_ZIP_DIR_SLOT_DEL or PAGE_ZIP_DIR_SLOT_OWNED
*/
func (p *Page) ZipDirGet(slotID uint32) uint16 {
	offset := p.Physical - PAGE_ZIP_DIR_SLOT_SIZE*(slotID+1)
	return binary.BigEndian.Uint16(p.OriginData[offset:])
}

/*
* Determine whether the page is empty.
@return true if the page is empty (PAGE_N_RECS = 0)
*/
func (p *Page) IsEmpty() bool {
	return binary.LittleEndian.Uint16(p.UncompressedData[PAGE_HEADER+PAGE_N_RECS:]) == 0
}

/*
* Determine whether the page is a B-tree leaf.
@return true if the page is a B-tree leaf (PAGE_LEVEL = 0)
*/
func (p *Page) GetPageLevel() {
	p.PageLevel = binary.LittleEndian.Uint16(p.UncompressedData[PAGE_HEADER+PAGE_LEVEL:])
}

func (p *Page) GetNextPageNum() {
	p.NextPageNum = binary.LittleEndian.Uint32(p.UncompressedData[FIL_PAGE_NEXT:])
}

/*
* Used to check the consistency of a record on a page.
@return true if succeed
*/
func (p *Page) RecCheck(recOffset uint32) (err error) {
	p.GetHeapTop()
	if recOffset > uint32(p.HeapTop) {
		return fmt.Errorf("recOffset %d > HeapTop %d", recOffset, p.HeapTop)
	}
	if recOffset < PAGE_DATA {
		return fmt.Errorf("recOffset %d < PAGE_DATA %d", recOffset, PAGE_DATA)
	}
	return nil
}

/*
  - true if the record is the infimum record on a page.
    @return true if the infimum record
*/
func (p *Page) RecIsInfimumLow(recOffset uint32) (isInfimumLow bool, err error) {
	if recOffset < PAGE_NEW_INFIMUM {
		return false, fmt.Errorf("offset %d < PAGE_NEW_INFIMUM %d", recOffset, PAGE_NEW_INFIMUM)
	}
	if recOffset > p.Logical-PAGE_EMPTY_DIR_START {
		return false, fmt.Errorf("offset %d > (p.Logical - PAGE_EMPTY_DIR_START) %d",
			recOffset, p.Logical-PAGE_EMPTY_DIR_START)
	}
	isInfimumLow = recOffset == PAGE_NEW_INFIMUM || recOffset == PAGE_OLD_INFIMUM
	return isInfimumLow, nil
}

/*
  - true if the record is the infimum record on a page.
    @return true if the infimum record
*/
func (p *Page) RecIsInfimum(recOffset uint32) (isInfimum bool, err error) {
	err = p.RecCheck(recOffset)
	if err != nil {
		return false, err
	}
	return p.RecIsInfimumLow(recOffset)
}

/*
* true if the record is the supremum record on a page.
@return true if the supremum record
*/
func (p *Page) RecIsSupremumLow(recOffset uint32) (isSupremumLow bool, err error) {
	if recOffset < PAGE_NEW_SUPREMUM {
		return false, fmt.Errorf("offset %d < PAGE_NEW_SUPREMUM %d", recOffset, PAGE_NEW_SUPREMUM)
	}
	if recOffset > p.Logical-PAGE_EMPTY_DIR_START {
		return false, fmt.Errorf("offset %d > (p.Logical - PAGE_EMPTY_DIR_START) %d",
			recOffset, p.Logical-PAGE_EMPTY_DIR_START)
	}
	isSupremumLow = (recOffset == PAGE_NEW_SUPREMUM || recOffset == PAGE_OLD_SUPREMUM)
	return isSupremumLow, nil
}

/*
* true if the record is the supremum record on a page.
@return true if the supremum record
*/
func (p *Page) RecIsSupremum(recOffset uint32) (isSupremum bool, err error) {
	err = p.RecCheck(recOffset)
	if err != nil {
		return false, err
	}
	return p.RecIsSupremumLow(recOffset)
}

/*
* Populate the sparse page directory from the dense directory.
@return true on success, false on failure
*/
func (p *Page) DecompressDIR() (err error) {
	p.GetNRecs()
	if p.NRecs > p.NDense {
		return fmt.Errorf("NRecs %d > NDense %d", p.NRecs, p.NDense)
	}
	p.Recs = make([]uint16, p.NDense)
	/* Traverse the list of stored records in the sorting order,
	starting from the first user record. */
	p.SlotOffset = p.Logical - PAGE_DIR - PAGE_DIR_SLOT_SIZE
	p.Slot = p.UncompressedData[p.SlotOffset : p.SlotOffset+PAGE_DIR_SLOT_SIZE]
	binary.BigEndian.PutUint16(p.Slot, uint16(PAGE_NEW_INFIMUM))
	p.SlotOffset -= PAGE_DIR_SLOT_SIZE
	p.Slot = p.UncompressedData[p.SlotOffset : p.SlotOffset+PAGE_DIR_SLOT_SIZE]
	/* Initialize the sparse directory and copy the dense directory. */
	for i := 0; i < int(p.NRecs); i++ {
		// Your code here
		offset := p.ZipDirGet(uint32(i))

		value := offset & uint16(PAGE_ZIP_DIR_SLOT_MASK)
		if offset&PAGE_ZIP_DIR_SLOT_OWNED != 0 {
			binary.BigEndian.PutUint16(p.Slot, value)
			p.SlotOffset -= PAGE_DIR_SLOT_SIZE
			p.Slot = p.UncompressedData[p.SlotOffset : p.SlotOffset+PAGE_DIR_SLOT_SIZE]
		}

		if uint32(value) < PAGE_ZIP_START+REC_N_NEW_EXTRA_BYTES {
			return fmt.Errorf("decode zip dir failed: slotID(%d), Nrecs(%d), offset(%d)",
				i, p.NRecs, offset)
		}

		p.Recs[i] = value
	}
	p.SlotOffset -= PAGE_DIR_SLOT_SIZE
	binary.BigEndian.PutUint16(p.Slot, uint16(PAGE_NEW_SUPREMUM))

	lastSlotOffset := p.GetNSlotOffset()
	if lastSlotOffset != p.SlotOffset {
		return fmt.Errorf("decode zip dir failed: offset(%d), slotOffset(%d)",
			lastSlotOffset, p.SlotOffset)
	}
	/* Copy the rest of the dense directory. */
	for i := 0; i < int(p.NDense); i++ {
		offset := p.ZipDirGet(uint32(i))
		if uint32(offset) & ^PAGE_ZIP_DIR_SLOT_MASK != 0 {
			return fmt.Errorf("decode zip dir failed: slotID(%d), Nrecs(%d), offset(%d)",
				i, p.NRecs, offset)
		}
		p.Recs[i] = offset
	}
	sort.Slice(p.Recs[:p.NDense], func(i, j int) bool {
		return p.Recs[i] < p.Recs[j]
	})
	return nil
}

/*
  - The following function is used to set the next record offset field
    of a new-style record.
*/
func (p *Page) RecSetNextOffsNew(data []byte, currentOffset, nextOffset uint32) error {
	if len(p.Recs) == 0 {
		return fmt.Errorf("recs is empty")
	}

	if nextOffset > p.Logical {
		return fmt.Errorf("nextOffset(%d) > p.Logical", nextOffset)
	}

	var fieldValue uint16
	if nextOffset == 0 {
		fieldValue = 0
	} else {
		/* The following two statements calculate
		   next - offset_of_rec mod 64Ki, where mod is the modulo
		   as a non-negative number */

		fieldValue = uint16(nextOffset - currentOffset)
		fieldValue &= uint16(REC_NEXT_MASK)
	}
	binary.BigEndian.PutUint16(data[currentOffset-REC_NEXT:], fieldValue)
	return nil
}

// pageZipDecompressLow decompresses a page. This function should tolerate errors on the compressed
// page. Instead of causing assertions to fail, it will return false if an inconsistency is detected.
//
// Returns true on success, false on failure.
func (p *Page) Decompress() (err error) {
	if !p.IsCompressed {
		return nil
	}

	p.GetNHeap()
	p.GetNDens()

	if uint32(p.NDense*PAGE_ZIP_DIR_SLOT_SIZE) >= p.Physical {
		return fmt.Errorf("nDense(%d)*PAGE_ZIP_DIR_SLOT_SIZE(%d) >= p.PageSize.Physical(%d)",
			p.NDense, PAGE_ZIP_DIR_SLOT_SIZE, p.Physical)
	}
	// copy page header to UncompressedData
	copy(p.UncompressedData, p.OriginData[:PAGE_DATA])

	// Copy the page directory.
	err = p.DecompressDIR()
	if err != nil {
		return err
	}

	// Copy the infimum and supremum records.
	copy(p.UncompressedData[PAGE_NEW_INFIMUM-REC_N_NEW_EXTRA_BYTES:], INFIMUM_EXTRA)
	// memcpy(page + (PAGE_NEW_INFIMUM - REC_N_NEW_EXTRA_BYTES), infimum_extra, sizeof infimum_extra);
	if p.IsEmpty() {
		p.RecSetNextOffsNew(p.UncompressedData, PAGE_NEW_INFIMUM, PAGE_NEW_SUPREMUM)
	} else {
		p.RecSetNextOffsNew(p.UncompressedData, PAGE_NEW_INFIMUM, uint32(p.ZipDirGet(0)&uint16(PAGE_ZIP_DIR_SLOT_MASK)))
	}
	copy(p.UncompressedData[PAGE_NEW_INFIMUM:], INFIMUM_DATA)
	copy(p.UncompressedData[PAGE_NEW_SUPREMUM-REC_N_NEW_EXTRA_BYTES+1:], SUPREMUM_EXTRA_DATA)

	input := p.OriginData[PAGE_DATA : p.Physical-1]
	r, err := zlib.NewReader(bytes.NewReader(input))
	if err != nil {
		return err
	}
	defer r.Close()

	output := p.UncompressedData[PAGE_ZIP_START:]
	if _, err = io.ReadFull(r, output); err != nil {
		return err
	}
	return nil
}

/*
* Gets a bit field from within 1 byte.
in:

	rec: pointer to record origin
	offs: offset from the origin down
	mask: mask used to filter bits
	shift: shift right applied after masking
*/
func (p *Page) RecGetBitField_1(recOffset uint32, offs uint32, mask uint32, shift uint32) uint32 {
	data := p.UncompressedData
	return ((uint32(data[recOffset-offs]) & mask) >> shift)
}

func (p *Page) RecGetDeletedFlag(recOffset uint32) uint32 {
	p.GetIsCompact()
	if p.IsCompact {
		return (p.RecGetBitField_1(
			recOffset,
			REC_NEW_INFO_BITS,
			REC_INFO_DELETED_FLAG,
			REC_INFO_BITS_SHIFT))
	}
	return (p.RecGetBitField_1(
		recOffset,
		REC_OLD_INFO_BITS,
		REC_INFO_DELETED_FLAG,
		REC_INFO_BITS_SHIFT))
}

func (p *Page) RecGetNextOffs(recOffset uint16) (nextRecOffset uint16, err error) {
	recOffsetPos := recOffset - uint16(REC_NEXT)
	fieldValue := binary.BigEndian.Uint16(p.OriginData[recOffsetPos:])
	p.GetIsCompact()
	if p.IsCompact {
		/** Check if the result offset is still on the same page. We allow
		  `field_value` to be interpreted as negative 16bit integer. This check does
		  nothing for 64KB pages. */
		if fieldValue == 0 {
			return 0, nil
		}
		/* There must be at least REC_N_NEW_EXTRA_BYTES + 1
		   between each record. */
		if uint32(fieldValue) > REC_N_NEW_EXTRA_BYTES && fieldValue < 32768 || fieldValue < uint16(0xFFFF+1-REC_N_NEW_EXTRA_BYTES) {
			nextRecOffset = (recOffset + fieldValue) & (uint16(p.Logical) - 1)
			return nextRecOffset, nil
		}
		return 0, fmt.Errorf("field value does not meet expected conditions")
	}
	if fieldValue >= uint16(p.Logical) {
		return 0, fmt.Errorf("fieldValue(%d) >= p.Logical(%d)", fieldValue, p.Logical)
	}
	nextRecOffset = fieldValue
	return nextRecOffset, nil
}
