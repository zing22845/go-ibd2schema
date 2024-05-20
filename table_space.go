package ibd2schema

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	/** SDI BLOB not expected before the following page number.
	0 (tablespace header), 1 (tabespace bitmap), 2 (ibuf bitmap)
	3 (SDI Index root page) */
	SDI_BLOB_ALLOWED = 4
)

type TablespaceFile struct {
	/** 0 in fil_per_table tablespaces, else the first page number in
	  subsequent data file in multi-file tablespace. */
	FirstPageNum uint32
	/** Total number of pages in a data file. */
	TotalNumOfPages uint32
	/** File handle of the data file. */
	File *os.File
}

type TableSpace struct {
	Reader          io.Reader
	Buf             *bytes.Buffer
	Flags           uint32
	SpaceID         uint32
	FirstPageNum    uint32
	PageSize        *PageSize
	TablespaceFiles []*TablespaceFile
	SDIVersion      uint32
	SDIRootOffset   uint32
	SDIRootPageNum  uint32
	SDIRootPage     *Page
	SDIPages        []*Page
	SDIPagesMap     map[uint32]*Page
	LeafSDIPage     *Page
	SDIs            []*SDI
	CurPage         *Page
	SDIResult       []byte
	DDLs            []string
}

func NewTableSpace(r io.Reader) (ts *TableSpace, err error) {
	ts = &TableSpace{
		Reader: r,
	}
	ts.Buf = bytes.NewBuffer(nil)

	_, err = io.CopyN(ts.Buf, ts.Reader, int64(UNIV_ZIP_SIZE_MIN))
	if err != nil {
		return nil, err
	}

	data := ts.Buf.Bytes()
	ts.SpaceID = binary.BigEndian.Uint32(data[FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID:])
	ts.FirstPageNum = binary.BigEndian.Uint32(data[FIL_PAGE_OFFSET:])

	err = ts.GetPageSize()
	if err != nil {
		return nil, err
	}
	// complete a page
	err = ts.ReadToOffset(ts.PageSize.Physical)
	if err != nil {
		return nil, fmt.Errorf("not enough data to read a page, err:%v", err)
	}

	if ts.FirstPageNum != 0 {
		return nil, fmt.Errorf("invalid first page number, expected 0, got %d", ts.FirstPageNum)
	}

	ts.GetSDIRoot()
	if ts.SDIRootPageNum == 0 {
		return nil, fmt.Errorf("tablespace does not have SDI")
	}
	ts.SDIPages = make([]*Page, 0)
	ts.SDIPagesMap = make(map[uint32]*Page)
	ts.SDIs = make([]*SDI, 0)
	return ts, nil
}

func (ts *TableSpace) ReadToOffset(offset uint32) (err error) {
	if offset > uint32(ts.Buf.Len()) {
		_, err = io.CopyN(ts.Buf, ts.Reader, int64(offset-uint32(ts.Buf.Len())))
		if err != nil {
			return err
		}
	}
	return nil
}

func (ts *TableSpace) GetPageSize() (err error) {
	data := ts.Buf.Bytes()
	ts.Flags = FspHeaderGetFlags(data)
	isValidFlags := true
	if !isValidFlags {
		return fmt.Errorf("invalid flags, page may corrupt")
	}

	ts.PageSize, err = NewPageSizeWithFlag(ts.Flags)
	if err != nil {
		return err
	}
	return nil
}

func (ts *TableSpace) GetSDIRoot() {
	ts.SDIRootOffset = FspHeaderGetSDIOffset(ts.PageSize)
	data := ts.Buf.Bytes()
	ts.SDIVersion = binary.BigEndian.Uint32(data[ts.SDIRootOffset:])
	ts.SDIRootPageNum = binary.BigEndian.Uint32(data[ts.SDIRootOffset+4:])
}

func (ts *TableSpace) FetchPage(pageNum uint32) (page *Page, err error) {
	pageStart := pageNum * ts.PageSize.Physical
	err = ts.ReadToOffset(pageStart + ts.PageSize.Physical)
	if err != nil {
		return nil, fmt.Errorf("get page failed, err:%v", err)
	}
	data := ts.Buf.Bytes()
	pageData := data[pageStart : pageStart+ts.PageSize.Physical]
	page, err = NewPage(pageNum, ts.PageSize, pageData)
	if err != nil {
		return nil, err
	}
	return page, nil
}

func (ts *TableSpace) FetchSDIPageAndLevel(pageNum uint32) (err error) {
	page, err := ts.FetchPage(pageNum)
	if err != nil {
		return fmt.Errorf("fetch page failed, err:%+v", err)
	}
	ts.SDIPages = append(ts.SDIPages, page)
	ts.SDIPagesMap[pageNum] = page
	// check page type
	page.GetPageType()
	if page.PageType != FIL_PAGE_SDI {
		return fmt.Errorf("page type is not SDI")
	}
	// get page level
	page.GetPageLevel()
	return nil
}

/*
Read SDI pages
*/
func (ts *TableSpace) FetchSDIPages() (err error) {
	// fetch root page
	err = ts.FetchSDIPageAndLevel(ts.SDIRootPageNum)
	if err != nil {
		return fmt.Errorf("read sdi page failed, err:%v", err)
	}
	ts.SDIRootPage = ts.SDIPages[0]
	// check recs of root page
	ts.SDIRootPage.GetNRecs()
	if ts.SDIRootPage.NRecs == 0 {
		return fmt.Errorf("SDI is empty")
	}
	// if root page level is 0, then it's the leaf page
	if ts.SDIRootPage.PageLevel == 0 {
		ts.LeafSDIPage = ts.SDIRootPage
		return nil
	}
	var recType byte
	var curPage, lastPage *Page
	curPage = ts.SDIRootPage
	// need to read more pages until reach level 0(leftmost leaf level)
	for {
		/* Find infimum record, from infimum, we can find first record */
		recType = curPage.GetRecType(PAGE_NEW_INFIMUM)
		if recType != REC_STATUS_INFIMUM {
			return fmt.Errorf("INFIMUM not found on index page %d", curPage.PageNum)
		}
		nextRecOffset := binary.BigEndian.Uint16(curPage.UncompressedData[PAGE_NEW_INFIMUM-REC_OFF_NEXT:])
		nextPageNum := binary.BigEndian.Uint32(curPage.UncompressedData[uint16(PAGE_NEW_INFIMUM)+nextRecOffset+REC_DATA_TYPE_LEN+REC_DATA_ID_LEN:])
		if nextPageNum < SDI_BLOB_ALLOWED {
			return fmt.Errorf("invalid child SDI page num:%d", nextPageNum)
		}
		// fetch next SDI page
		err = ts.FetchSDIPageAndLevel(nextPageNum)
		if err != nil {
			return fmt.Errorf("read sdi page failed, err:%v", err)
		}
		// check page level
		lastPage = curPage
		curPage = ts.SDIPages[len(ts.SDIPages)-1]
		if lastPage.PageLevel-1 != curPage.PageLevel {
			return fmt.Errorf("page level not match, lastPageLevel:%d, curPageLevel:%d",
				lastPage.PageLevel, curPage.PageLevel)
		}
		// 0 means leaf page
		if curPage.PageLevel == 0 {
			ts.LeafSDIPage = curPage
			return nil
		}
	}
}

func (ts *TableSpace) GetNextRecOffset(recOffset uint16) (nextRecOffset uint16, err error) {
	nextRecOffset, err = ts.CurPage.RecGetNextOffs(recOffset)
	if err != nil {
		return 0, err
	}
	if nextRecOffset == 0 {
		return 0, fmt.Errorf("record curruption detected")
	}
	if nextRecOffset > uint16(len(ts.CurPage.UncompressedData)) {
		return 0, fmt.Errorf("nextRecOffset:%d exceeds page len", nextRecOffset)
	}
	deletedFlag := ts.CurPage.RecGetDeletedFlag(uint32(nextRecOffset))
	if deletedFlag != 0 {
		return ts.GetNextRecOffset(nextRecOffset)
	}
	recType := ts.CurPage.GetRecType(uint32(nextRecOffset))
	if recType == REC_STATUS_SUPREMUM {
		/* Reached last record on current page.
		   Fetch record from next_page */
		ts.CurPage.GetNextPageNum()
		nextPageNum := ts.CurPage.NextPageNum
		if nextPageNum == FIL_NULL {
			return 0, nil
		}
		fmt.Printf("nextPageNum:%d\n", nextPageNum)
		var ok bool
		ts.CurPage, ok = ts.SDIPagesMap[nextPageNum]
		if !ok {
			ts.CurPage, err = ts.FetchPage(nextPageNum)
			if err != nil {
				return 0, fmt.Errorf("read sdi page failed, err:%v", err)
			}
		}
		nextRecOffset, err = ts.GetFirstUserRec()
		if err != nil {
			return 0, err
		}
	}
	return nextRecOffset, nil
}

func (ts *TableSpace) GetFirstUserRec() (curRecOffset uint16, err error) {
	data := ts.CurPage.UncompressedData
	nextRecOffsetPos := uint16(PAGE_NEW_INFIMUM - REC_OFF_NEXT)
	nextRecOffset := binary.BigEndian.Uint16(data[nextRecOffsetPos:])
	/* First user record shouldn't be supremum */
	if PAGE_NEW_INFIMUM+uint32(nextRecOffset) == PAGE_NEW_SUPREMUM {
		return 0, fmt.Errorf("first user record shouldn't be supremum")
	}
	if int(nextRecOffset) > len(data) {
		return 0, fmt.Errorf("nextRecOffset:%d exceeded page len", nextRecOffset)
	}
	curRecOffset = uint16(PAGE_NEW_INFIMUM) + nextRecOffset
	/* current rec should be within page */
	if int(curRecOffset) >= len(data) {
		return 0, fmt.Errorf("curRecOffset:%d exceeded page len", curRecOffset)
	}
	deletedFlag := ts.CurPage.RecGetDeletedFlag(uint32(curRecOffset))
	if deletedFlag == 0 {
		return curRecOffset, nil
	}
	/* record is delete marked, get next record */
	fmt.Printf("debug 1\n")
	curRecOffset, err = ts.GetNextRecOffset(curRecOffset)
	if err != nil {
		return 0, err
	}
	return curRecOffset, nil
}

/*
* Read the compressed blob stored in off-pages to the buffer.
@param[in]	ts			tablespace structure
@param[in]	first_blob_page_num	first blob page number of the chain
@param[in]	total_off_page_length	total Length of blob stored in record
@param[in,out]	dest_buf		blob will be copied to this buffer
@return 0 if blob is not read, else the total length of blob read from
off-pages
*/
func (ts *TableSpace) CopyCompressedBlob(
	firstBlobPageNum uint32, totalOffsetPageLength uint64, destBuf []byte) (err error) {
	if ts.PageSize.IsCompressed {
		return fmt.Errorf("page is not compressed")
	}
	pageNum := firstBlobPageNum
	var partLen uint32
	var blobLenRetrieved uint64
	for {
		page, err := ts.FetchPage(pageNum)
		if err != nil {
			return fmt.Errorf("fetch page failed, err:%+v", err)
		}
		page.GetPageType()
		if page.PageType != FIL_PAGE_SDI_ZBLOB {
			return fmt.Errorf("page type is not FIL_PAGE_SDI_ZBLOB")
		}
		partLen = binary.BigEndian.Uint32(page.OriginData[FIL_PAGE_DATA+LOB_HDR_PART_LEN:])
		blobLenRetrieved += uint64(partLen)
		page.GetNextPageNum()
		if page.NextPageNum <= SDI_BLOB_ALLOWED {
			return fmt.Errorf("page num is not valid")
		}
		if page.NextPageNum == FIL_NULL {
			break
		}
		pageNum = page.NextPageNum
	}
	if blobLenRetrieved != totalOffsetPageLength {
		return fmt.Errorf("calculated length %d != total length %d",
			blobLenRetrieved, totalOffsetPageLength)
	}
	return nil
}

/*
* Read the uncompressed blob stored in off-pages to the buffer.
@param[in]	ts			tablespace structure
@param[in]	first_blob_page_num	first blob page number of the chain
@param[in]	total_off_page_length	total length of blob stored in record
@param[in,out]	dest_buf		blob will be copied to this buffer
@return 0 if blob is not read, else the total length of blob read from
off-pages
*/
func (ts *TableSpace) CopyUncompressedBlob(
	firstBlobPageNum uint32, totalOffsetPageLength uint64, destBuf []byte) (err error) {

	var partLen uint32
	var blobLenRetrieved uint64
	pageNum := firstBlobPageNum
	for {
		page, err := ts.FetchPage(pageNum)
		if err != nil {
			return fmt.Errorf("fetch page failed, err:%+v", err)
		}
		page.GetPageType()
		if page.PageType != FIL_PAGE_SDI_ZBLOB {
			return fmt.Errorf("page type is not FIL_PAGE_SDI_ZBLOB")
		}
		partLen = binary.BigEndian.Uint32(page.OriginData[FIL_PAGE_DATA+LOB_HDR_PART_LEN:])
		copy(destBuf[blobLenRetrieved:], page.OriginData[LOB_PAGE_DATA:LOB_PAGE_DATA+partLen])
		blobLenRetrieved += uint64(partLen)
		page.GetNextPageNum()
		if page.NextPageNum <= SDI_BLOB_ALLOWED {
			return fmt.Errorf("page num is not valid")
		}
		if page.NextPageNum == FIL_NULL {
			break
		}
		pageNum = page.NextPageNum
	}
	if blobLenRetrieved != totalOffsetPageLength {
		return fmt.Errorf("calculated length %d != total length %d",
			blobLenRetrieved, totalOffsetPageLength)
	}
	return nil
}

/*
* Extract SDI record fields
@param[in]	rec		pointer to record
@param[in,out]	sdi_type	sdi type
@param[in,out]	sdi_id		sdi id
@param[in,out]	sdi_data	sdi blob
@param[in,out]	sdi_data_len	length of sdi blob
@return DB_SUCCESS on success, else error code
*/
func (ts *TableSpace) ParseFieldsInRec(curRecOffset uint16) (err error) {
	// check if infimum
	isInfimum, err := ts.CurPage.RecIsInfimum(uint32(curRecOffset))
	if err != nil {
		return err
	}
	// check if supremum
	isSumpremum, err := ts.CurPage.RecIsSupremum(uint32(curRecOffset))
	if err != nil {
		return err
	}
	if isInfimum || isSumpremum {
		return fmt.Errorf("page corruption")
	}

	sdi := &SDI{}
	sdi.Type = uint64(binary.BigEndian.Uint32(ts.CurPage.OriginData[curRecOffset+uint16(REC_OFF_DATA_TYPE):]))
	sdi.ID = binary.BigEndian.Uint64(ts.CurPage.OriginData[curRecOffset+uint16(REC_OFF_DATA_ID):])
	sdi.UncompressedDataLen = uint64(binary.BigEndian.Uint32(ts.CurPage.OriginData[curRecOffset+uint16(REC_OFF_DATA_UNCOMP_LEN):]))
	sdi.OriginDataLen = uint64(binary.BigEndian.Uint32(ts.CurPage.OriginData[curRecOffset+uint16(REC_OFF_DATA_COMP_LEN):]))
	recDataLenPartial := ts.CurPage.OriginData[curRecOffset-uint16(REC_MIN_HEADER_SIZE)-1]

	var recDataLength uint64
	var isRecDataExternal bool
	var recDataInPageLen uint32

	if (recDataLenPartial & 0x80) != 0 {
		/* Rec length is store in two bytes. Read next
		byte and calculate the total length. */
		recDataInPageLen = uint32(recDataLenPartial&0x3f) << 8
		if (recDataLenPartial & 0x40) != 0 {
			isRecDataExternal = true
			/* Rec is stored externally with 768 byte prefix
			inline */
			recDataLength = binary.BigEndian.Uint64(
				ts.CurPage.OriginData[uint32(curRecOffset)+REC_OFF_DATA_VARCHAR+recDataInPageLen+BTR_EXTERN_LEN:])

			recDataLength += uint64(recDataInPageLen)
		} else {
			recDataLength = uint64(ts.CurPage.OriginData[curRecOffset-uint16(REC_MIN_HEADER_SIZE)-2])
			recDataLength += uint64(recDataInPageLen)
		}
	} else {
		/* Rec length is <= 127. Read the length from
		one byte only. */
		recDataLength = uint64(recDataLenPartial)
	}

	sdi.OriginData = make([]byte, recDataLength+1)
	recDataOrigin := ts.CurPage.OriginData[curRecOffset+uint16(REC_OFF_DATA_VARCHAR):]

	if isRecDataExternal {
		if recDataInPageLen != 0 {
			copy(sdi.OriginData, recDataOrigin[:recDataInPageLen])
		}

		/* Copy from off-page blob-pages */
		firstBlobPageNum := binary.BigEndian.Uint32(
			recDataOrigin[REC_OFF_DATA_VARCHAR+recDataInPageLen+BTR_EXTERN_PAGE_NO:])

		if ts.CurPage.IsCompressed {
			err = ts.CopyCompressedBlob(
				firstBlobPageNum, recDataLength-uint64(recDataInPageLen), sdi.OriginData[recDataInPageLen:])
			if err != nil {
				return err
			}
		} else {
			err = ts.CopyUncompressedBlob(
				firstBlobPageNum, recDataLength-uint64(recDataInPageLen), sdi.OriginData[recDataInPageLen:])
			if err != nil {
				return err
			}
		}
	} else {
		copy(sdi.OriginData, recDataOrigin[:recDataLength])
	}

	if recDataLength != uint64(sdi.OriginDataLen) {
		return fmt.Errorf("recDataLength %d != OriginDataLen %d", recDataLength, sdi.OriginDataLen)
	}

	input := sdi.OriginData[:sdi.OriginDataLen]
	r, err := zlib.NewReader(bytes.NewReader(input))
	if err != nil {
		return err
	}
	defer r.Close()
	sdi.UncompressedData = make([]byte, sdi.UncompressedDataLen)
	n, err := io.ReadFull(r, sdi.UncompressedData)
	if err != nil {
		return fmt.Errorf("decompress failed: %+v", err)
	}
	if uint64(n) != sdi.UncompressedDataLen {
		return fmt.Errorf("decompress failed: decompressed data len %d != expected %d", n,
			sdi.UncompressedDataLen)
	}
	ts.SDIs = append(ts.SDIs, sdi)
	return nil
}

func (ts *TableSpace) DumpSDIs() (err error) {
	err = ts.DumpAllRecsInLeafLevel()
	if err != nil {
		return err
	}
	results := make([][]byte, len(ts.SDIs)+1)
	results[0] = []byte(`["ibd2sdi"`)
	for n, sdi := range ts.SDIs {
		result := sdi.DumpJson()
		results[n+1] = result
		ts.SDIResult = append(ts.SDIResult, result...)
		err = sdi.DumpDDL()
		if err != nil {
			return err
		}
	}
	ts.SDIResult = bytes.Join(results, []byte(","))
	ts.SDIResult = append(ts.SDIResult, []byte("]")...)
	return nil
}

func (ts *TableSpace) DumpDDLs() (err error) {
	err = ts.DumpAllRecsInLeafLevel()
	if err != nil {
		return err
	}
	ts.DDLs = make([]string, 0)
	for _, sdi := range ts.SDIs {
		err = sdi.DumpDDL()
		if err != nil {
			return err
		}
		ts.DDLs = append(ts.DDLs, sdi.DDL)
	}
	return nil
}

func (ts *TableSpace) DumpAllRecsInLeafLevel() (err error) {
	if len(ts.SDIs) != 0 {
		return nil
	}
	err = ts.FetchSDIPages()
	if err != nil {
		return err
	}

	ts.CurPage = ts.LeafSDIPage
	curRecOffset, err := ts.GetFirstUserRec()
	if err != nil {
		return err
	}
	for {
		recType := ts.CurPage.GetRecType(uint32(curRecOffset))
		if recType == REC_STATUS_SUPREMUM {
			break
		}
		err = ts.ParseFieldsInRec(curRecOffset)
		if err != nil {
			return err
		}
		curRecOffset, err = ts.GetNextRecOffset(curRecOffset)
		if err != nil {
			return err
		}
		if curRecOffset == 0 {
			break
		}
	}
	if len(ts.SDIs) == 0 {
		return fmt.Errorf("no SDI found")
	}
	return nil
}
