package ibd2schema

import "fmt"

/*
* Page size descriptor. Contains the physical and logical page size, as well
as whether the page is compressed or not.
*/
type PageSize struct {
	Physical      uint32
	PhysicalShift uint32
	Logical       uint32
	LogicalShift  uint32
	SSize         uint32
	IsCompressed  bool
	IsCompact     bool
}

func NewPageSize(physical, logical uint32, isCompressed bool) (ps *PageSize, err error) {
	ps = &PageSize{
		Physical:     physical,
		Logical:      logical,
		IsCompressed: isCompressed,
	}
	SetUnivPageSize(ps.Logical)
	err = ps.GetShift()
	return ps, err
}

func NewPageSizeWithFlag(fsp_flags uint32) (ps *PageSize, err error) {
	ps = &PageSize{}
	ssize := FspFlagsGetPageSsize(fsp_flags)
	/* If the logical page size is zero in fsp_flags, then use the
	   legacy 16k page size. */
	if ssize == 0 {
		ssize = UNIV_PAGE_SSIZE_ORIG
	}

	/* Convert from a 'log2 minus 9' to a page size in bytes. */
	ps.Logical = (UNIV_ZIP_SIZE_MIN >> 1) << ssize
	SetUnivPageSize(ps.Logical)

	ssize = FspFlagsGetZipSsize(fsp_flags)

	/* If the fsp_flags have zero in the zip_ssize field, then
	   it means that the tablespace does not have compressed pages
	   and the physical page size is the same as the logical page
	   size. */
	if ssize == 0 {
		ps.IsCompressed = false
		ps.Physical = ps.Logical
		err = ps.GetShift()
		return ps, err
	}
	ps.IsCompressed = true
	/* Convert from a 'log2 minus 9' to a page size
	   in bytes. */
	ps.Physical = (UNIV_ZIP_SIZE_MIN >> 1) << ssize
	err = ps.GetShift()
	return ps, err
}

func (ps *PageSize) GetShift() (err error) {
	var min, max uint32
	if ps.IsCompressed {
		min = UNIV_ZIP_SIZE_SHIFT_MIN
		max = UNIV_ZIP_SIZE_SHIFT_MAX
	} else {
		min = UNIV_PAGE_SIZE_SHIFT_MIN
		max = UNIV_PAGE_SIZE_SHIFT_MAX
	}
	for n := UNIV_PAGE_SIZE_SHIFT_MIN; n <= UNIV_PAGE_SIZE_SHIFT_MAX; n++ {
		if ps.Logical == (1 << n) {
			ps.LogicalShift = uint32(n)
			break
		}
	}
	if ps.LogicalShift == 0 {
		return fmt.Errorf("invalid logical page size(%d) shift, not between %d and %d",
			ps.Logical, UNIV_PAGE_SIZE_SHIFT_MIN, UNIV_PAGE_SIZE_SHIFT_MAX)
	}

	for n := min; n <= max; n++ {
		if ps.Physical == (1 << n) {
			ps.PhysicalShift = uint32(n)
			break
		}
	}
	if ps.PhysicalShift == 0 {
		return fmt.Errorf("invalid physical page size(%d) shift, not between %d and %d",
			ps.Physical, min, max)
	}
	if ps.IsCompressed {
		ps.SSize = ps.PhysicalShift - UNIV_ZIP_SIZE_SHIFT_MIN + 1
	}
	return nil
}
