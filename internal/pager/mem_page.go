package pager

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/joeandaverde/tinydb/internal/storage"
)

// NewPageHeader creates a new PageHeader
func NewPageHeader(pageType PageType, pageSize int) PageHeader {
	return PageHeader{
		Type:                pageType,
		CellsOffset:         uint16(pageSize),
		FreeBlock:           0,
		NumCells:            0,
		FragmentedFreeBytes: 0,
		RightPage:           0,
	}
}

// InteriorHeaderLen is the length of an interior btree node
const InteriorHeaderLen = 12

// LeafHeaderLen is the length of a btree leaf node
const LeafHeaderLen = 8

// PageType type of page. See associated enumeration values.
type PageType byte

const (
	// PageTypeInternal internal table page
	// 内部页
	PageTypeInternal PageType = 0x05

	// PageTypeLeaf leaf table page
	// 叶子页
	PageTypeLeaf PageType = 0x0D

	// PageTypeInternalIndex internal index page
	// 内部索引页
	PageTypeInternalIndex PageType = 0x02

	// PageTypeLeafIndex leaf index page
	PageTypeLeafIndex PageType = 0x0A
)

// PageHeader contains metadata about the page
// BTree Page
// The 100-byte database file header (found on page 1 only)
// The 8 or 12 byte b-tree page header
// The cell pointer array
// Unallocated space
// The cell content area
// The reserved region.
//      The size of the reserved region is determined by the
//      one-byte unsigned integer found at an offset of 20 into
//      the database file header. The size of the reserved region is usually zero.
// Example First page header
// 0D (00 00) (00 01) (0F 8A) (00)
//
//
//
type PageHeader struct {
	// Type is the PageType for the page
	// 页类型
	Type PageType

	// FreeBlock The two-byte integer at offset 1 gives the start of the
	// first freeblock on the page, or is zero if there are no freeblocks.
	// A freeblock is a structure used to identify unallocated space within a b-tree page.
	//
	// Freeblocks are organized as a chain.
	// The first 2 bytes of a freeblock are a big-endian integer which is the offset in the
	// b-tree page of the next freeblock in the chain, or zero if the freeblock is the last
	// on the chain.
	//
	//
	//
	FreeBlock uint16

	// NumCells is the number of cells stored in this page.
	// Cell 总数
	NumCells uint16

	// CellsOffset the start of the cell content area.
	// A zero value for this integer is interpreted as 65536.
	// If the page contains no cells, this field contains the value PageSize.
	//
	// 注意，Cell 的 offset 是从前向后写，Cell 的内容是从后向前写。
	// 所以 CellsOffset 是从大向小变化的。
	CellsOffset uint16

	// FragmentedFreeBytes the number of fragmented free bytes within the cell content area.
	FragmentedFreeBytes byte

	// RightPage internal nodes only
	RightPage int
}


// MemPage represents a raw table page
type MemPage struct {
	header     PageHeader	// 页头
	pageNumber int			// 页标号
	data       []byte		// 数据
	dirty      bool			// 是否脏页
}

// Number is the page number
func (p *MemPage) Number() int {
	return p.pageNumber
}

// WriteTo writes the page to the specified writer
func (p *MemPage) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(p.data)
	return int64(n), err
}

// SetHeader sets the page header and marks the page as dirty.
func (p *MemPage) SetHeader(h PageHeader) {
	p.dirty = true
	p.header = h
	p.updateHeaderData()
}

// CopyTo copies the page data to dst and marks dst as dirty.
func (p *MemPage) CopyTo(dst *MemPage) {
	dst.dirty = true
	dst.header = p.header
	copy(dst.data, p.data)
}

// Fits determines if there's enough space in the page for a cell
// of the specified size.
func (p *MemPage) Fits(recordLen int) bool {
	// Where the cell pointer will be stored
	cellPointerOffset := cellPointersStart(p.header.Type, p.pageNumber) + int(p.header.NumCells)*2

	// Where cell data would start
	cellDataOffset := int(p.header.CellsOffset) - recordLen

	return cellPointerOffset+2 <= cellDataOffset
}

// CellCount the total number of cells in this page
func (p *MemPage) CellCount() int {
	return int(p.header.NumCells)
}

// ReadRecord returns a slice of bytes of the requested cell.
func (p *MemPage) ReadRecord(cellIndex int) (*storage.Record, error) {
	cellDataStart := p.cellDataOffset(cellIndex)

	// TODO: Should this be capped upper and lower bound?
	reader := bytes.NewReader(p.data[cellDataStart:])
	return storage.ReadRecord(reader)
}

// ReadInteriorNode returns a slice of bytes of the requested cell.
func (p *MemPage) ReadInteriorNode(cellIndex int) (*storage.InteriorNode, error) {
	cellDataStart := p.cellDataOffset(cellIndex)

	// TODO: Should this be capped upper and lower bound?
	return storage.ReadInteriorNode(p.data[cellDataStart:])
}

// AddCell adds a cell entry to the page.
// This function assumes that the page can fit the new cell.
//
//
func (p *MemPage) AddCell(data []byte) {
	// 置为脏页
	p.dirty = true

	// Every cell is 2 bytes
	// 每个 Cell 的 Offset 占 2B ，当前 Page 内有 NumCells 个 cell ，那么前 2*NumCells 个 Bytes 需要跳过。
	//
	// Cell 的 Offset 是从小偏移向大偏移写入的，所以当前的 Cell 的 Offset 写入到 cellPointerOffset 位置。
	cellPointerOffset := cellPointersStart(p.header.Type, p.pageNumber) + int(2*p.header.NumCells)

	// 当前 Cell 占用字节数
	cellLength := uint16(len(data))
	// Cell 的 Data 是从大偏移向小偏移量写入的，所以当前 Cell 的写入偏移是 p.header.CellsOffset - cellLength 位置。
	cellOffset := p.header.CellsOffset - cellLength

	// Write a pointer to the new cell
	// 写入 Cell Offset
	binary.BigEndian.PutUint16(p.data[cellPointerOffset:], cellOffset)

	// Write the data to the cell pointer
	// 写入 Cell Data
	copy(p.data[cellOffset:], data)

	// Update cells offset for the next page
	// 更新 Cell 写入偏移
	p.header.CellsOffset = cellOffset

	// Update number of cells in this page
	// 更新 Cell 数目 +1
	p.header.NumCells = p.header.NumCells + 1

	// Update the header
	// 更新 Page 页头
	p.updateHeaderData()
}

// 更新页头
func (p *MemPage) updateHeaderData() {
	headerOffset := headerOffset(p.pageNumber)
	header := p.data[headerOffset:]
	header[0] = byte(p.header.Type)									// 页类型
	binary.BigEndian.PutUint16(header[1:3], p.header.FreeBlock)		//
	binary.BigEndian.PutUint16(header[3:5], p.header.NumCells)		//
	binary.BigEndian.PutUint16(header[5:7], p.header.CellsOffset)	//
	header[7] = p.header.FragmentedFreeBytes						//
	//
	if p.header.Type == PageTypeInternal || p.header.Type == PageTypeInternalIndex {
		binary.BigEndian.PutUint32(header[8:12], uint32(p.header.RightPage))
	}
}

func (p *MemPage) cellDataOffset(cellIndex int) int {
	// Every cell is 2 bytes
	cellPointerOffset := cellPointersStart(p.header.Type, p.pageNumber) + 2*cellIndex

	// Read 2 bytes at the pointer
	var cellDataOffset uint16
	reader := bytes.NewReader(p.data[cellPointerOffset : cellPointerOffset+2])
	binary.Read(reader, binary.BigEndian, &cellDataOffset)

	// This is where the cell data starts
	return int(cellDataOffset)
}

func cellPointersStart(pageType PageType, pageNumber int) int {
	if pageType == PageTypeInternal || pageType == PageTypeInternalIndex {
		return headerOffset(pageNumber) + InteriorHeaderLen
	}
	return headerOffset(pageNumber) + LeafHeaderLen
}

func headerOffset(pageNumber int) int {
	if pageNumber == 1 {
		return 100
	}
	return 0
}

// FromBytes parses a byte slice to a MemPage and takes ownership of the slice.
func FromBytes(pageNumber int, data []byte) (*MemPage, error) {
	offset := headerOffset(pageNumber)

	view := data[offset:]

	header := PageHeader{
		Type:                PageType(view[0]),
		FreeBlock:           binary.BigEndian.Uint16(view[1:3]),
		NumCells:            binary.BigEndian.Uint16(view[3:5]),
		CellsOffset:         binary.BigEndian.Uint16(view[5:7]),
		FragmentedFreeBytes: view[7],
		RightPage:           0,
	}

	if header.Type == PageTypeInternal || header.Type == PageTypeInternalIndex {
		header.RightPage = int(binary.BigEndian.Uint32(view[8:12]))
	}

	return &MemPage{
		header:     header,
		pageNumber: pageNumber,
		data:       data,
		dirty:      false,
	}, nil
}

func WriteRecord(p *MemPage, r *storage.Record) error {
	buf := bytes.Buffer{}
	if err := r.Write(&buf); err != nil {
		return err
	}

	recordBytes := buf.Bytes()
	p.AddCell(recordBytes)

	return nil
}
