package pager

import (
	"bytes"
	"errors"

	"github.com/joeandaverde/tinydb/internal/storage"
)

type BTreeTable struct {
	rootPage int
	pager    Pager
}

func NewBTreeTable(rootPage int, p Pager) *BTreeTable {
	return &BTreeTable{
		rootPage: rootPage,
		pager:    p,
	}
}

func (b *BTreeTable) Insert(r *storage.Record) error {
	buf := bytes.Buffer{}
	if err := r.Write(&buf); err != nil {
		return err
	}
	recordBytes := buf.Bytes()

	// Load the table root page
	//
	// 读取根页
	root, err := b.pager.Read(b.rootPage)
	if err != nil {
		return err
	}

	// 检查节点类型

	// 如果 root 是页节点
	if root.header.Type == PageTypeLeaf {
		// 如果 root 不足以容纳新数据
		if !root.Fits(len(recordBytes)) {
			// 分裂页，得到 父、左、右 页
			parent, left, right, err := splitPage(b.pager, root)
			if err != nil {
				return err
			}
			// Write the record to the right page
			// 把新数据写入到右页
			right.AddCell(recordBytes)
			// Write all pages to disk
			// 把所有页刷盘
			return b.pager.Write(left, right, parent)
		}

		// Write the record to the leaf page
		// 如果 root 可以容纳新数据，就直接插入到 root 中。
		root.AddCell(recordBytes)

		// Save the page
		// 刷盘
		return b.pager.Write(root)

	// 如果 root 非页节点
	} else if root.header.Type == PageTypeInternal {

		// TODO: For now, just write to the right most page.
		//
		destPage, err := b.pager.Read(root.header.RightPage)
		if err != nil {
			return err
		}

		// If the rightmost page is full, create a new page and update the pointer.
		// 如果最右节点不足以容纳新数据，需要分裂
		if !destPage.Fits(len(recordBytes)) {

			// 获取 destPage 内最大 RowID
			maxRowID, err := maxRowID(destPage)
			if err != nil {
				return err
			}

			//
			internalNode := storage.InteriorNode{
				LeftChild: uint32(destPage.Number()),
				Key:       maxRowID,
			}

			/// 现在实现比较原始 ，内部节点的分裂还不支持
			if !root.Fits(storage.InteriorNodeSize) {
				return errors.New("not yet supporting adding another internal node")
			}

			// Allocate a new page, update internal node right pointer.
			destPage, err = b.pager.Allocate(PageTypeLeaf) //Leaf
			if err != nil {
				return err
			}
			root.header.RightPage = destPage.Number()

			// Add link to the newly added page.
			interiorCell, err := internalNode.ToBytes()
			if err != nil {
				return err
			}
			root.AddCell(interiorCell)

			// Write the record
			destPage.AddCell(recordBytes)
			return b.pager.Write(root, destPage)
		}

		// Write the record
		destPage.AddCell(recordBytes)
		return b.pager.Write(destPage)
	} else {
		return errors.New("unsupported page type")
	}
}

func splitPage(pager Pager, p *MemPage) (*MemPage, *MemPage, *MemPage, error) {
	// New page for the left node
	// 创建一个 左 叶节点
	leftPage, err := pager.Allocate(PageTypeLeaf)
	if err != nil {
		return nil, nil, nil, err
	}

	// New page for the right node
	// 创建一个 右 叶节点
	rightPage, err := pager.Allocate(PageTypeLeaf)
	if err != nil {
		return nil, nil, nil, err
	}

	// Max key
	//
	maxRowID, err := maxRowID(p)
	if err != nil {
		return nil, nil, nil, err
	}

	// Copy the page to the left
	// 把当前页 p 的数据拷贝到 左 叶节点
	p.CopyTo(leftPage)

	// Update the header to make the page an interior node
	// 把当前页 p 设置为内部节点，作为左、右页节点的父节点
	newHeader := NewPageHeader(PageTypeInternal, len(p.data))
	newHeader.RightPage = rightPage.Number()
	p.SetHeader(newHeader)

	// Add a cell to the new internal page
	// 创建一个 Cell 添加到 p 上
	cell := storage.InteriorNode{
		LeftChild: uint32(leftPage.Number()),	// 左子页节点
		Key:       maxRowID,					// 最大 RowId
	}
	cellBytes, err := cell.ToBytes()
	if err != nil {
		return nil, nil, nil, err
	}
	p.AddCell(cellBytes)

	// 返回
	return p, leftPage, rightPage, nil
}

// 获取 Page *p 内最大 RowID
func maxRowID(p *MemPage) (uint32, error) {
	// 构造迭代器
	recordIter := newRecordIter(p)
	maxRowID := uint32(0)

	// 不断读取 Page *p 内的 records
	for recordIter.Next() {
		// 错误检查
		if recordIter.Error() != nil {
			return 0, recordIter.Error()
		}
		// 取当前 record
		record := recordIter.Current()
		// 更新 maxRowID
		if record.RowID > maxRowID {
			maxRowID = record.RowID
		}
	}

	// 返回 Page *p 内最大 RowID
	return maxRowID, nil
}


// 用于遍历 Page 下的所有 Records
type recordIterator struct {
	idx      int
	record *storage.Record
	p      *MemPage
	err    error
}

func newRecordIter(p *MemPage) *recordIterator {
	return &recordIterator{
		p: p,
		idx: 0,
	}
}

func (i *recordIterator) Next() bool {
	if i.idx < i.p.CellCount() {
		// 读取第 i.idx 个 record ，保存 record 和 err 。
		i.record, i.err = i.p.ReadRecord(i.idx)
		// 递增游标
		i.idx++
		return true
	}
	return false
}

// Error 获取当前 err
func (i *recordIterator) Error() error {
	return i.err
}

// Current 获取当前 record
func (i *recordIterator) Current() *storage.Record {
	return i.record
}
