package pager

import (
	"fmt"
	"github.com/joeandaverde/tinydb/internal/storage"
)


// PageReader 读取页
type PageReader interface {
	Read(page int) (*MemPage, error)
}

// PageWriter 写入页/刷新页
type PageWriter interface {
	Write(pages ...*MemPage) error
	Allocate(PageType) (*MemPage, error)
	Flush() error
	Reset()
}

// Pager manages database paging
type Pager interface {
	PageReader
	PageWriter
}

type pager struct {
	pageCount int					// 页总数
	pageCache map[int]*MemPage		// 页缓存
	file storage.File				// 底层文件
}

func Initialize(file storage.File) error {
	newPage := &MemPage{
		header:     NewPageHeader(PageTypeLeaf, file.PageSize()),
		pageNumber: 1,
		data:       make([]byte, file.PageSize()),
	}
	newPage.updateHeaderData()

	return file.Write(storage.Page{PageNumber: 1, Data: newPage.data})
}

//
func NewPager(file storage.File) Pager {
	return &pager{
		pageCount: file.TotalPages(),
		pageCache: make(map[int]*MemPage),
		file:      file,
	}
}

// Read reads a full page from cache or the page source
//
// 从缓存或者文件中读取一个完整页并返回
func (p *pager) Read(pageNumber int) (*MemPage, error) {
	// 参数检查
	if pageNumber < 1 {
		return nil, fmt.Errorf("page [%d] out of bounds", pageNumber)
	}

	// 检查页是否在缓存中，若在直接返回
	if tablePage, ok := p.pageCache[pageNumber]; ok {
		return tablePage, nil
	}

	// Ensure the page hasn't been retrieved into the cache since releasing the read lock
	// 检查页是否在缓存中，若在直接返回
	if tablePage, ok := p.pageCache[pageNumber]; ok {
		return tablePage, nil
	}

	// Read raw page data from the source
	// 读取页
	data, err := p.file.Read(pageNumber)
	if err != nil {
		return nil, err
	}

	// Parse bytes to a page
	// 解析页
	page, err := FromBytes(pageNumber, data)
	if err != nil {
		return nil, err
	}

	// Cache the result for later reads
	// 缓存页
	p.pageCache[pageNumber] = page

	// 返回页
	return p.pageCache[pageNumber], nil
}

// Write updates pages in the pager
//
// 将页(列表)写入到缓存中
func (p *pager) Write(pages ...*MemPage) error {
	for _, pg := range pages {
		p.pageCache[pg.Number()] = pg
	}
	return nil
}

// Flush flushes all dirty pages to destination
//
//
func (p *pager) Flush() error {
	var dirtyPages []storage.Page
	var dirtyMemPages []*MemPage

	// 遍历所有缓存页，获取所有脏页
	for _, page := range p.pageCache {
		if !page.dirty {
			continue
		}
		dirtyPages = append(dirtyPages, storage.Page{PageNumber: page.pageNumber, Data: page.data})
		dirtyMemPages = append(dirtyMemPages, page)
	}

	// 将脏页刷盘
	if len(dirtyPages) > 0 {
		if err := p.file.Write(dirtyPages...); err != nil {
			return err
		}
		p.pageCount = p.file.TotalPages()
	}

	// 将脏页标记重置
	for _, p := range dirtyMemPages {
		p.dirty = false
	}

	return nil
}

// Reset clears all dirty pages
//
// 将脏页标记重置
func (p *pager) Reset() {
	p.pageCount = p.file.TotalPages()
	for k, page := range p.pageCache {
		if page.dirty {
			delete(p.pageCache, k)
		}
	}
}

// Allocate allocates a new dirty page in the pager.
//
// Page 1 of a database file is the root page of a table b-tree that
// holds a special table named "sqlite_master" (or "sqlite_temp_master" in
// the case of a TEMP database) which stores the complete database schema.
// The structure of the sqlite_master table is as if it had been
// created using the following SQL:
//
// CREATE TABLE sqlite_master(
//    type text,
//    name text,
//    tbl_name text,
//    rootpage integer,
//    sql text
// );
//
// 分配一个新页。
//
//
//
func (p *pager) Allocate(pageType PageType) (*MemPage, error) {
	// 更新页计数 +1
	p.pageCount = p.pageCount + 1

	// 创建内存页
	newPage := &MemPage{
		header:     NewPageHeader(pageType, p.file.PageSize()),	// 页头
		pageNumber: p.pageCount,								// 页号
		data:       make([]byte, p.file.PageSize()),			// 数据
		dirty:      true,										// 脏页标记
	}

	// 更新页头
	newPage.updateHeaderData()

	// 缓存页
	p.pageCache[p.pageCount] = newPage

	// 返回页
	return p.pageCache[p.pageCount], nil
}

var _ Pager = (*pager)(nil)
