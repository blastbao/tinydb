package backend

import (
	"errors"
	"path"
	"sync"
	"sync/atomic"

	"github.com/joeandaverde/tinydb/internal/pager"
	"github.com/joeandaverde/tinydb/internal/storage"
	"github.com/sirupsen/logrus"
)

// Config describes the configuration for the database
//
// 数据库配置
type Config struct {
	DataDir  string	// 数据目录
	PageSize int	// 页大小
}

// Engine holds metadata and indexes about the database
//
// 维护数据库元数据和索引
type Engine struct {
	sync.RWMutex
	log       logrus.FieldLogger
	config    Config
	wal       *storage.WAL	// WAL 日志
	pagerPool *pager.Pool	// 页缓存
	txID      uint32		// 事务 ID
}

// Start initializes a new TinyDb database engine
func Start(log logrus.FieldLogger, config Config) (*Engine, error) {
	log.Infof("Starting database engine [DataDir: %s]", config.DataDir)

	if config.PageSize < 1024 {
		return nil, errors.New("page size must be greater than or equal to 1024")
	}

	// 数据文件
	dbPath := path.Join(config.DataDir, "tiny.db")

	// Open the main database file
	//
	// 打开数据文件
	dbFile, err := storage.OpenDbFile(dbPath, config.PageSize)
	if err != nil {
		return nil, err
	}

	// Brand new database needs at least one page.
	//
	// 检查页数目，为空则初始化
	if dbFile.TotalPages() == 0 {
		if err := pager.Initialize(dbFile); err != nil {
			return nil, err
		}
	}

	// Initialize WAL
	//
	// 初始化 WAL
	wal, err := storage.OpenWAL(dbFile)
	if err != nil {
		return nil, err
	}

	return &Engine{
		config:    config,
		log:       log,
		wal:       wal,
		pagerPool: pager.NewPool(pager.NewPager(wal)),
	}, nil
}

// TxID provides a new transaction id
func (e *Engine) TxID() uint32 {
	// 事务 ID 自增
	return atomic.AddUint32(&e.txID, 1)
}

func (e *Engine) NewPager() pager.Pager {
	return pager.NewPager(e.wal)
}
