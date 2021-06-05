package db

import (
	"context"
	_ "github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/kutty-kumar/charminder/pkg"
)

type BaseNoSQLRepo interface {
	BaseRepository
	ExactSearch(ctx context.Context, key string, value interface{}) (error, []pkg.Base)
	RangeSearch(ctx context.Context, key string, start, end interface{}) (error, []pkg.Base)
	TextSearch(ctx context.Context, value string) (error, []pkg.Base)
	IndexMappings(ctx context.Context) error
}
