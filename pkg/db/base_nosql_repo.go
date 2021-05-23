package db

import (
	"context"
	_ "github.com/elastic/go-elasticsearch/v7/esapi"
)

type BaseNoSQLRepo interface {
	BaseRepository
	ExactSearch(ctx context.Context, key string, value interface{}) (error, []Base)
	RangeSearch(ctx context.Context, key string, start, end interface{}) (error, []Base)
	TextSearch(ctx context.Context, value string) (error, []Base)
	IndexMappings(ctx context.Context) error
}
