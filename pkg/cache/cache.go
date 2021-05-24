package cache

import (
	"github.com/kutty-kumar/charminder/pkg/db"
	"time"
)

type Cache interface {
	Put(base db.Base) error
	Get(externalId string) (db.Base, error)
	MultiGet(externalIds []string) ([]db.Base, error)
	Delete(externalId string) error
	MultiDelete(externalIds []string) error
	PutWithTtl(base db.Base, duration time.Duration) error
	DeleteAll() error
	Health() error
}
