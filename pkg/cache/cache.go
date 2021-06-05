package cache

import (
	"github.com/kutty-kumar/charminder/pkg"
	"time"
)

type Cache interface {
	Put(base pkg.Base) error
	Get(externalId string) (pkg.Base, error)
	MultiGet(externalIds []string) ([]pkg.Base, error)
	Delete(externalId string) error
	MultiDelete(externalIds []string) error
	PutWithTtl(base pkg.Base, duration time.Duration) error
	DeleteAll() error
	Health() error
}
