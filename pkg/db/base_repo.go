package db

import (
	"context"
	"github.com/kutty-kumar/charminder/pkg"
)

type BaseRepository interface {
	GetById(ctx context.Context, id uint64) (error, pkg.Base)
	GetByExternalId(ctx context.Context, externalId string) (error, pkg.Base)
	MultiGetByExternalId(ctx context.Context, externalIds []string) (error, []pkg.Base)
	Create(ctx context.Context, base pkg.Base) (error, pkg.Base)
	Update(ctx context.Context, externalId string, updatedBase pkg.Base) (error, pkg.Base)
	Search(ctx context.Context, params map[string]string) (error, []pkg.Base)
	GetDb() interface{}
}

type BaseDao struct {
	BaseRepository
}

func NewBaseGORMDao(opts ...GORMRepositoryOption) BaseDao {
	return BaseDao{
		NewGORMRepository(opts...),
	}
}
