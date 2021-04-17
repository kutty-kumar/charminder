package pkg

import (
	"context"
	"gorm.io/gorm"
)

type BaseRepository interface {
	GetById(ctx context.Context, id uint64) (error, Base)
	GetByExternalId(ctx context.Context, externalId string) (error, Base)
	MultiGetByExternalId(ctx context.Context, externalIds [] string) (error, []Base)
	Create(ctx context.Context, base Base) (error, Base)
	Update(ctx context.Context, externalId string, updatedBase Base) (error, Base)
	Search(ctx context.Context, params map[string]string) (error, []Base)
	GetDb() *gorm.DB
}

type BaseDao struct {
	BaseRepository
}

func NewBaseGORMDao(opts ...GORMRepositoryOption) BaseDao {
	return BaseDao{
		NewGORMRepository(opts...),
	}
}
