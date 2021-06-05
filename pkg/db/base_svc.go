package db

import (
	"context"
	"github.com/kutty-kumar/charminder/pkg"
)

type BaseSvc struct {
	Persistence BaseRepository
}

func (b *BaseSvc) Init(repo BaseRepository) {
	b.Persistence = repo
}

func (b *BaseSvc) FindById(ctx context.Context, id uint64) (error, pkg.Base) {
	return b.Persistence.GetById(ctx, id)
}

func (b *BaseSvc) FindByExternalId(ctx context.Context, id string) (error, pkg.Base) {
	return b.Persistence.GetByExternalId(ctx, id)
}

func (b *BaseSvc) MultiGetByExternalId(ctx context.Context, ids []string) (error, []pkg.Base) {
	return b.Persistence.MultiGetByExternalId(ctx, ids)
}

func (b *BaseSvc) Create(ctx context.Context, base pkg.Base) (error, pkg.Base) {
	return b.Persistence.Create(ctx, base)
}

func (b *BaseSvc) Update(ctx context.Context, id string, base pkg.Base) (error, pkg.Base) {
	return b.Persistence.Update(ctx, id, base)
}

func (b *BaseSvc) GetPersistence() BaseRepository {
	return b.Persistence
}

func NewBaseSvc(persistence BaseRepository) BaseSvc {
	return BaseSvc{
		Persistence: persistence,
	}
}
