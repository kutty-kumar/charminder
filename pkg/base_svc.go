package pkg

import "context"

type BaseSvc struct {
	Persistence BaseRepository
}

func (b *BaseSvc) Init(repo BaseRepository) {
	b.Persistence = repo
}

func (b *BaseSvc) FindById(ctx context.Context, id uint64) (error, Base) {
	return b.Persistence.GetById(ctx, id)
}

func (b *BaseSvc) FindByExternalId(ctx context.Context, id string) (error, Base) {
	return b.Persistence.GetByExternalId(ctx, id)
}

func (b *BaseSvc) MultiGetByExternalId(ctx context.Context, ids []string) (error, []Base) {
	return b.Persistence.MultiGetByExternalId(ctx, ids)
}

func (b *BaseSvc) Create(ctx context.Context, base Base) (error, Base) {
	return b.Persistence.Create(ctx, base)
}

func (b *BaseSvc) Update(ctx context.Context, id string, base Base) (error, Base) {
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
