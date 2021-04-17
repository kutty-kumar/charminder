package charminder

type BaseSvc struct {
	Persistence BaseRepository
}

func (b *BaseSvc) Init(repo BaseRepository) {
	b.Persistence = repo
}

func (b *BaseSvc) FindById(id uint64) (error, Base) {
	return b.Persistence.GetById(id)
}

func (b *BaseSvc) FindByExternalId(id string) (error, Base) {
	return b.Persistence.GetByExternalId(id)
}

func (b *BaseSvc) MultiGetByExternalId(ids []string) (error, []Base) {
	return b.Persistence.MultiGetByExternalId(ids)
}

func (b *BaseSvc) Create(base Base) (error, Base) {
	return b.Persistence.Create(base)
}

func (b *BaseSvc) Update(id string, base Base) (error, Base) {
	return b.Persistence.Update(id, base)
}

func (b *BaseSvc) GetPersistence() BaseRepository {
	return b.Persistence
}

func NewBaseSvc(persistence BaseRepository) BaseSvc {
	return BaseSvc{
		Persistence: persistence,
	}
}
