package pkg

import (
	"context"
	"database/sql"
	"errors"
	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type GORMRepositoryOption func(repository *GORMRepository)

type GORMRepository struct {
	db               *gorm.DB
	creator          EntityCreator
	externalIdSetter ExternalIdSetter
	logger *logrus.Logger
}

func WithCreator(creator EntityCreator) GORMRepositoryOption {
	return func(r *GORMRepository)	{
		r.creator = creator
	}
}

func WithExternalIdSetter(setter ExternalIdSetter) GORMRepositoryOption {
	return func(r *GORMRepository){
		r.externalIdSetter = setter
	}
}

func WithLogger(logger *logrus.Logger) GORMRepositoryOption {
	return func(r *GORMRepository) {
		r.logger = logger
	}
}

func WithDb(db *gorm.DB) GORMRepositoryOption {
	return func(r *GORMRepository) {
		r.db = db
	}
}

func (r *GORMRepository) GetDb() *gorm.DB {
	return r.db
}

func NewGORMRepository(opts ...GORMRepositoryOption) *GORMRepository {
	repo := GORMRepository{}
	for _, opt := range opts {
		opt(&repo)
	}
	return &repo
}

func (r *GORMRepository) GetById(ctx context.Context, id uint64) (error, Base) {
	entity := r.creator()
	if err := r.db.WithContext(ctx).Where("id = ?", id).First(&entity).Error; err != nil {
		return err, nil
	}
	return nil, entity
}

func (r *GORMRepository) GetByExternalId(ctx context.Context, externalId string) (error, Base) {
	entity := r.creator()
	if err := r.db.WithContext(ctx).Table(string(entity.GetName())).Where("external_id = ?", externalId).First(entity).Error; err != nil {
		return err, nil
	}
	return nil, entity
}

func (r *GORMRepository) populateRows(rows *sql.Rows) (error, []Base) {
	var models []Base
	for rows.Next() {
		entity := r.creator()
		entity, err := entity.FromSqlRow(rows)
		if err != nil {
			return err, nil
		}
		models = append(models, entity)
	}
	return nil, models
}

func (r *GORMRepository) MultiGetByExternalId(ctx context.Context, externalIds [] string) (error, []Base) {
	entity := r.creator()
	rows, err := r.db.WithContext(ctx).Table(string(entity.GetName())).Where("external_id IN (?)", externalIds).Rows()
	if err != nil {
		return err, nil
	}
	return r.populateRows(rows)
}

func (r *GORMRepository) generateExternalId(base Base) (error, string) {
	if base.GetExternalId() == "" {
		uid := uuid.NewV4()
		return nil, uid.String()
	}
	return nil, base.GetExternalId()
}

func (r *GORMRepository) Create(ctx context.Context, base Base) (error, Base) {
	err, externalId := r.generateExternalId(base)
	if err != nil {
		return err, nil
	}
	r.externalIdSetter(externalId, base)
	if err := r.db.WithContext(ctx).Create(base).Error; err != nil {
		return err, nil
	}
	return nil, base
}

func (r *GORMRepository) Update(ctx context.Context, externalId string, updatedBase Base) (error, Base) {
	err, entity := r.GetByExternalId(ctx, externalId)
	if err != nil {
		return err, nil
	}
	entity.Merge(updatedBase)
	if err := r.db.WithContext(ctx).Table(string(entity.GetName())).Model(entity).Updates(entity).Error; err != nil {
		return err, nil
	}
	return nil, entity
}

func (r *GORMRepository) Search(ctx context.Context, params map[string]string) (error, []Base) {
	return errors.New("not implemented"), nil
}
