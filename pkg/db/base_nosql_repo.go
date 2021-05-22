package db

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	_ "github.com/elastic/go-elasticsearch/v7/esapi"
	"gorm.io/gorm"
	"strconv"
)

type BaseNoSQLRepo interface {
	BaseRepository
	ExactSearch(ctx context.Context, key, value string) (error, []Base)
	TextSearch(ctx context.Context, value string) (error, []Base)
	IndexMappings(ctx context.Context, base Base) error
}
type ElasticsearchRepo struct {
	client        *elasticsearch.Client
	entityCreator EntityCreator
}

func NewElasticsearchRepo(config *elasticsearch.Config, creator EntityCreator) (error, *ElasticsearchRepo) {
	client, err := elasticsearch.NewClient(*config)
	if err != nil {
		return err, nil
	}
	return nil, &ElasticsearchRepo{
		client:        client,
		entityCreator: creator,
	}
}
func (e *ElasticsearchRepo) Get(ctx context.Context, key, value string) (error, []Base) {
	err, entities := e.ExactSearch(ctx, key, value)
	if err != nil {
		return err, nil
	}
	return nil, entities
}
func (e *ElasticsearchRepo) GetSingle(ctx context.Context, key, value string) (error, Base) {
	err, entities := e.Get(ctx, key, value)
	if err != nil {
		return err, nil
	}
	return nil, entities[0]
}
func (e *ElasticsearchRepo) GetById(ctx context.Context, id uint64) (error, Base) {
	return e.GetSingle(ctx, "id", strconv.FormatUint(id, 64))
}
func (e *ElasticsearchRepo) GetByExternalId(ctx context.Context, externalId string) (error, Base) {
	return e.GetSingle(ctx, "external_id", externalId)
}
func (e *ElasticsearchRepo) MultiGetByExternalId(ctx context.Context, externalIds []string) (error, []Base) {
	entity := e.entityCreator()
	payload, err := json.Marshal(map[string][]string{"ids": externalIds})
	if err != nil {
		return err, nil
	}
	resp, err := esapi.MgetRequest{
		Index: string(entity.GetName()),
		Body:  bytes.NewBuffer(payload),
	}.Do(ctx, e.client)
	fmt.Println(resp.Body)
	if err != nil {
		return err, nil
	}
	return nil, nil
}
func (e *ElasticsearchRepo) Create(ctx context.Context, base Base) (error, Base) {
	panic("implement me")
}
func (e *ElasticsearchRepo) Update(ctx context.Context, externalId string, updatedBase Base) (error, Base) {
	panic("implement me")
}
func (e *ElasticsearchRepo) Search(ctx context.Context, params map[string]string) (error, []Base) {
	panic("implement me")
}
func (e *ElasticsearchRepo) GetDb() *gorm.DB {
	panic("implement me")
}
func (e *ElasticsearchRepo) Healthy() bool {
	_, err := e.client.Info()
	return err == nil
}
func (e *ElasticsearchRepo) ExactSearch(ctx context.Context, key, value string) (error, []Base) {
	panic("implement me")
}
func (e *ElasticsearchRepo) TextSearch(ctx context.Context, value string) (error, []Base) {
	panic("implement me")
}
func (e *ElasticsearchRepo) IndexMappings(ctx context.Context, base Base) error {
	panic("implement me")
}
