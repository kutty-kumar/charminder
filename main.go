package main

import (
	"charminder/pkg/db"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"io"
	"strings"
	"time"
)

type Product struct {
	BasePrice          float64   `json:"base_price"`
	DiscountPercentage int       `json:"discount_percentage"`
	Quantity           int       `json:"quantity"`
	Manufacturer       string    `json:"manufacturer"`
	TaxAmount          int       `json:"tax_amount"`
	ProductID          int       `json:"product_id"`
	Category           string    `json:"category"`
	Sku                string    `json:"sku"`
	TaxlessPrice       float64   `json:"taxless_price"`
	UnitDiscountAmount int       `json:"unit_discount_amount"`
	MinPrice           float64   `json:"min_price"`
	ID                 string    `json:"_id"`
	DiscountAmount     int       `json:"discount_amount"`
	CreatedOn          time.Time `json:"created_on"`
	ProductName        string    `json:"product_name"`
	Price              float64   `json:"price"`
	TaxfulPrice        float64   `json:"taxful_price"`
	BaseUnitPrice      float64   `json:"base_unit_price"`
}

type KibanaSampleDataEcommerce struct {
	Category          []string  `json:"category"`
	CustomerFirstName string    `json:"customer_first_name"`
	CustomerFullName  string    `json:"customer_full_name"`
	CustomerGender    string    `json:"customer_gender"`
	CustomerId        uint      `json:"customer_id"`
	CustomerLastName  string    `json:"customer_last_name"`
	CustomerPhone     string    `json:"customer_phone"`
	DayOfWeek         string    `json:"day_of_week"`
	DayOfWeekI        uint      `json:"day_of_week_i"`
	Email             string    `json:"email"`
	Manufacturer      []string  `json:"manufacturer"`
	OrderDate         time.Time `json:"order_date"`
	OrderId           uint      `json:"order_id"`
	Products          []Product `json:"products"`
}

func (k KibanaSampleDataEcommerce) GetExternalId() string {
	return ""
}

func (k KibanaSampleDataEcommerce) GetName() db.DomainName {
	panic("implement me")
}

func (k KibanaSampleDataEcommerce) GetId() uint64 {
	panic("implement me")
}

func (k KibanaSampleDataEcommerce) GetStatus() db.Status {
	panic("implement me")
}

func (k KibanaSampleDataEcommerce) GetCreatedAt() time.Time {
	panic("implement me")
}

func (k KibanaSampleDataEcommerce) GetUpdatedAt() time.Time {
	panic("implement me")
}

func (k KibanaSampleDataEcommerce) GetDeletedAt() time.Time {
	panic("implement me")
}

func (k KibanaSampleDataEcommerce) ToDto() interface{} {
	panic("implement me")
}

func (k KibanaSampleDataEcommerce) FillProperties(dto interface{}) db.Base {
	panic("implement me")
}

func (k KibanaSampleDataEcommerce) Merge(other interface{}) {
	panic("implement me")
}

func (k KibanaSampleDataEcommerce) FromSqlRow(rows *sql.Rows) (db.Base, error) {
	panic("implement me")
}

func (k KibanaSampleDataEcommerce) SetExternalId(externalId string) {
	panic("implement me")
}

func (k KibanaSampleDataEcommerce) MarshalBinary() ([]byte, error) {
	panic("implement me")
}

func (k KibanaSampleDataEcommerce) ToJson() (string, error) {
	panic("implement me")
}

func (k KibanaSampleDataEcommerce) String() string {
	panic("implement me")
}

func (k KibanaSampleDataEcommerce) UnmarshalBinary(buffer []byte) error {
	panic("implement me")
}

type ESHealth struct {
	ClusterName                 string  `json:"cluster_name"`
	Status                      string  `json:"status"`
	TimedOut                    bool    `json:"timed_out"`
	NumberOfNodes               uint    `json:"number_of_nodes"`
	ActivePrimaryShards         uint    `json:"active_primary_shards"`
	ActiveShards                uint    `json:"active_shards"`
	InitializingShards          uint    `json:"initializing_shards"`
	UnAssignedShards            uint    `json:"unassigned_shards"`
	DelayedUnassignedShards     uint    `json:"delayed_unassigned_shards"`
	NumberOfPendingTasks        uint    `json:"number_of_pending_tasks"`
	NumberOfInFlightFetch       uint    `json:"number_of_in_flight_fetch"`
	TaskMaxWaitingInQueueMillis uint    `json:"task_max_waiting_in_queue_millis"`
	ActiveShardsPercentAsNumber float32 `json:"active_shards_percent_as_number"`
}

type Shards struct {
	Total      uint `json:"total"`
	Successful uint `json:"successful"`
	Skipped    uint `json:"skipped"`
	Failed     uint `json:"failed"`
}

type Total struct {
	Value    uint   `json:"value"`
	Relation string `json:"relation"`
}

type EsHit struct {
	Index  string                 `json:"_index"`
	Type   string                 `json:"_type"`
	Id     string                 `json:"_id"`
	Score  float64                `json:"_score"`
	Source map[string]interface{} `json:"_source"`
}

type Hits struct {
	Total    Total   `json:"total"`
	MaxScore float64 `json:"max_score"`
	Hits     []EsHit `json:"hits"`
}

type ESSearchResponse struct {
	Took     uint   `json:"took"`
	TimedOut bool   `json:"timed_out"`
	Shards   Shards `json:"_shards"`
	Hits     Hits   `json:"hits"`
}

type ESQuery struct {
	AttrName string
	Value    interface{}
}

func (eq *ESQuery) Term() string {
	return ""
}

type HttpStatusChecker struct {
}

func (hsc *HttpStatusChecker) IsSuccessFul(statusCode int) bool {
	return statusCode/100 == 2
}

func (hsc *HttpStatusChecker) IsInternalError(statusCode int) bool {
	return statusCode/100 == 5
}

func (hsc *HttpStatusChecker) IsClientError(statusCode int) bool {
	return statusCode/100 == 4
}

type HttpBodyUtil struct {
}

func (hbu *HttpBodyUtil) BytesToResponse(resBody io.ReadCloser, factory func() interface{}) error {
	if err := json.NewDecoder(resBody).Decode(factory()); err != nil {
		return err
	}
	return nil
}

func (hbu *HttpBodyUtil) ResponseToString(factory func() interface{}) (string, error) {
	rBytes, err := json.Marshal(factory())
	if err != nil {
		return "", err
	}
	return string(rBytes), nil
}

type ElasticsearchRepo struct {
	marshaller      *HttpBodyUtil
	client          *elasticsearch.Client
	sChecker        *HttpStatusChecker
	entityCreator   db.EntityCreator
	index           string
	entityConverter func(from map[string]interface{}) db.Base
}

func (esr *ElasticsearchRepo) GetById(ctx context.Context, id uint64) (error, db.Base) {
	panic("implement me")
}

func (esr *ElasticsearchRepo) Search(ctx context.Context, params map[string]string) (error, []db.Base) {
	panic("implement me")
}

func (esr *ElasticsearchRepo) GetDb() interface{} {
	return esr.client
}

func (esr *ElasticsearchRepo) ExactSearch(ctx context.Context, key, value string) (error, []db.Base) {
	panic("implement me")
}

func (esr *ElasticsearchRepo) TextSearch(ctx context.Context, value string) (error, []db.Base) {
	panic("implement me")
}

func (esr *ElasticsearchRepo) IndexMappings(ctx context.Context, base db.Base) error {
	panic("implement me")
}

func (esh *ESHealth) IsHealthy() bool {
	return (esh.Status == "yellow" || esh.Status == "green") && esh.ActiveShardsPercentAsNumber >= 50.00
}

func (esr *ElasticsearchRepo) IsHealthy() bool {
	info, err := esr.client.Cluster.Health()
	if err != nil {
		return false
	}
	var esResponse ESHealth
	err = esr.marshaller.BytesToResponse(info.Body, func() interface{} {
		return &esResponse
	})
	if err != nil {
		return false
	}
	return esResponse.IsHealthy()
}

func (esr *ElasticsearchRepo) Create(ctx context.Context, base db.Base) (error, db.Base) {
	jBody, err := base.ToJson()
	if err != nil {
		return err, nil
	}
	req := esapi.IndexRequest{
		Index:      string(base.GetName()),
		DocumentID: base.GetExternalId(),
		Body:       strings.NewReader(jBody),
		Refresh:    "true",
	}
	res, err := req.Do(ctx, esr.client)
	if err != nil || esr.sChecker.IsInternalError(res.StatusCode) || esr.sChecker.IsClientError(res.StatusCode) {
		return errors.New(fmt.Sprintf("Error while indexing %v", err)), nil
	}

	return nil, base
}

func (esr *ElasticsearchRepo) Update(ctx context.Context, entityId string, base db.Base) (error, db.Base) {
	req := esapi.UpdateRequest{DocumentID: entityId, Index: base.GetExternalId()}
	res, err := req.Do(ctx, esr.client)
	if err != nil || esr.sChecker.IsInternalError(res.StatusCode) || esr.sChecker.IsClientError(res.StatusCode) {
		return errors.New(fmt.Sprintf("Error while updating %v", err)), nil
	}
	return nil, base
}

func (esr *ElasticsearchRepo) GetByExternalId(ctx context.Context, entityId string) (error, db.Base) {
	truthy := true
	req := esapi.GetRequest{DocumentID: entityId, Refresh: &truthy, Realtime: &truthy}
	res, err := req.Do(ctx, esr.client)
	if err != nil {
		return err, nil
	}
	entity := esr.entityCreator()
	err = esr.marshaller.BytesToResponse(res.Body, func() interface{} {
		return &entity
	})
	if err != nil {
		return err, nil
	}
	return nil, entity
}

func (esr *ElasticsearchRepo) MultiGetByExternalId(ctx context.Context, entityIds []string) (error, []db.Base) {
	var nEntityIds []string
	for _, entityId := range entityIds {
		nEntityIds = append(nEntityIds, fmt.Sprintf("\"%v\"", entityId))
	}
	req := esapi.SearchRequest{
		Index: []string{esr.index},
		Body:  strings.NewReader(fmt.Sprintf("{\"query\":{\"terms\":{\"_id\":[%v]}}}", strings.Join(nEntityIds, ","))),
	}
	res, err := req.Do(ctx, esr.client)
	if err != nil || !esr.sChecker.IsSuccessFul(res.StatusCode) {
		return err, nil
	}
	var response ESSearchResponse
	err = esr.marshaller.BytesToResponse(res.Body, func() interface{} {
		return &response
	})
	if err != nil {
		return err, nil
	}
	var result []db.Base
	for _, hit := range response.Hits.Hits {
		result = append(result, esr.entityConverter(hit.Source))
	}
	return nil, result
}

func main() {
	ctx := context.Background()
	client, err := elasticsearch.NewDefaultClient()
	if err != nil {
		panic("failed initializing elasticsearch client")
	}
	repo := ElasticsearchRepo{
		marshaller: &HttpBodyUtil{},
		client:     client,
		sChecker:   &HttpStatusChecker{},
		index:      "kibana_sample_data_ecommerce",
		entityConverter: func(from map[string]interface{}) db.Base {
			jsonBody, _ := json.Marshal(from)
			order := KibanaSampleDataEcommerce{}
			json.Unmarshal(jsonBody, &order)
			return &order
		},
		entityCreator: func() db.Base {
			return &KibanaSampleDataEcommerce{}
		},
	}
	fmt.Println(repo.IsHealthy())
	err, orders := repo.MultiGetByExternalId(ctx, []string{"a6sCcHkBnezkLGHEXzVg", "aqsCcHkBnezkLGHEXzVg"})
	if err != nil {
		panic("search failed")
	}
	fmt.Println(len(orders))
}
