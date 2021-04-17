package pkg

import (
	"bytes"
	"database/sql"
	"time"
)

const (
	active   = 0
	inactive = 1
)

type Status int

var statusMapping map[string]int
var statusReverseMapping map[int]string

func init() {
	statusMapping = make(map[string]int)
	statusReverseMapping = make(map[int]string)
	statusMapping["active"] = active
	statusMapping["inactive"] = inactive
	statusReverseMapping[active] = "active"
	statusReverseMapping[inactive] = "inactive"
}

func GetStatusInt(status string) int {
	return statusMapping[status]
}

func GetStatusStr(status int) string {
	return statusReverseMapping[status]
}

type EntityCreator func() Base

type DomainName string

type DomainFactory struct {
	entityMappings map[DomainName]EntityCreator
}

func (d *DomainFactory) RegisterMapping(domainName DomainName, creator EntityCreator) {
	d.entityMappings[domainName] = creator
}

func (d *DomainFactory) GetMapping(domainName DomainName) EntityCreator {
	return d.entityMappings[domainName]
}

func NewDomainFactory() *DomainFactory {
	return &DomainFactory{entityMappings: make(map[DomainName]EntityCreator)}
}

type Base interface {
	GetExternalId() string
	GetName() DomainName
	GetId() uint64
	GetStatus() Status
	GetCreatedAt() time.Time
	GetUpdatedAt() time.Time
	GetDeletedAt() time.Time
	ToDto() interface{}
	FillProperties(dto interface{}) Base
	Merge(other interface{})
	FromSqlRow(rows *sql.Rows) (Base, error)
	SetExternalId(externalId string)
	ToBytes() (*bytes.Buffer, error)
	ToJson() (string, error)
	String() string
}

type Attribute interface {
	GetKey() string
	GetValue() string
}

type AttributeWithLanguage interface {
	Attribute
	GetLanguage() string
}

type ExternalIdSetter func(externalId string, base Base) Base

type BaseDomain struct {
	ExternalId string `gorm:"type:varchar(100);unique_index;column:external_id"`
	Id         uint64 `gorm:"primaryKey"`
	CreatedAt  *time.Time
	UpdatedAt  *time.Time
	DeletedAt  *time.Time
	Status     int
}

func (bd BaseDomain) GetExternalId() string {
	return bd.ExternalId
}

func (bd BaseDomain) GetId() uint64 {
	return bd.Id
}

func (bd BaseDomain) GetStatus() Status {
	return Status(bd.Status)
}

func (bd BaseDomain) GetCreatedAt() time.Time {
	return *bd.CreatedAt
}

func (bd BaseDomain) GetUpdatedAt() time.Time {
	return *bd.UpdatedAt
}

func (bd BaseDomain) GetDeletedAt() time.Time {
	return *bd.DeletedAt
}
