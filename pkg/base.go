package pkg

import (
	"database/sql"
	"fmt"
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
	MarshalBinary() ([]byte, error)
	ToJson() (string, error)
	String() string
	UnmarshalBinary(buffer []byte) error
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

type ETime time.Time

func (et ETime) MarshalJSON() ([]byte, error) {
	stamp := fmt.Sprintf("\"%v\"", time.Time(et).Format(time.RFC3339))
	return []byte(stamp), nil
}

type BaseDomain struct {
	ExternalId string `json:"external_id" gorm:"type:varchar(100);uniqueIndex"`
	Id         uint64 `json:"id" gorm:"primaryKey"`
	CreatedAt  ETime  `json:"created_at" type:"date"`
	UpdatedAt  ETime  `type:"date"`
	DeletedAt  ETime  `type:"date"`
	Status     int    `type:"int"`
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
	return time.Time(bd.CreatedAt)
}

func (bd BaseDomain) GetUpdatedAt() time.Time {
	return time.Time(bd.UpdatedAt)
}

func (bd BaseDomain) GetDeletedAt() time.Time {
	return time.Time(bd.DeletedAt)
}

type Event interface {
	GetEntityId() string
	GetEntityType() string
	GetId() string
	ToBytes() []byte
	FromByte(bytes []byte)
	Entity() interface{}
}