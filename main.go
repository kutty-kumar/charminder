package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/kutty-kumar/charminder/pkg/db"
	"github.com/sirupsen/logrus"
	"log"
	"net/http"
	"time"
)

type Identity struct {
	Key   string `json:"key" analyzer:"my_analyzer" search_analyzer:"my_analyzer"`
	Value string `json:"value"  analyzer:"my_analyzer" search_analyzer:"my_analyzer"`
}

type Test struct {
	TestName string `json:"test_name" analyzer:"my_analyzer" search_analyzer:"my_analyzer"`
}

type Location struct {
	Name     string   `analyzer:"my_analyzer" search_analyzer:"my_analyzer"`
	ZipCodes []string `json:"zip_codes" analyzer:"my_analyzer" search_analyzer:"my_analyzer" type:"text"`
	Test     Test     `json:"test"`
}

type University struct {
	Name     string   `json:"name" analyzer:"my_analyzer" search_analyzer:"my_analyzer"`
	Location Location `json:"location"`
	Credits  []string `json:"credits" analyzer:"my_analyzer" search_analyzer:"my_analyzer" type:"text"`
}

type Student struct {
	db.BaseDomain
	Fname             string     `json:"f_name" analyzer:"my_analyzer" search_analyzer:"my_analyzer"`
	Lname             string     `json:"l_name" analyzer:"my_analyzer" search_analyzer:"my_analyzer"`
	City              string     `json:"city" analyzer:"my_analyzer" search_analyzer:"my_analyzer"`
	Mobile            string     `json:"mobile" analyzer:"my_analyzer" search_analyzer:"my_analyzer"`
	Identities        []Identity `json:"identities"`
	Courses           []string   `json:"courses" analyzer:"my_analyzer" search_analyzer:"my_analyzer"`
	CurrentUniversity University `json:"current_university"`
}

func (s Student) GetExternalId() string {
	return "external_id"
}

func (s Student) GetName() db.DomainName {
	return "students"
}

func (s Student) GetId() uint64 {
	return 1
}

func (s Student) GetStatus() db.Status {
	panic("implement me")
}

func (s Student) GetCreatedAt() time.Time {
	panic("implement me")
}

func (s Student) GetUpdatedAt() time.Time {
	panic("implement me")
}

func (s Student) GetDeletedAt() time.Time {
	panic("implement me")
}

func (s Student) ToDto() interface{} {
	panic("implement me")
}

func (s Student) FillProperties(dto interface{}) db.Base {
	panic("implement me")
}

func (s Student) Merge(other interface{}) {
	panic("implement me")
}

func (s Student) FromSqlRow(rows *sql.Rows) (db.Base, error) {
	panic("implement me")
}

func (s Student) SetExternalId(externalId string) {
	panic("implement me")
}

func (s Student) MarshalBinary() ([]byte, error) {
	panic("implement me")
}

func (s Student) String() string {
	str, _ := json.Marshal(s)
	return string(str)
}

func (s Student) UnmarshalBinary(buffer []byte) error {
	panic("implement me")
}

func (s Student) ToJson() (string, error) {
	str, err := json.Marshal(s)
	return string(str), err
}

func NewStudent() Student {
	s := Student{}
	s.Fname = "Chethan"
	s.Lname = "Tulsyan"
	s.City = "Bangalore"
	s.Mobile = "77777777"
	s.Identities = []Identity{{Key: "email", Value: "kutty.aarathorn@gmail.com"}}
	s.Courses = []string{"mathematics-1"}
	s.CurrentUniversity = University{
		"Harvard",
		Location{
			"New York",
			[]string{"560073"},
			Test{
				TestName: "Test name",
			},
		},
		[]string{"1"},
	}
	timeNow := db.ETime(time.Now())
	s.BaseDomain = db.BaseDomain{
		ExternalId: "",
		Id:         0,
		CreatedAt:  timeNow,
		UpdatedAt:  timeNow,
		DeletedAt:  timeNow,
		Status:     0,
	}
	return s
}

func main() {
	ctx := context.Background()
	client, err := elasticsearch.NewDefaultClient()
	if err != nil {
		panic("failed initializing elasticsearch client")
	}
	student := NewStudent()
	repo := db.NewElasticsearchRepo(
		db.WithClient(client),
		db.WithIndex("students"),
		db.WithMarshaller(&db.HttpBodyUtil{}),
		db.WithStatusChecker(&db.HttpStatusChecker{}),
		db.WithEntityConverter(func(from map[string]interface{}) db.Base {
			jsonBody, _ := json.Marshal(from)
			student := Student{}
			json.Unmarshal(jsonBody, &student)
			return &student
		}),
		db.WithEntityCreator(func() db.Base {
			return &Student{}
		}),
		db.WithDefaultEntity(student),
		db.WithESLogger(logrus.New()),
		db.WithSettings(db.Settings{
			Analysis: db.Analysis{
				Analyzer: map[string]db.Analyzer{
					"my_analyzer": {
						Tokenizer: string(db.TokenizerMapping.Standard),
						Type:      "custom",
						Filter: []string{
							string(db.TokenFilterMapping.Lowercase),
						},
					},
					"my_stop_analyzer": {
						Tokenizer: string(db.TokenizerMapping.Standard),
						Type:      "custom",
						Filter: []string{
							string(db.TokenFilterMapping.Lowercase),
						},
					},
				},
			},
		}),
		db.WithHttpClient(&http.Client{}),
	)
	err = repo.IndexMappings(ctx)
	if err != nil {
		log.Fatalf("Error creating Index %v\n", err)
	}
	err, base := repo.Create(ctx, &student)
	if err != nil {
		log.Fatalf("Error creating Index %v\n", err)
	}
	log.Printf("Created %v\n", base)
	err, students := repo.TextSearch(ctx, "chethan")
	if err != nil {
		log.Fatalf("Error creating Index %v\n", err)
	}
	for _, student := range students {
		log.Printf("Search returned %v\n", student)
	}
}
