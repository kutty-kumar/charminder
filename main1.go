package main

import (
	"encoding/json"
	"fmt"
	"github.com/gobeam/stringy"
	"reflect"
	"strings"
)

var (
	kindStr = map[reflect.Kind]string{
		reflect.String: "text",
		reflect.Int64:  "long",
		reflect.Int32:  "integer",
		reflect.Bool:   "bool",
		reflect.Uint64: "long",
		reflect.Slice:  "nested",
		reflect.Struct: "nested",
	}
	searchableFields []string
	fieldMappings    = make(map[string]FieldAnalysis)
)

type FieldAnalysis struct {
	Analyzer       string
	SearchAnalyzer string
}

func (f *FieldAnalysis) String() string {
	return fmt.Sprintf("{\"analyzer\":\"%v\", \"search_analyzer\":\"%v\"}", f.Analyzer, f.SearchAnalyzer)
}

type Identity struct {
	Key   string `json:"name:key;analyzer:my_analyzer;search_analyzer:my_analyzer"`
	Value string `json:"name:value;analyzer:my_analyzer;search_analyzer:my_analyzer"`
}

type Test struct {
	TestName string `json:"test_name;analyzer:my_analyzer;search_analyzer:my_analyzer"`
}

type Location struct {
	Name     string   `json:"name;analyzer:my_analyzer;search_analyzer:my_analyzer"`
	ZipCodes []string `json:"zip_codes;analyzer:my_analyzer;search_analyzer:my_analyzer"`
	Test     Test     `json:"test"`
}

type University struct {
	Name     string   `json:"name;analyzer:my_analyzer;search_analyzer:my_analyzer"`
	Location Location `json:"location"`
	Credits  []string `json:"credits;analyzer:my_analyzer;search_analyzer:my_analyzer"`
}

type Student struct {
	Fname             string     `json:"name:f_name;analyzer:my_analyzer;search_analyzer:my_analyzer"`
	Lname             string     `json:"name:l_name;analyzer:my_analyzer;search_analyzer:my_analyzer"`
	City              string     `json:"name:city;analyzer:my_analyzer;search_analyzer:my_analyzer"`
	Mobile            string     `json:"name:mobile;analyzer:my_analyzer;search_analyzer:my_analyzer"`
	Identities        []Identity `json:"name:identities"`
	Courses           []string   `json:"courses;analyzer:my_analyzer;search_analyzer:my_analyzer"`
	CurrentUniversity University `json:"current_university"`
}

type Analyzer struct {
	Type      string   `json:"type"`
	Tokenizer string   `json:"tokenizer"`
	Filter    []string `json:"filter"`
}

type Analysis struct {
	Analyzer map[string]Analyzer `json:"analyzer"`
	Filter   Filter              `json:"filter"`
}

type FilterObj struct {
	Type      string `json:"type"`
	StopWords string `json:"stop_words"`
}

type Filter struct {
	Filter map[string]FilterObj `json:"filter"`
}

type Settings struct {
	Analysis Analysis `json:"analysis"`
}

func getTagValue(rawTag, key string) string {
	tags := strings.Split(rawTag, ";")
	for _, tag := range tags {
		if strings.HasPrefix(tag, key) {
			return tag[len(key)+1:]
		}
	}
	return ""
}

func GetMappingForSlice(w reflect.Type, parentPath string) string {
	var mappings []string
	for j := 0; j < w.NumField(); j++ {
		if w.Field(j).Type.Kind() != reflect.Struct && w.Field(j).Type.Kind() != reflect.Slice && w.Field(j).Type.Kind() != reflect.Chan {
			if w.Field(j).Type.Kind() == reflect.String {
				mappings = append(mappings, fmt.Sprintf("   \"%v\": {\n     \"type\": \"%v\"\n, \"fields\": {\n\"keyword\": {\n \"type\": \"keyword\"} } }", toSnakeCase(w.Field(j).Name), kindStr[w.Field(j).Type.Kind()]))
				attrName := fmt.Sprintf("%v.%v", parentPath, toSnakeCase(w.Field(j).Name))
				searchableFields = append(searchableFields, attrName)
				fieldMappings[attrName] = FieldAnalysis{
					Analyzer:       getTagValue(w.Field(j).Tag.Get("json"), "analyzer"),
					SearchAnalyzer: getTagValue(w.Field(j).Tag.Get("json"), "search_analyzer"),
				}
			} else {
				mappings = append(mappings, fmt.Sprintf("\"%v\": {\"type\": \"%v\"}", toSnakeCase(w.Field(j).Name), kindStr[w.Field(j).Type.Kind()]))
			}
		} else if w.Field(j).Type.Kind() == reflect.Struct || (w.Field(j).Type.Kind() == reflect.Slice && w.Field(j).Type.Elem().Kind() == reflect.Struct) {
			mappings = append(mappings, fmt.Sprintf("\"%v\":{\"properties\": {\n %v \n}\n}", toSnakeCase(w.Field(j).Name), GetMappingForSlice(w.Field(j).Type, fmt.Sprintf("%v.%v", parentPath, toSnakeCase(w.Field(j).Name)))))
		} else if w.Field(j).Type.Kind() == reflect.Slice && (w.Field(j).Type.Elem().Kind() != reflect.Struct && w.Field(j).Type.Elem().Kind() != reflect.Chan) {
			if w.Field(j).Type.Kind() == reflect.String {
				attrName := fmt.Sprintf("%v.%v", parentPath, toSnakeCase(w.Field(j).Name))
				searchableFields = append(searchableFields, attrName)
				fieldMappings[attrName] = FieldAnalysis{
					Analyzer:       getTagValue(w.Field(j).Tag.Get("json"), "analyzer"),
					SearchAnalyzer: getTagValue(w.Field(j).Tag.Get("json"), "search_analyzer"),
				}
				mappings = append(mappings, fmt.Sprintf("   \"%v\": {\n     \"type\": \"%v\"\n, \"fields\": {\n\"keyword\": {\n \"type\": \"keyword\"} } }", toSnakeCase(w.Field(j).Name), kindStr[w.Field(j).Type.Kind()]))
			} else {
				attrName := fmt.Sprintf("%v.%v", parentPath, toSnakeCase(w.Field(j).Name))
				searchableFields = append(searchableFields, attrName)
				fieldMappings[attrName] = FieldAnalysis{
					Analyzer:       getTagValue(w.Field(j).Tag.Get("json"), "analyzer"),
					SearchAnalyzer: getTagValue(w.Field(j).Tag.Get("json"), "search_analyzer"),
				}
				mappings = append(mappings, fmt.Sprintf("\"%v\": {\"type\": \"%v\"}", toSnakeCase(w.Field(j).Name), kindStr[w.Field(j).Type.Elem().Kind()]))
			}
		}
	}
	return strings.Join(mappings, ",\n")
}

func GetMapping(v reflect.Value) string {
	var mappings []string
	valType := v.Type()
	for i := 0; i < v.NumField(); i++ {
		attr := v.Field(i)
		if attr.Kind() != reflect.Struct && attr.Kind() != reflect.Slice && attr.Kind() != reflect.Chan {
			if attr.Kind() == reflect.String {
				attrName := toSnakeCase(valType.Field(i).Name)
				searchableFields = append(searchableFields, attrName)
				fieldMappings[attrName] = FieldAnalysis{
					Analyzer:       getTagValue(valType.Field(i).Tag.Get("json"), "analyzer"),
					SearchAnalyzer: getTagValue(valType.Field(i).Tag.Get("json"), "search_analyz	er"),
				}
				mappings = append(mappings, fmt.Sprintf("   \"%v\": {\n     \"type\": \"%v\"\n, \"fields\": {\n\"keyword\": {\n \"type\": \"keyword\"} } }", toSnakeCase(valType.Field(i).Name), kindStr[attr.Kind()]))
			} else {
				mappings = append(mappings, fmt.Sprintf("   \"%v\": {\n     \"type\": \"%v\"\n   }", toSnakeCase(valType.Field(i).Name), kindStr[attr.Kind()]))
			}
		} else if attr.Kind() == reflect.Slice {
			attrType := reflect.TypeOf(attr.Interface()).Elem().Kind()
			if attrType != reflect.Struct {
				if attrType == reflect.String {
					attrName := toSnakeCase(valType.Field(i).Name)
					mappings = append(mappings, fmt.Sprintf("   \"%v\": {\n     \"type\": \"%v\"\n, \"fields\": {\n\"keyword\": {\n \"type\": \"keyword\"} } }", toSnakeCase(valType.Field(i).Name), kindStr[attr.Kind()]))
					searchableFields = append(searchableFields, attrName)
					fieldMappings[attrName] = FieldAnalysis{
						Analyzer:       getTagValue(valType.Field(i).Tag.Get("json"), "analyzer"),
						SearchAnalyzer: getTagValue(valType.Field(i).Tag.Get("json"), "search_analyzer"),
					}
				} else {
					mappings = append(mappings, fmt.Sprintf("   \"%v\": {\n     \"type\": \"%v\"\n   }", toSnakeCase(valType.Field(i).Name), kindStr[attrType]))
				}
			} else {
				sFace := reflect.TypeOf(attr.Interface()).Elem()
				mappings = append(mappings, fmt.Sprintf("\"%v\":{\n    \"properties\":    {\n %v }\n  }", toSnakeCase(valType.Field(i).Name), GetMappingForSlice(sFace, toSnakeCase(valType.Field(i).Name))))
			}
		} else if attr.Kind() == reflect.Struct {
			mappings = append(mappings, fmt.Sprintf("\"%v\":{\n    \"properties\":    {\n %v }\n  }", toSnakeCase(valType.Field(i).Name), GetMappingForSlice(reflect.TypeOf(attr.Interface()), toSnakeCase(valType.Field(i).Name))))
		}
	}
	return "{\n \"mappings\":{ \n  \"properties\":{\n" + strings.Join(mappings, ",\n") + "\n  }\n }\n}"
}

func toSnakeCase(input string) string {
	return stringy.New(input).SnakeCase().ToLower()
}
func main() {
	s := Student{"Chetan", "Tulsyan", "Bangalore", "7777777777", []Identity{{Key: "email", Value: "kutty.aarathorn@gmail.com"}}, []string{"mathematics-1"}, University{
		"Harvard",
		Location{
			"New York",
			[]string{"560073"},
			Test{
				TestName: "Test name",
			},
		},
		[]string{"1"},
	}}
	v := reflect.ValueOf(s)
	var setting map[string]interface{}
	err := json.Unmarshal([]byte(GetMapping(v)), &setting)
	if err != nil {
		panic(err)
	}
	setting["settings"] = Analysis{
		Analyzer: map[string]Analyzer{
			"my_analyzer": {
				Tokenizer: "standard",
				Type:      "custom",
				Filter: []string{
					"lowercase",
				},
			},
			"my_stop_analyzer": {
				Tokenizer: "standard",
				Type:      "custom",
				Filter: []string{
					"lowercase",
					"english_stop",
				},
			},
		},
		Filter: Filter{
			map[string]FilterObj{
				"english_stop": {
					StopWords: "_english_",
					Type:      "stop",
				},
			},
		},
	}
	settingsJson, err := json.Marshal(setting)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Settings %v\n", string(settingsJson))
	fmt.Printf("Searchable felds %v\n", searchableFields)
	for field, mapping := range fieldMappings {
		fmt.Println(fmt.Sprintf("%v %v %v", field, mapping.Analyzer, mapping.SearchAnalyzer))
	}
}
