package cute

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type MoveEval struct {
	Ply        int32  `parquet:"name=ply, type=INT32"`
	ScoreType  string `parquet:"name=score_type, type=BYTE_ARRAY, convertedtype=UTF8"`
	ScoreValue int32  `parquet:"name=score_value, type=INT32"`
}

type GameRecord struct {
	GameID      string     `parquet:"name=game_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	SenteName   string     `parquet:"name=sente_name, type=BYTE_ARRAY, convertedtype=UTF8"`
	SenteRating int32      `parquet:"name=sente_rating, type=INT32"`
	GoteName    string     `parquet:"name=gote_name, type=BYTE_ARRAY, convertedtype=UTF8"`
	GoteRating  int32      `parquet:"name=gote_rating, type=INT32"`
	Result      string     `parquet:"name=result, type=BYTE_ARRAY, convertedtype=UTF8"`
	WinReason   string     `parquet:"name=win_reason, type=BYTE_ARRAY, convertedtype=UTF8"`
	MoveCount   int32      `parquet:"name=move_count, type=INT32"`
	MoveEvals   []MoveEval `parquet:"name=move_evals, type=LIST"`
}

type ParquetSchema struct {
	Name   string         `json:"name"`
	Fields []ParquetField `json:"fields"`
}

type ParquetField struct {
	Name     string      `json:"name"`
	Type     interface{} `json:"type"`
	Nullable bool        `json:"nullable"`
}

const schemaPath = "schema/parquet_schema.json"

func WriteParquet(path string, records <-chan GameRecord, parallel int64) error {
	fmt.Printf("writing parquet to %s\n", path)

	schema, err := loadParquetSchema(schemaPath)
	if err != nil {
		return err
	}
	if err := validateSchema(schema, GameRecord{}); err != nil {
		return err
	}

	fileWriter, err := local.NewLocalFileWriter(path)
	if err != nil {
		return err
	}
	defer fileWriter.Close()

	parquetWriter, err := writer.NewParquetWriter(fileWriter, new(GameRecord), parallel)
	if err != nil {
		return err
	}
	parquetWriter.CompressionType = parquet.CompressionCodec_SNAPPY

	for record := range records {
		if err := parquetWriter.Write(record); err != nil {
			return err
		}
	}
	if err := parquetWriter.WriteStop(); err != nil {
		return err
	}
	return fileWriter.Close()
}

func loadParquetSchema(path string) (ParquetSchema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ParquetSchema{}, err
	}
	var schema ParquetSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return ParquetSchema{}, err
	}
	return schema, nil
}

func validateSchema(schema ParquetSchema, sample any) error {
	schemaFields := make(map[string]struct{}, len(schema.Fields))
	for _, field := range schema.Fields {
		schemaFields[field.Name] = struct{}{}
	}
	structFields := structParquetFieldNames(sample)
	missing := diffKeys(schemaFields, structFields)
	extra := diffKeys(structFields, schemaFields)
	if len(missing) > 0 || len(extra) > 0 {
		return fmt.Errorf("parquet schema mismatch: missing=%v extra=%v", missing, extra)
	}
	return nil
}

func structParquetFieldNames(sample any) map[string]struct{} {
	fields := map[string]struct{}{}
	v := reflect.TypeOf(sample)
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		name := parseParquetName(field.Tag.Get("parquet"))
		if name != "" {
			fields[name] = struct{}{}
		}
	}
	return fields
}

func parseParquetName(tag string) string {
	if tag == "" {
		return ""
	}
	parts := strings.Split(tag, ",")
	for _, part := range parts {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) == 2 && kv[0] == "name" {
			return kv[1]
		}
	}
	return ""
}

func diffKeys(a, b map[string]struct{}) []string {
	var diff []string
	for key := range a {
		if _, ok := b[key]; !ok {
			diff = append(diff, key)
		}
	}
	return diff
}
