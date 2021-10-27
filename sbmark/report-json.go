package sbmark

import (
	"encoding/json"
	"io/ioutil"
)

func ToJson(ctx *BenchmarkContext) ([]byte, error) {
	return json.MarshalIndent(ctx, "", "  ")
}

func FromJsonFile(jsonFile string) (*BenchmarkContext, error) {
	jsonData, err := ioutil.ReadFile(jsonFile)
	if err != nil {
		return nil, err
	}
	return FromJsonByteArray(jsonData)
}

func FromJsonByteArray(jsonData []byte) (*BenchmarkContext, error) {
	b := &BenchmarkContext{}
	err := json.Unmarshal(jsonData, b)
	return b, err
}
