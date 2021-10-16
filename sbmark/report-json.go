package sbmark

import "encoding/json"

func ToJson(ctx *BenchmarkContext) ([]byte, error) {
	return json.MarshalIndent(ctx, "", "  ")
}
