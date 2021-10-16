package sbmark

import "encoding/json"

func ToJson(report Report) ([]byte, error) {
	return json.MarshalIndent(report, "", "  ")
}
