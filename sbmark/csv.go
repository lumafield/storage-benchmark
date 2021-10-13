package sbmark

import (
	"bytes"
	"encoding/csv"
	"fmt"
)

func CsvReader(report Report) *bytes.Reader {
	// array of csv records used to upload the results
	var csvRecords [][]string

	for _, record := range report.Records {
		// add the results to the csv array
		csvRecords = append(csvRecords, []string{
			report.ClientEnv,
			report.ServerEnv,
			fmt.Sprintf("%d", record.ObjectSizeBytes),
			fmt.Sprintf("%d", record.Threads),
			fmt.Sprintf("%.3f", record.ThroughputMBps()),
			fmt.Sprintf("%.1f", record.TimeToFirstByte["avg"]),
			fmt.Sprintf("%.1f", record.TimeToFirstByte["min"]),
			fmt.Sprintf("%.1f", record.TimeToFirstByte["p25"]),
			fmt.Sprintf("%.1f", record.TimeToFirstByte["p50"]),
			fmt.Sprintf("%.1f", record.TimeToFirstByte["p75"]),
			fmt.Sprintf("%.1f", record.TimeToFirstByte["p90"]),
			fmt.Sprintf("%.1f", record.TimeToFirstByte["p99"]),
			fmt.Sprintf("%.1f", record.TimeToFirstByte["max"]),
			fmt.Sprintf("%.2f", record.TimeToLastByte["avg"]),
			fmt.Sprintf("%.2f", record.TimeToLastByte["min"]),
			fmt.Sprintf("%.1f", record.TimeToLastByte["p25"]),
			fmt.Sprintf("%.1f", record.TimeToLastByte["p50"]),
			fmt.Sprintf("%.1f", record.TimeToLastByte["p75"]),
			fmt.Sprintf("%.1f", record.TimeToLastByte["p90"]),
			fmt.Sprintf("%.1f", record.TimeToLastByte["p99"]),
			fmt.Sprintf("%.1f", record.TimeToLastByte["max"]),
		})
	}

	b := &bytes.Buffer{}
	w := csv.NewWriter(b)
	_ = w.WriteAll(csvRecords)

	return bytes.NewReader(b.Bytes())
}
