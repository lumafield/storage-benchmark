package sbmark

import "fmt"

type BenchmarkMode interface {
	IsFinished(numberOfRuns int) bool
	PrintHeader(objectSize uint64, operationToTest string)
}

// formats bytes to KB or MB
func ByteFormat(bytes float64) string {
	if bytes >= 1024*1024 {
		return fmt.Sprintf("%.f MB", bytes/1024/1024)
	}
	return fmt.Sprintf("%.f KB", bytes/1024)
}
