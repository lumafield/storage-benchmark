package sbmark

import (
	"bytes"
	"io"
)

// Specific Object API abstraction used to create benchmarks for
type BenchmarkAPI interface {
	CreateBucket(bucketName string) error
	HeadObject(bucket string, key string) error
	PutObject(bucket string, key string, reader *bytes.Reader) error
	GetObject(bucket string, key string) (io.ReadCloser, error)
	DeleteObject(bucket string, key string) error
}