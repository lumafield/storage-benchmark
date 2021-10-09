package obmark

import (
	"bytes"
	"io"
)

// Specific Object API abstraction used to create benchmarks for
type ObjectClient interface {
	CreateBucket(bucketName string) error
	HeadObject(bucket string, key string) error
	PutObject(bucket string, key string, reader *bytes.Reader) error
	GetObject(bucket string, key string) (io.ReadCloser, error)
	DeleteObject(bucket string, key string) error
}

type ObjectClientConfig struct {
	Region   string
	Endpoint string
	Insecure bool
}
