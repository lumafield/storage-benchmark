package obmark

import (
	"io"
	"bytes"
)

// Specific Object API abstraction used to create benchmarks for
type ObjectClient interface {
	CreateBucketRequest(bucket string, region string) ObjectRequest
	HeadObjectRequest(bucket string, key string) ObjectRequest
	PutObjectRequest(bucket string, key string, reader *bytes.Reader) ObjectRequest
	GetObjectRequest(bucket string, key string) ObjectRequest
	DeleteObjectRequest(bucket string, key string) ObjectRequest
}

type ObjectRequest interface {
	Send() (*ObjectResponse, error)
}

type ObjectResponse struct {
	Body *io.ReadCloser
}

type ObjectClientConfig struct {
	region string
	endpoint string
}