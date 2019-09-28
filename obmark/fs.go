package obmark

import (
	"bytes"
)

type FsObjectClient struct {
}

func NewFsClient(obConfig *ObjectClientConfig) *FsObjectClient {
	return &FsObjectClient{}
}

func (c *FsObjectClient) CreateBucketRequest(bucketName string, region string) ObjectRequest {
	return nil
}

func (c *FsObjectClient) HeadObjectRequest(bucketName string, key string) ObjectRequest {
	return nil
}

func (c *FsObjectClient) PutObjectRequest(bucketName string, key string, reader *bytes.Reader) ObjectRequest {
	return nil
}

func (c *FsObjectClient) GetObjectRequest(bucketName string, key string) ObjectRequest {
	return nil
}

func (c *FsObjectClient) DeleteObjectRequest(bucketName string, key string) ObjectRequest {
	return nil
}
