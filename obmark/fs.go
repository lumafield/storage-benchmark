package obmark

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type FsObjectClient struct {
	rootPath string
}

func NewFsClient(obConfig *ObjectClientConfig) *FsObjectClient {
	return &FsObjectClient{
		rootPath: obConfig.Endpoint,
	}
}

func (c *FsObjectClient) bucketPath(bucketName string) string {
	return filepath.Join(c.rootPath, bucketName)
}

func (c *FsObjectClient) objectPath(bucketName string, key string) string {
	return  c.createDirectory(filepath.Join(c.rootPath, bucketName), key)
}

func (c *FsObjectClient) createDirectory(path string, key string) string{
	splitteKey := strings.Split(key,"/")
	_, err := os.Stat(filepath.Join(path, splitteKey[0]))
	if os.IsNotExist(err) {
		os.Mkdir(filepath.Join(path, splitteKey[0]), os.ModePerm)
	}
	return filepath.Join(path, splitteKey[0], splitteKey[1] )
}


func (c *FsObjectClient) CreateBucket(bucketName string) error {
	return os.MkdirAll(c.bucketPath(bucketName), os.ModePerm)
}

func (c *FsObjectClient) HeadObject(bucketName string, key string) error {
	_, err := os.Stat(c.objectPath(bucketName, key))
	if os.IsExist(err) {
		return nil
	}
	return errors.New("NotFound: "+c.objectPath(bucketName, key))
}

func (c *FsObjectClient) PutObject(bucketName string, key string, reader *bytes.Reader) error {
	writer, err := os.Create(c.objectPath(bucketName, key))
	if err != nil {
		return err
	}
	_, err = reader.WriteTo(writer)
	return err
}

func (c *FsObjectClient) GetObject(bucketName string, key string) (io.ReadCloser, error) {
	return os.Open(c.objectPath(bucketName, key))
}

func (c *FsObjectClient) DeleteObject(bucketName string, key string) error {

	if err := os.Remove(c.objectPath(bucketName, key)); err != nil {
		// TODO if file does not exist return errors.New("NotFound: Not found "+c.objectPath(bucketName, key))
		// How to check err for ENOENT?
	}
	return nil
}

