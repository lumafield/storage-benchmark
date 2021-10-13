package sbmark

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/tcnksm/go-httpstat"
)

type S3ObjectClient struct {
	delegate *s3.Client
	cfg      *S3ObjectClientConfig
}

type S3ObjectClientConfig struct {
	Region   string
	Endpoint string
	Insecure bool
}

func NewS3Client(obConfig *S3ObjectClientConfig) BenchmarkAPI {
	// gets the AWS credentials from the default file or from the EC2 instance profile
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic("Unable to load AWS SDK config: " + err.Error())
	}

	// set the endpoint in the configuration
	if obConfig.Endpoint != "" {
		customResolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL: obConfig.Endpoint,
			}, nil
		})

		cfg, err = config.LoadDefaultConfig(context.TODO(), config.WithEndpointResolver(customResolver))
		if err != nil {
			panic("Unable to load AWS SDK config: " + err.Error())
		}
	}

	// set the SDK region to either the one from the program arguments or else to the same region as the EC2 instance
	cfg.Region = obConfig.Region

	// trust all certificates
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: obConfig.Insecure},
	}
	// set a 3-minute timeout for all S3 calls, including downloading the body
	cfg.HTTPClient = &http.Client{
		Timeout:   time.Second * 180,
		Transport: tr,
	}

	// custom endpoints don't generally work with the bucket in the host prefix
	usePathStyleOptFunc := func(options *s3.Options) {
		options.UsePathStyle = obConfig.Endpoint != ""
	}

	// crete the S3 client
	var s3Client = s3.NewFromConfig(cfg, usePathStyleOptFunc)

	return &S3ObjectClient{
		delegate: s3Client,
		cfg:      obConfig,
	}
}

func (c *S3ObjectClient) CreateBucket(bucketName string) error {
	s3Client := c.delegate
	_, err := s3Client.CreateBucket(context.TODO(), &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	return err
}

func (c *S3ObjectClient) HeadObject(bucketName string, key string) error {
	s3Client := c.delegate
	_, err := s3Client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	return err
}

func (c *S3ObjectClient) PutObject(bucketName string, key string, reader *bytes.Reader) error {
	var result httpstat.Result
	ctx := httpstat.WithHTTPStat(context.TODO(), &result)
	s3Client := c.delegate
	_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   reader,
	})

	// Show the results
	fmt.Printf("DNS lookup: %d ms\n", int(result.DNSLookup/time.Millisecond))
	fmt.Printf("TCP connection: %d ms\n", int(result.TCPConnection/time.Millisecond))
	fmt.Printf("TLS handshake: %d ms\n", int(result.TLSHandshake/time.Millisecond))
	fmt.Printf("Server processing: %d ms\n", int(result.ServerProcessing/time.Millisecond))
	return err
}

func (c *S3ObjectClient) GetObject(bucketName string, key string) (io.ReadCloser, error) {
	var result httpstat.Result
	ctx := httpstat.WithHTTPStat(context.TODO(), &result)
	s3Client := c.delegate
	resp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	// Show the results
	fmt.Printf("DNS lookup: %d ms\n", int(result.DNSLookup/time.Millisecond))
	fmt.Printf("TCP connection: %d ms\n", int(result.TCPConnection/time.Millisecond))
	fmt.Printf("TLS handshake: %d ms\n", int(result.TLSHandshake/time.Millisecond))
	fmt.Printf("Server processing: %d ms\n", int(result.ServerProcessing/time.Millisecond))
	return resp.Body, err
}

func (c *S3ObjectClient) DeleteObject(bucketName string, key string) error {
	s3Client := c.delegate
	_, err := s3Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	return err
}
