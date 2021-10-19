package sbmark

import (
	"bytes"
	"context"
	"crypto/tls"
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
	Region            string
	Endpoint          string
	Insecure          bool
	DisableKeepAlives bool
}

func NewS3Client(obConfig *S3ObjectClientConfig) StorageInterface {
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
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: obConfig.Insecure},
		DisableKeepAlives: obConfig.DisableKeepAlives, // false disables connection pooling, so that TCP and TLS handshakes has to be done on every request.
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

func (c *S3ObjectClient) CreateBucket(bucketName string) (Latency, error) {
	var result httpstat.Result
	ctx := httpstat.WithHTTPStat(context.TODO(), &result)
	s3Client := c.delegate

	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})

	return latency(result), err
}

func (c *S3ObjectClient) HeadObject(bucketName string, key string) (Latency, error) {
	var result httpstat.Result
	ctx := httpstat.WithHTTPStat(context.TODO(), &result)
	s3Client := c.delegate

	_, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})

	return latency(result), err
}

func (c *S3ObjectClient) PutObject(bucketName string, key string, reader *bytes.Reader) (Latency, error) {
	var result httpstat.Result
	ctx := httpstat.WithHTTPStat(context.TODO(), &result)
	s3Client := c.delegate

	_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   reader,
	})

	lat := latency(result)
	return lat, err
}

func (c *S3ObjectClient) GetObject(bucketName string, key string) (Latency, io.ReadCloser, error) {
	var result httpstat.Result
	ctx := httpstat.WithHTTPStat(context.TODO(), &result)
	s3Client := c.delegate

	resp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})

	if err != nil {
		return latency(result), nil, err
	}

	return latency(result), resp.Body, err
}

func (c *S3ObjectClient) DeleteObject(bucketName string, key string) (Latency, error) {
	var result httpstat.Result
	ctx := httpstat.WithHTTPStat(context.TODO(), &result)
	s3Client := c.delegate

	_, err := s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	return latency(result), err
}

func latency(result httpstat.Result) Latency {
	return Latency{
		DNSLookup:        result.DNSLookup,
		TCPConnection:    result.TCPConnection,
		TLSHandshake:     result.TLSHandshake,
		ServerProcessing: result.ServerProcessing,
	}
}
