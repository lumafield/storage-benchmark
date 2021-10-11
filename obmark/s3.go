package obmark

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3ObjectClient struct {
	delegate *s3.Client
	cfg      *ObjectClientConfig
}

func NewS3Client(obConfig *ObjectClientConfig) ObjectClient {
	// gets the AWS credentials from the default file or from the EC2 instance profile
	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		panic("Unable to load AWS SDK config: " + err.Error())
	}

	// set the SDK region to either the one from the program arguments or else to the same region as the EC2 instance
	cfg.Region = obConfig.Region

	// set the endpoint in the configuration
	if obConfig.Endpoint != "" {
		cfg.EndpointResolver = aws.ResolveWithEndpointURL(obConfig.Endpoint)
	}

	// trust all certificates
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: obConfig.Insecure},
	}
	// set a 3-minute timeout for all S3 calls, including downloading the body
	cfg.HTTPClient = &http.Client{
		Timeout:   time.Second * 180,
		Transport: tr,
	}

	// crete the S3 client
	var s3Client = s3.New(cfg)

	// custom endpoints don't generally work with the bucket in the host prefix
	if obConfig.Endpoint != "" {
		s3Client.ForcePathStyle = true
	}

	return &S3ObjectClient{
		delegate: s3Client,
		cfg:      obConfig,
	}
}

func (c *S3ObjectClient) CreateBucket(bucketName string) error {
	s3Client := c.delegate
	region := c.cfg.Region
	// try to create the S3 bucket
	createBucketReq := s3Client.CreateBucketRequest(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: s3.NormalizeBucketLocation(s3.BucketLocationConstraint(region)),
		},
	})

	// AWS S3 has this peculiar issue in which if you want to create bucket in us-east-1 region, you should NOT specify
	// any location constraint. https://github.com/boto/boto3/issues/125
	if strings.ToLower(region) == "us-east-1" {
		createBucketReq = s3Client.CreateBucketRequest(&s3.CreateBucketInput{
			Bucket: aws.String(bucketName),
		})
	}

	_, err := createBucketReq.Send(context.Background())
	return err
}

func (c *S3ObjectClient) HeadObject(bucketName string, key string) error {
	s3Client := c.delegate
	headReq := s3Client.HeadObjectRequest(&s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	_, err := headReq.Send(context.Background())
	return err
}

func (c *S3ObjectClient) PutObject(bucketName string, key string, reader *bytes.Reader) error {
	s3Client := c.delegate
	putReq := s3Client.PutObjectRequest(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   reader,
	})
	_, err := putReq.Send(context.Background())
	return err
}

func (c *S3ObjectClient) GetObject(bucketName string, key string) (io.ReadCloser, error) {
	s3Client := c.delegate
	req := s3Client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	resp, err := req.Send(context.Background())
	return resp.Body, err
}

func (c *S3ObjectClient) DeleteObject(bucketName string, key string) error {
	s3Client := c.delegate
	headReq := s3Client.DeleteObjectRequest(&s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	_, err := headReq.Send(context.Background())
	return err
}
