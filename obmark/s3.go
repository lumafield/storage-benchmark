package obmark

import (
	"bytes"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3ObjectClient struct {
	delegate *s3.S3
}

func NewS3Client(obConfig *ObjectClientConfig) *S3ObjectClient {
	// gets the AWS credentials from the default file or from the EC2 instance profile
	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		panic("Unable to load AWS SDK config: " + err.Error())
	}

	// set the SDK region to either the one from the program arguments or else to the same region as the EC2 instance
	cfg.Region = obConfig.region

	// set the endpoint in the configuration
	if obConfig.endpoint != "" {
		cfg.EndpointResolver = aws.ResolveWithEndpointURL(obConfig.endpoint)
	}

	// set a 3-minute timeout for all S3 calls, including downloading the body
	cfg.HTTPClient = &http.Client{
		Timeout: time.Second * 180,
	}

	// crete the S3 client
	var s3Client = s3.New(cfg)

	// custom endpoints don't generally work with the bucket in the host prefix
	if obConfig.endpoint != "" {
		s3Client.forcePathStyle = true
	}

	return &S3ObjectClient{
		delegate: s3Client,
	}
}

func (c *S3ObjectClient) CreateBucketRequest(bucketName string, region string) ObjectRequest {
	s3Client := c.delegate

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

	return createBucketReq
}

func (c *S3ObjectClient) HeadObjectRequest(bucketName string, key string) ObjectRequest {
	s3Client := c.delegate
	return s3Client.HeadObjectRequest(&s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
}

func (c *S3ObjectClient) PutObjectRequest(bucketName string, key string, reader *bytes.Reader) ObjectRequest {
	s3Client := c.delegate
	return s3Client.PutObjectRequest(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   reader,
	})
}

func (c *S3ObjectClient) GetObjectRequest(bucketName string, key string) ObjectRequest {
	s3Client := c.delegate
	return s3Client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
}

func (c *S3ObjectClient) DeleteObjectRequest(bucketName string, key string) ObjectRequest {
	s3Client := c.delegate
	return s3Client.DeleteObjectRequest(&s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
}
