package sbmark

import (
	"errors"
	"fmt"
	"strings"
)

const samplesMin = 4 // to calculate the 0.25 percentile we need at least 4 samples of each run

type state int

const (
	stopped state = iota + 1
	started
)

type BenchmarkMode interface {
	IsFinished(numberOfRuns int) bool
	PrintHeader(objectSize uint64, operationToTest string)
}

type BenchmarkContext struct {
	// an optional description of the test run. It will be added to the .json report.
	Description string

	// the hostname where this benchmark is executed from
	Hostname string

	// the region if available.
	Region string

	// the endpoint URL or path if applicable
	Endpoint string

	// the script will automatically create a bucket to use for the test, and it tries to get a unique bucket name
	// by generating a sha hash of the hostname
	BucketName string

	// the min and max object sizes to test - 1 = 1 KB, and the size doubles with every increment
	PayloadsMin int
	PayloadsMax int

	// the min and max thread count to use in the test
	ThreadsMin int
	ThreadsMax int

	// the number of samples to collect for each benchmark record
	Samples int

	// the client to operate on objects. It's safe to use a single client across multiple go routines.
	Client StorageInterface

	// the mode of the testrun
	Mode BenchmarkMode

	// operations might be "read" or "write. Default is "read".
	OperationToTest string

	// The final report of this benchmark run
	Report Report

	// For infinite or burst mode a numberOfRuns is incremented for every loop.
	NumberOfRuns int

	// The state of this context (started or stopped)
	state state
}

func (ctx *BenchmarkContext) Start() error {
	err := ctx.Validate()
	if err != nil {
		return err
	}
	ctx.setupClient()
	ctx.state = started
	return nil
}

func (ctx *BenchmarkContext) setupClient() {
	if !strings.HasPrefix(strings.ToLower(ctx.Endpoint), "http") {
		ctx.Client = NewFsClient(&FsObjectClientConfig{
			RootPath: ctx.Endpoint,
		})
	} else {
		ctx.Client = NewS3Client(&S3ObjectClientConfig{
			Region:   ctx.Region,
			Endpoint: ctx.Endpoint,
			Insecure: true,
		})
	}
}

func (ctx *BenchmarkContext) Validate() error {

	if ctx.BucketName == "" {
		return errors.New("sbmark.BenchmarkContext: BucketName may not be empty")
	}

	if ctx.Samples < samplesMin {
		return fmt.Errorf("sbmark.BenchmarkContext: Minimum number of samples must be '%d'", samplesMin)
	}

	if ctx.OperationToTest != "read" && ctx.OperationToTest != "write" {
		return fmt.Errorf("sbmark.BenchmarkContext: Unknown operation '%s'. Please use 'read' or 'write'", ctx.OperationToTest)
	}

	if ctx.PayloadsMin > ctx.PayloadsMax {
		return errors.New("sbmark.BenchmarkContext: PayloadsMin can't be greater than PayloadsMax")
	}

	if ctx.ThreadsMin > ctx.ThreadsMax {
		return errors.New("sbmark.BenchmarkContext: ThreadsMin can't be greater than ThreadsMax")
	}

	return nil
}

func (ctx *BenchmarkContext) NumberOfObjectsPerPayload() int {
	return ctx.NumberOfThreads() * ctx.Samples
}

func (ctx *BenchmarkContext) NumberOfThreads() int {
	return ctx.ThreadsMax - ctx.ThreadsMin + 1
}

// formats bytes to KB or MB
func ByteFormat(bytes float64) string {
	if bytes >= 1024*1024 {
		return fmt.Sprintf("%.f MB", bytes/1024/1024)
	}
	return fmt.Sprintf("%.f KB", bytes/1024)
}
