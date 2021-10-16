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
	Description   string `json:"description"`  // An optional description that helps to identify the test run
	Hostname      string `json:"hostname"`     // the hostname where this benchmark is executed from
	Region        string `json:"region"`       // S3 region
	Endpoint      string `json:"endpoint"`     // the endpoint URL or path where the operations are directed to
	Path          string `json:"path"`         // the path where to operations are executed on
	PayloadsMin   int    `json:"payloads_min"` // the minimum payload to test (powers of 2)
	PayloadsMax   int    `json:"payloads_max"` // the maximum payload to test (powers of 2)
	ThreadsMin    int    `json:"threads_min"`  // the mininum number of threads to test
	ThreadsMax    int    `json:"threads_max"`  // the maxiumu number of threads to test
	Samples       int    `json:"samples"`      // the number of samples to collect for each benchmark record
	OperationName string `json:"operation"`    // operations might be "read" or "write. Default is "read".
	ModeName      string `json:"mode"`         // latency, burst, ...
	Report        Report `json:"report"`       // The final report of this benchmark run

	// the BenchmarkMode instance of the testrun (corresponds to the ModeName)
	Mode BenchmarkMode

	// the client to operate on objects. It's safe to use a single client across multiple go routines.
	Client StorageInterface

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

	if ctx.Path == "" {
		return errors.New("sbmark.BenchmarkContext: BucketName may not be empty")
	}

	if ctx.Samples < samplesMin {
		return fmt.Errorf("sbmark.BenchmarkContext: Minimum number of samples must be '%d'", samplesMin)
	}

	if ctx.OperationName != "read" && ctx.OperationName != "write" {
		return fmt.Errorf("sbmark.BenchmarkContext: Unknown operation '%s'. Please use 'read' or 'write'", ctx.OperationName)
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
