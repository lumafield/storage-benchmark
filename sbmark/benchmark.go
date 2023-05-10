package sbmark

import (
	"crypto/sha1"
	"errors"
	"fmt"
	log2 "log"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

const samplesMin = 4 // to calculate the 0.25 percentile we need at least 4 samples of each run

type state int

const (
	stopped state = iota + 1
	started
)

type Ticker interface {
	Add(ticks int) error
}

type NilTicker struct{}

func (t *NilTicker) Add(ticks int) error {
	return nil
}

type BenchmarkMode interface {
	DisableKeepAlives() bool
	PrintHeader(operationToTest string)
	PrintPayloadHeader(objectSize uint64, operationToTest string)
	PrintRecord(record Record)
	PrintPayloadFooter()
	PrintFooter()
	EnsureTestdata(ctx *BenchmarkContext, payloadSize uint64)
	CleanupTestdata(ctx *BenchmarkContext, payloadSize uint64)
	ExecuteBenchmark(ctx *BenchmarkContext, payloadSize uint64)
	IsFinished(numberOfRuns int) bool
}

type BenchmarkOperation interface {
	EnsureTestdata(ctx *BenchmarkContext, payloadSize uint64, ticker Ticker)
	Execute(ctx *BenchmarkContext, sampleId int, payloadSize uint64) Latency
	CleanupTestdata(ctx *BenchmarkContext, ticker Ticker)
}

type BenchmarkContext struct {
	Description   string `json:"description"`  // An optional description that helps to identify the test run.
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
	ModeName      string `json:"mode"`         // latency, throughput, ...
	Report        Report `json:"report"`       // The final report of this benchmark run

	// the BenchmarkMode instance of the testrun (corresponds to the ModeName)
	Mode BenchmarkMode `json:"-"`

	// The BenchmarkOperation instance of this testrun (corresponds to the OperationName)
	Operation BenchmarkOperation `json:"-"`

	// the client to operate on objects. It's safe to use a single client across multiple go routines.
	Client StorageInterface `json:"-"`

	// For burst mode a numberOfRuns is incremented for every loop.
	NumberOfRuns int `json:"-"`

	// The state of this context (started or stopped)
	state state `json:"-"`

	// The logger can be used from anywhere to log stuff
	InfoLogger    *log2.Logger `json:"-"`
	WarningLogger *log2.Logger `json:"-"`
	ErrorLogger   *log2.Logger `json:"-"`

	// Whether a connection pooling should be 'enabled' or 'disabled'. If not specified or set to 'mode' the default value of the BenchmarkMode is used.
	KeepAlive string `json:"-"`
}

func (ctx *BenchmarkContext) Start() error {
	err := ctx.Validate()
	if err != nil {
		return err
	}
	ctx.setupMode()
	ctx.setupOperation()
	ctx.setupClient()
	ctx.state = started
	return nil
}

func (ctx *BenchmarkContext) setupClient() {
	ctx.Client = NewS3Client(&S3ObjectClientConfig{
		Region:            ctx.Region,
		Endpoint:          ctx.Endpoint,
		Insecure:          true,
		DisableKeepAlives: ctx.disableKeepAlives(),
	})
}

func (ctx *BenchmarkContext) disableKeepAlives() bool {
	switch strings.ToLower(ctx.KeepAlive) {
	case "enabled":
		return false
	case "disabled":
		return true
	default:
		return ctx.Mode.DisableKeepAlives()
	}
}

func (ctx *BenchmarkContext) setupMode() {
	if strings.HasPrefix(strings.ToLower(ctx.ModeName), "burst") {
		ctx.Mode = &BurstBenchmarkMode{}
	} else {
		ctx.Mode = &LatencyBenchmarkMode{}
	}
}

func (ctx *BenchmarkContext) setupOperation() {
	if ctx.OperationName == "write" {
		ctx.Operation = &OperationWrite{}
	} else {
		ctx.Operation = &OperationRead{}
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

func (ctx *BenchmarkContext) PrintSettings() {
	fmt.Print("\n--- storage-benchmark - Settings ------------------------------------------------------------------------------------------------------------------------------\n\n")
	fmt.Printf(`Date:			%s`+"\n"+
		`Description:	%s`+"\n"+
		`Client:		%s`+"\n"+
		`Server:		%s`+"\n"+
		`Mode:			%s`+"\n"+
		`Samples:		%d`+"\n",
		ctx.Report.DateTimeUTC, ctx.Description, ctx.Report.ClientEnv, ctx.Report.ServerEnv, ctx.ModeName, ctx.Samples)
}

// formats bytes to KB or MB
func ByteFormat(bytes float64) string {
	if bytes >= 1024*1024 {
		return fmt.Sprintf("%.f MB", bytes/1024/1024)
	}
	return fmt.Sprintf("%.f KB", bytes/1024)
}

// generates an object key from the sha hash of a string, the thread index, and the object size
func generateObjectKey(threadIndex int, payloadSize uint64) string {
	var key string
	keyHash := sha1.Sum([]byte(fmt.Sprintf("%03d-%012d", threadIndex, payloadSize)))
	folder := strconv.Itoa(int(payloadSize))
	key = folder + "/" + (fmt.Sprintf("%x", keyHash))
	return key
}

func GenerateRandomString(seed int) string {
	rand.Seed(time.Now().UnixNano())
	chars := []rune("1234")
	length := 4
	var b strings.Builder
	for i := 0; i < length; i++ {
		b.WriteRune(chars[rand.Intn(len(chars))])
	}
	return b.String()
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
