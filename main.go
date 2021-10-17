package main

import (
	"crypto/sha1"
	"flag"
	"fmt"
	log2 "log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/iternity-dotcom/storage-benchmark/sbmark"
	"github.com/schollz/progressbar/v2"
)

const bucketNamePrefix = "storage-benchmark"

var defaultBucketName = fmt.Sprintf("%s-%x", bucketNamePrefix, sha1.Sum([]byte(getHostname())))

// use go build -ldflags "-X main.buildstamp=`date -u '+%Y-%m-%d_%I:%M:%S%p'` -X main.githash=`git rev-parse HEAD`"
var buildstamp = "No build stamp provided"
var githash = "No git hash provided"

// wether to display the version information
var showVersion bool

// if not empty, the results of the test are saved as .csv file
var csvFileName string

// if not empty, the results of the test are saved as .json file
var jsonFileName string

// whether to create a bucket on startup
var createBucket bool

// path for log file
var logPath string

// a test mode to find the maximum throughput for different object sizes
var burstMode bool

// flag for a load test
var infiniteMode bool

// the context for this benchmark includes everything that's needed to run the benchmark
var ctx *sbmark.BenchmarkContext

// program entry point
func main() {
	parseFlags()
	if showVersion {
		displayVersion()
		return
	}
	if createBucket {
		createBenchmarkBucket()
	}
	runBenchmark()
}

func parseFlags() {
	versionArg := flag.Bool("version", false, "Displays the version information.")
	descriptionArg := flag.String("description", "", "The description of your test run run will be added to the .json report.")
	threadsMinArg := flag.Int("threads-min", 8, "The minimum number of threads to use when fetching objects.")
	threadsMaxArg := flag.Int("threads-max", 16, "The maximum number of threads to use when fetching objects.")
	payloadsMinArg := flag.Int("payloads-min", 1, "The minimum object size to test, with 1 = 1 KB, and every increment is a double of the previous value.")
	payloadsMaxArg := flag.Int("payloads-max", 10, "The maximum object size to test, with 1 = 1 KB, and every increment is a double of the previous value.")
	samplesArg := flag.Int("samples", 50, "The number of samples to collect for each test of a single object size and thread count. Default is 50. Minimum value is 4.")
	bucketNameArg := flag.String("bucket-name", defaultBucketName, "The target bucket or folder to be used.")
	regionArg := flag.String("region", "", "Sets the AWS region to use for the S3 bucket. Only applies if the bucket doesn't already exist.")
	endpointArg := flag.String("endpoint", "", "Sets the endpoint to use. Might be any URI.")
	csvArg := flag.String("csv", "", "Saves the results as .csv file.")
	jsonArg := flag.String("json", "", "Saves the results as .json file.")
	operationArg := flag.String("operation", "read", "Specify if you want to measure 'read' or 'write'. Default is 'read'")
	createBucketArg := flag.Bool("create-bucket", false, "create new bucket(default false)")
	logPathArg := flag.String("log-path", "", "Specify the path of the log file. Default is 'currentDir'")
	burstModeArg := flag.Bool("burst-mode", false, "Runs a continuous test to find the maximum throughput for different payload sizes.")
	infiniteModeArg := flag.Bool("infinite-mode", false, "Run the tests in a infinite loop. 'Read' or 'Write' depends on the value of operation flag. Default is 'false'")

	// parse the arguments and set all the global variables accordingly
	flag.Parse()

	showVersion = *versionArg
	csvFileName = *csvArg
	jsonFileName = *jsonArg
	createBucket = *createBucketArg
	if *logPathArg == "" {
		logPath, _ = os.Getwd()
	} else {
		logPath = *logPathArg
	}
	burstMode = *burstModeArg
	infiniteMode = *infiniteModeArg

	ctx = &sbmark.BenchmarkContext{
		Description:   *descriptionArg,
		ModeName:      "LatencyMode",
		Mode:          &sbmark.LatencyBenchmarkMode{},
		OperationName: *operationArg,
		Endpoint:      *endpointArg,
		Region:        *regionArg,
		Path:          *bucketNameArg,
		PayloadsMin:   *payloadsMinArg,
		PayloadsMax:   *payloadsMaxArg,
		ThreadsMin:    *threadsMinArg,
		ThreadsMax:    *threadsMaxArg,
		Samples:       *samplesArg,
		NumberOfRuns:  0,
		Hostname:      getHostname(),
		Logger:        createLogger(),
	}

	err := ctx.Start()
	if err != nil {
		panic(err)
	}
}

func displayVersion() {
	fmt.Printf("Git Commit Hash: %s\n", githash)
	fmt.Printf("UTC Build Time: %s", buildstamp)
}

func createLogger() *log2.Logger {
	file, _ := os.OpenFile(filepath.FromSlash(logPath+"/")+"storage-benchmark.log", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	return log2.New(file, "storage-benchmark", log2.Ldate+log2.Ltime+log2.Lshortfile+log2.Lmsgprefix)
}

func createBenchmarkBucket() {
	fmt.Print("\n--- SETUP --------------------------------------------------------------------------------------------------------------------\n\n")

	_, err := ctx.Client.CreateBucket(ctx.Path)

	fmt.Printf("Created target path %s\n\n", getTargetPath())

	// if the error is because the bucket already exists, ignore the error
	if err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou:") {
		panic("Failed to create bucket: " + err.Error())
	}
}

func runBenchmark() {

	fmt.Print("\n--- BENCHMARK ---------------------------------------------------------------------------------------------------------------------------------------------------\n\n")

	// Init the final report
	ctx.Report = sbmark.Report{
		ClientEnv:   fmt.Sprintf("Application: %s, Host: %s, OS: %s", filepath.Base(os.Args[0]), getHostname(), runtime.GOOS),
		ServerEnv:   ctx.Endpoint, // How can we get some informations about the ServerEnv? Or should this be a CLI param?
		DateTimeUTC: time.Now().UTC().String(),
		Records:     []sbmark.Record{},
	}

	// an object size iterator that starts from 1 KB and doubles the size on every iteration
	generatePayload := payloadSizeGenerator()

	// loop over every payload size (we need to start at p := 1 because of the generatePayload() function)
	for p := 1; p <= ctx.PayloadsMax; p++ {
		// get an object size from the iterator
		payloadSize := generatePayload()

		// ignore payloads smaller than the min argument
		if p < ctx.PayloadsMin {
			continue
		}

		fmt.Printf("Preparing benchmark for %s objects\n", sbmark.ByteFormat(float64(payloadSize)))
		uploadBar := progressbar.NewOptions(ctx.Samples-1, progressbar.OptionSetRenderBlankState(true))
		ctx.Operation.EnsureTestdata(ctx, payloadSize, uploadBar)

		fmt.Printf("\n\n")

		ctx.Mode.PrintHeader(payloadSize, ctx.OperationName)

		ctx.Mode.ExecuteBenchmark(ctx, payloadSize)

		ctx.Mode.PrintFooter()

		fmt.Printf("Deleting %d x %s objects\n", ctx.NumberOfObjectsPerPayload(), sbmark.ByteFormat(float64(payloadSize)))
		cleanupBar := progressbar.NewOptions(ctx.Samples-1, progressbar.OptionSetRenderBlankState(true))
		ctx.Operation.CleanupTestdata(ctx, cleanupBar)

		fmt.Printf("\n\n\n\n")
	}

	// if the csv option is set, save the report as .csv
	if csvFileName != "" {
		csvReport, err := sbmark.ToCsv(ctx.Report)
		if err != nil {
			panic("Failed to create .csv output: " + err.Error())
		}
		err = os.WriteFile(csvFileName, csvReport, 0644)
		if err != nil {
			panic("Failed to create .csv output: " + err.Error())
		}
		fmt.Printf("CSV results were written to %s\n", csvFileName)
	}

	// if the json option is set, save the report as .json
	if jsonFileName != "" {
		jsonReport, err := sbmark.ToJson(ctx)
		if err != nil {
			panic("Failed to create .json output: " + err.Error())
		}
		err = os.WriteFile(jsonFileName, jsonReport, 0644)
		if err != nil {
			panic("Failed to create .json output: " + err.Error())
		}
		fmt.Printf("JSON results were written to %s\n", jsonFileName)
	}
}

// gets the name of the host that executes the test.
func getHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return hostname
}

func getTargetPath() string {
	return fmt.Sprintf("%s/%s", ctx.Endpoint, ctx.Path)
}

// returns an object size iterator, starting from 1 KB and double in size by each iteration
func payloadSizeGenerator() func() uint64 {
	nextPayloadSize := uint64(1024)

	return func() uint64 {
		thisPayloadSize := nextPayloadSize
		nextPayloadSize *= 2
		return thisPayloadSize
	}
}
