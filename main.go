package main

import (
	"crypto/sha1"
	"flag"
	"fmt"
	log2 "log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/iternity-dotcom/storage-benchmark/sbmark"
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

// add a logging
var logger *log2.Logger

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
	setupLogger()
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
		Mode:          &sbmark.LatencyMode{},
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

func setupLogger() {
	file, _ := os.OpenFile(filepath.FromSlash(logPath+"/")+"storage-benchmark.log", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	logger = log2.New(file, "storage-benchmark", log2.Ldate+log2.Ltime+log2.Lshortfile+log2.Lmsgprefix)
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

		// Prepare the payload for the following tests
		ctx.Operation.EnsureTestdata(ctx, payloadSize)

		// print the header for the benchmark of this object size
		ctx.Mode.PrintHeader(payloadSize, ctx.OperationName)

		// run a test per thread count and object size combination
		for t := ctx.ThreadsMin; t <= ctx.ThreadsMax; t++ {
			for {
				ctx.NumberOfRuns++
				execTest(t, payloadSize, ctx.NumberOfRuns)
				if ctx.Mode.IsFinished(ctx.NumberOfRuns) {
					break
				}
			}
		}

		fmt.Print("+---------+----------------+------------------------------------------------+------------------------------------------------+----------------------------------+\n\n")

		fmt.Printf("Deleting %d x %s objects\n", ctx.NumberOfObjectsPerPayload(), sbmark.ByteFormat(float64(payloadSize)))
		//cleanupBar := progressbar.NewOptions(ctx.Samples-1, progressbar.OptionSetRenderBlankState(true))
		//cleanupObjects(payload, cleanupBar)
		ctx.Operation.CleanupTestdata(ctx)

		fmt.Printf("\n\n")
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

func execTest(threadCount int, payloadSize uint64, runId int) {
	// a channel to submit the test tasks
	testTasks := make(chan int, threadCount)

	// a channel to receive results from the test tasks back on the main thread
	results := make(chan sbmark.Latency, ctx.Samples)

	// create the workers for all the threads in this test
	for t := 1; t <= threadCount; t++ {
		go func(threadId int, tasks <-chan int, results chan<- sbmark.Latency) {
			for sampleId := range tasks {
				// execute operation and add the latency result to the results channel
				results <- ctx.Operation.Execute(ctx, sampleId, payloadSize)
			}
		}(t, testTasks, results)
	}

	// construct a new benchmark record
	dataPoints := []sbmark.Latency{}
	sumFirstByte := int64(0)
	sumLastByte := int64(0)
	sumDNSLookup := int64(0)
	sumTCPConnection := int64(0)
	sumTLSHandshake := int64(0)
	sumServerProcessing := int64(0)
	sumUnassigned := int64(0)

	record := sbmark.Record{
		ObjectSizeBytes:  0,
		Operation:        ctx.OperationName,
		Threads:          threadCount,
		TimeToFirstByte:  make(map[string]float64),
		TimeToLastByte:   make(map[string]float64),
		DNSLookup:        make(map[string]float64),
		TCPConnection:    make(map[string]float64),
		TLSHandshake:     make(map[string]float64),
		ServerProcessing: make(map[string]float64),
		Unassigned:       make(map[string]float64),
	}

	// start the timer for this benchmark
	benchmarkTimer := time.Now()

	// submit all the test tasks
	for s := 1; s <= ctx.Samples; s++ {
		testTasks <- s
	}

	// close the channel
	close(testTasks)

	// wait for all the results to come and collect the individual datapoints
	for s := 1; s <= ctx.Samples; s++ {
		timing := <-results
		dataPoints = append(dataPoints, timing)
		sumFirstByte += timing.FirstByte.Nanoseconds()
		sumLastByte += timing.LastByte.Nanoseconds()
		sumDNSLookup += timing.DNSLookup.Nanoseconds()
		sumTCPConnection += timing.TCPConnection.Nanoseconds()
		sumTLSHandshake += timing.TLSHandshake.Nanoseconds()
		sumServerProcessing += timing.ServerProcessing.Nanoseconds()
		sumUnassigned += timing.Unassigned().Nanoseconds()
		record.ObjectSizeBytes += payloadSize
	}

	// stop the timer for this benchmark
	totalTime := time.Since(benchmarkTimer)
	record.DurationSeconds = totalTime.Seconds()

	// calculate the summary statistics for the first byte latencies
	sort.Sort(sbmark.ByFirstByte(dataPoints))
	record.TimeToFirstByte["avg"] = (float64(sumFirstByte) / float64(ctx.Samples)) / 1000000
	record.TimeToFirstByte["min"] = float64(dataPoints[0].FirstByte.Nanoseconds()) / 1000000
	record.TimeToFirstByte["max"] = float64(dataPoints[len(dataPoints)-1].FirstByte.Nanoseconds()) / 1000000
	record.TimeToFirstByte["p25"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.25))-1].FirstByte.Nanoseconds()) / 1000000
	record.TimeToFirstByte["p50"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.5))-1].FirstByte.Nanoseconds()) / 1000000
	record.TimeToFirstByte["p75"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.75))-1].FirstByte.Nanoseconds()) / 1000000
	record.TimeToFirstByte["p90"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.90))-1].FirstByte.Nanoseconds()) / 1000000
	record.TimeToFirstByte["p99"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.99))-1].FirstByte.Nanoseconds()) / 1000000

	// calculate the summary statistics for the last byte latencies
	sort.Sort(sbmark.ByLastByte(dataPoints))
	record.TimeToLastByte["avg"] = (float64(sumLastByte) / float64(ctx.Samples)) / 1000000
	record.TimeToLastByte["min"] = float64(dataPoints[0].LastByte.Nanoseconds()) / 1000000
	record.TimeToLastByte["max"] = float64(dataPoints[len(dataPoints)-1].LastByte.Nanoseconds()) / 1000000
	record.TimeToLastByte["p25"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.25))-1].LastByte.Nanoseconds()) / 1000000
	record.TimeToLastByte["p50"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.5))-1].LastByte.Nanoseconds()) / 1000000
	record.TimeToLastByte["p75"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.75))-1].LastByte.Nanoseconds()) / 1000000
	record.TimeToLastByte["p90"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.90))-1].LastByte.Nanoseconds()) / 1000000
	record.TimeToLastByte["p99"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.99))-1].LastByte.Nanoseconds()) / 1000000

	// calculate the summary statistics for the DNS lookup latencies
	sort.Sort(sbmark.ByDNSLookup(dataPoints))
	record.DNSLookup["avg"] = (float64(sumDNSLookup) / float64(ctx.Samples)) / 1000000
	record.DNSLookup["min"] = float64(dataPoints[0].DNSLookup.Nanoseconds()) / 1000000
	record.DNSLookup["max"] = float64(dataPoints[len(dataPoints)-1].DNSLookup.Nanoseconds()) / 1000000
	record.DNSLookup["p25"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.25))-1].DNSLookup.Nanoseconds()) / 1000000
	record.DNSLookup["p50"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.5))-1].DNSLookup.Nanoseconds()) / 1000000
	record.DNSLookup["p75"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.75))-1].DNSLookup.Nanoseconds()) / 1000000
	record.DNSLookup["p90"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.90))-1].DNSLookup.Nanoseconds()) / 1000000
	record.DNSLookup["p99"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.99))-1].DNSLookup.Nanoseconds()) / 1000000

	// calculate the summary statistics for the TCP connection latencies
	sort.Sort(sbmark.ByTCPConnection(dataPoints))
	record.TCPConnection["avg"] = (float64(sumTCPConnection) / float64(ctx.Samples)) / 1000000
	record.TCPConnection["min"] = float64(dataPoints[0].TCPConnection.Nanoseconds()) / 1000000
	record.TCPConnection["max"] = float64(dataPoints[len(dataPoints)-1].TCPConnection.Nanoseconds()) / 1000000
	record.TCPConnection["p25"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.25))-1].TCPConnection.Nanoseconds()) / 1000000
	record.TCPConnection["p50"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.5))-1].TCPConnection.Nanoseconds()) / 1000000
	record.TCPConnection["p75"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.75))-1].TCPConnection.Nanoseconds()) / 1000000
	record.TCPConnection["p90"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.90))-1].TCPConnection.Nanoseconds()) / 1000000
	record.TCPConnection["p99"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.99))-1].TCPConnection.Nanoseconds()) / 1000000

	// calculate the summary statistics for the TLS handshake latencies
	sort.Sort(sbmark.ByTLSHandshake(dataPoints))
	record.TLSHandshake["avg"] = (float64(sumTLSHandshake) / float64(ctx.Samples)) / 1000000
	record.TLSHandshake["min"] = float64(dataPoints[0].TLSHandshake.Nanoseconds()) / 1000000
	record.TLSHandshake["max"] = float64(dataPoints[len(dataPoints)-1].TLSHandshake.Nanoseconds()) / 1000000
	record.TLSHandshake["p25"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.25))-1].TLSHandshake.Nanoseconds()) / 1000000
	record.TLSHandshake["p50"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.5))-1].TLSHandshake.Nanoseconds()) / 1000000
	record.TLSHandshake["p75"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.75))-1].TLSHandshake.Nanoseconds()) / 1000000
	record.TLSHandshake["p90"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.90))-1].TLSHandshake.Nanoseconds()) / 1000000
	record.TLSHandshake["p99"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.99))-1].TLSHandshake.Nanoseconds()) / 1000000

	// calculate the summary statistics for the server processing latencies
	sort.Sort(sbmark.ByServerProcessing(dataPoints))
	record.ServerProcessing["avg"] = (float64(sumServerProcessing) / float64(ctx.Samples)) / 1000000
	record.ServerProcessing["min"] = float64(dataPoints[0].ServerProcessing.Nanoseconds()) / 1000000
	record.ServerProcessing["max"] = float64(dataPoints[len(dataPoints)-1].ServerProcessing.Nanoseconds()) / 1000000
	record.ServerProcessing["p25"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.25))-1].ServerProcessing.Nanoseconds()) / 1000000
	record.ServerProcessing["p50"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.5))-1].ServerProcessing.Nanoseconds()) / 1000000
	record.ServerProcessing["p75"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.75))-1].ServerProcessing.Nanoseconds()) / 1000000
	record.ServerProcessing["p90"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.90))-1].ServerProcessing.Nanoseconds()) / 1000000
	record.ServerProcessing["p99"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.99))-1].ServerProcessing.Nanoseconds()) / 1000000

	// calculate the summary statistics for the unassigned latencies
	sort.Sort(sbmark.ByUnassigned(dataPoints))
	record.Unassigned["avg"] = (float64(sumUnassigned) / float64(ctx.Samples)) / 1000000
	record.Unassigned["min"] = float64(dataPoints[0].Unassigned().Nanoseconds()) / 1000000
	record.Unassigned["max"] = float64(dataPoints[len(dataPoints)-1].Unassigned().Nanoseconds()) / 1000000
	record.Unassigned["p25"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.25))-1].Unassigned().Nanoseconds()) / 1000000
	record.Unassigned["p50"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.5))-1].Unassigned().Nanoseconds()) / 1000000
	record.Unassigned["p75"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.75))-1].Unassigned().Nanoseconds()) / 1000000
	record.Unassigned["p90"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.90))-1].Unassigned().Nanoseconds()) / 1000000
	record.Unassigned["p99"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.99))-1].Unassigned().Nanoseconds()) / 1000000

	// determine what to put in the first column of the results
	c := record.Threads
	if burstMode {
		c = ctx.NumberOfRuns
	}

	// print the results to stdout
	fmt.Printf("| %7d | %9.3f MB/s |%5.0f %5.0f %5.0f %5.0f %5.0f %5.0f %5.0f %5.0f |%5.0f %5.0f %5.0f %5.0f %5.0f %5.0f %5.0f %5.0f |%7.0f %5.0f %5.0f %5.0f %6.0f  |\n",
		c, record.ThroughputMBps(),
		record.TimeToFirstByte["avg"], record.TimeToFirstByte["min"], record.TimeToFirstByte["p25"], record.TimeToFirstByte["p50"], record.TimeToFirstByte["p75"], record.TimeToFirstByte["p90"], record.TimeToFirstByte["p99"], record.TimeToFirstByte["max"],
		record.TimeToLastByte["avg"], record.TimeToLastByte["min"], record.TimeToLastByte["p25"], record.TimeToLastByte["p50"], record.TimeToLastByte["p75"], record.TimeToLastByte["p90"], record.TimeToLastByte["p99"], record.TimeToLastByte["max"],
		record.DNSLookup["avg"], record.TCPConnection["avg"], record.TLSHandshake["avg"], record.ServerProcessing["avg"], record.Unassigned["avg"])

	// append the record to the report
	ctx.Report.Records = append(ctx.Report.Records, record)
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
