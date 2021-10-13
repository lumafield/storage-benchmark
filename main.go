package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	log2 "log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/iternity-dotcom/storage-benchmark/sbmark"
	"github.com/schollz/progressbar/v2"
)

// represents the duration from making an operation to getting the first byte and last byte
type latency struct {
	FirstByte time.Duration
	LastByte  time.Duration
}

// default settings
const bucketNamePrefix = "storage-benchmark"
const samplesMin = 4 // to calculate the 0.25 percentile we need at least 4 samples of each run

// the hostname where this benchmark is executed from
var hostname = getHostname()

// the region if available.
var region string

// the endpoint URL or path if applicable
var endpoint string

// the script will automatically create a bucket to use for the test, and it tries to get a unique bucket name
// by generating a sha hash of the hostname
var bucketName = fmt.Sprintf("%s-%x", bucketNamePrefix, sha1.Sum([]byte(hostname)))

// the min and max object sizes to test - 1 = 1 KB, and the size doubles with every increment
var payloadsMin int
var payloadsMax int

// the min and max thread count to use in the test
var threadsMin int
var threadsMax int

// the number of samples to collect for each benchmark record
var samples int

// a test mode to find out when EC2 network throttling kicks in
var throttlingMode bool

// flag to cleanup the bucket and exit the program
var cleanupOnly bool

// if not empty, the results of the test get uploaded using this key prefix
var csvResults string

// the client to operate on objects
var client sbmark.BenchmarkAPI

// operations might be "read" or "write. Default is "read".
var operationToTest string

var createBucket bool

// path for log file
var logPath string

// add a logging
var logger *log2.Logger

// flag for a load test
var infiniteMode bool

// The final report of this benchmark run
var report sbmark.Report

// program entry point
func main() {
	// parse the program arguments and set the global variables
	parseFlags()

	//
	setupLogger()

	// set up the client SDK
	setupClient()

	// if given the flag to cleanup only, then run the cleanup and exit the program
	if cleanupOnly {
		cleanup()
		return
	}

	if createBucket {
		// create the bucket
		createBenchmarkBucket()
	}

	// upload the test data (if needed)
	uploadObjects()

	// run the test against the uploaded data
	runBenchmark()

	// remove the objects uploaded for this test (but doesn't remove the bucket)
	cleanup()
}

func parseFlags() {
	threadsMinArg := flag.Int("threads-min", 8, "The minimum number of threads to use when fetching objects.")
	threadsMaxArg := flag.Int("threads-max", 16, "The maximum number of threads to use when fetching objects.")
	payloadsMinArg := flag.Int("payloads-min", 1, "The minimum object size to test, with 1 = 1 KB, and every increment is a double of the previous value.")
	payloadsMaxArg := flag.Int("payloads-max", 10, "The maximum object size to test, with 1 = 1 KB, and every increment is a double of the previous value.")
	samplesArg := flag.Int("samples", 50, "The number of samples to collect for each test of a single object size and thread count. Default is 50. Minimum value is 4.")
	bucketNameArg := flag.String("bucket-name", "", "Cleans up all artifacts used by the benchmarks.")
	regionArg := flag.String("region", "", "Sets the AWS region to use for the S3 bucket. Only applies if the bucket doesn't already exist.")
	endpointArg := flag.String("endpoint", "", "Sets the endpoint to use. Might be any URI.")
	fullArg := flag.Bool("full", false, "Runs the full exhaustive test, and overrides the threads and payload arguments.")
	throttlingModeArg := flag.Bool("throttling-mode", false, "Runs a continuous test to find out when EC2 network throttling kicks in.")
	cleanupArg := flag.Bool("cleanup", false, "Cleans all the objects uploaded for this test.")
	csvResultsArg := flag.String("upload-csv", "", "Uploads the test results as a CSV file.")
	operationArg := flag.String("operation", "read", "Specify if you want to measure 'read' or 'write'. Default is 'read'")
	createBucketArg := flag.Bool("create-bucket", false, "create new bucket(default false)")
	logPathArg := flag.String("log-path", "", "Specify the path of the log file. Default is 'currentDir'")
	infiniteModeArg := flag.Bool("infinite-mode", false, "Run the tests in a infinite loop. 'Read' or 'Write' depends on the value of operation flag. Default is 'false'")

	// parse the arguments and set all the global variables accordingly
	flag.Parse()

	if *bucketNameArg != "" {
		bucketName = *bucketNameArg
	}

	if *regionArg != "" {
		region = *regionArg
	}

	if *endpointArg != "" {
		endpoint = *endpointArg
	}

	payloadsMin = *payloadsMinArg
	payloadsMax = *payloadsMaxArg
	threadsMin = *threadsMinArg
	threadsMax = *threadsMaxArg
	samples = maxOf(*samplesArg, samplesMin)
	cleanupOnly = *cleanupArg
	csvResults = *csvResultsArg
	createBucket = *createBucketArg

	if *logPathArg == "" {
		logPath, _ = os.Getwd()
	} else {
		logPath = *logPathArg
	}

	if payloadsMin > payloadsMax {
		payloadsMin = payloadsMax
	}

	if threadsMin > threadsMax {
		threadsMin = threadsMax
	}

	if *fullArg {
		// if running the full exhaustive test, the threads and payload arguments get overridden with these
		threadsMin = 1
		threadsMax = 48
		payloadsMin = 1  //  1 KB
		payloadsMax = 16 // 32 MB
	}

	if *throttlingModeArg {
		// if running the network throttling test, the threads and payload arguments get overridden with these
		threadsMin = 36
		threadsMax = 36
		payloadsMin = 15 // 16 MB
		payloadsMax = 15 // 16 MB
		throttlingMode = *throttlingModeArg
	}

	if *infiniteModeArg {
		if payloadsMax != payloadsMin {
			fmt.Println("paylaod-min and paylaods-max must to be equal")
			os.Exit(-1)
		}
		if threadsMin != threadsMax {
			fmt.Println("threads-min and threads-max must be equal")
			os.Exit(-1)
		}
		infiniteMode = *infiniteModeArg
	}

	if *operationArg != "" {
		if *operationArg != "read" && *operationArg != "write" {
			panic("Unknown operation '" + *operationArg + "'. Please use 'read' or 'write'")
		}
		operationToTest = *operationArg
	}
}

func maxOf(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func setupLogger() {
	file, _ := os.OpenFile(filepath.FromSlash(logPath+"/")+"storage-benchmark.log", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	logger = log2.New(file, "storage-benchmark", log2.Ldate+log2.Ltime+log2.Lshortfile+log2.Lmsgprefix)
}

func setupClient() {
	if !strings.HasPrefix(strings.ToLower(endpoint), "http") {
		client = sbmark.NewFsClient(&sbmark.FsObjectClientConfig{
			RootPath: endpoint,
		})
	} else {
		client = sbmark.NewS3Client(&sbmark.S3ObjectClientConfig{
			Region:   region,
			Endpoint: endpoint,
			Insecure: true,
		})
	}
}

func createBenchmarkBucket() {
	fmt.Print("\n--- SETUP --------------------------------------------------------------------------------------------------------------------\n\n")

	err := client.CreateBucket(bucketName)

	// if the error is because the bucket already exists, ignore the error
	if err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou:") {
		panic("Failed to create bucket: " + err.Error())
	}
}

func uploadObjects() {
	// If "write" operation should be tested, we don't need to upload test data.
	if operationToTest == "write" {
		return
	}

	// an object size iterator that starts from 1 KB and doubles the size on every iteration
	generatePayload := payloadSizeGenerator()

	// loop over every payload size
	for p := 1; p <= payloadsMax; p++ {
		// get an object size from the iterator
		objectSize := generatePayload()

		// ignore payloads smaller than the min argument
		if p < payloadsMin {
			continue
		}

		fmt.Printf("Uploading %-s objects\n", byteFormat(float64(objectSize)))

		// create a progress bar
		bar := progressbar.NewOptions(threadsMax-1, progressbar.OptionSetRenderBlankState(true))

		// create an object for every thread, so that different threads don't download the same object
		for t := 1; t <= threadsMax; t++ {
			// increment the progress bar for each object
			_ = bar.Add(1)

			// generate an object key from the sha hash of the hostname, thread index, and object size
			key := generateObjectKey(hostname, t, objectSize)

			// do a HeadObject request to avoid uploading the object if it already exists from a previous test run
			err := client.HeadObject(bucketName, key)

			// if no error, then the object exists, so skip this one
			if err == nil {
				continue
			}

			// if other error, exit
			if err != nil && !strings.Contains(err.Error(), "NotFound:") {
				panic("Failed to head object: " + err.Error())
			}

			// generate empty payload
			payload := make([]byte, objectSize)

			// do a PutObject request to create the object
			err = client.PutObject(bucketName, key, bytes.NewReader(payload))

			// if the put fails, exit
			if err != nil {
				panic("Failed to put object: " + err.Error())
			}
		}

		fmt.Print("\n")
	}
}

func runBenchmark() {
	fmt.Print("\n--- BENCHMARK ----------------------------------------------------------------------------------------------------------------\n\n")

	// Init the final report
	report = sbmark.Report{
		Endpoint:    endpoint,
		Path:        bucketName,
		ClientEnv:   fmt.Sprintf("Application: %s, Host: %s, OS: %s", filepath.Base(os.Args[0]), getHostname(), runtime.GOOS),
		ServerEnv:   endpoint,
		DateTimeUTC: time.Now().UTC().Local().String(),
		Records:     []sbmark.Record{},
	}

	// an object size iterator that starts from 1 KB and doubles the size on every iteration
	generatePayload := payloadSizeGenerator()
	writeRunNumber := 0

	// loop over every payload size
	for p := 1; p <= payloadsMax; p++ {
		// get an object size from the iterator
		payload := generatePayload()

		// ignore payloads smaller than the min argument
		if p < payloadsMin {
			continue
		}

		// print the header for the benchmark of this object size
		printHeader(payload)

		// run a test per thread count and object size combination
		for t := threadsMin; t <= threadsMax; t++ {
			if operationToTest == "write" {
				// must change run id to prevent collisions
				for true {
					writeRunNumber++
					execTest(t, payload, writeRunNumber)
					if !infiniteMode {
						break
					}
				}
			} else {
				// if throttling mode or infinite mode, loop forever
				for n := 1; true; n++ {
					execTest(t, payload, n)
					if !throttlingMode && !infiniteMode {
						break
					}
				}
			}
		}
		fmt.Print("+---------+----------------+------------------------------------------------+------------------------------------------------+\n\n")
	}

	// if the csv option is true, upload the csv results
	if csvResults != "" {

		// array of csv records used to upload the results
		var csvRecords [][]string

		for _, record := range report.Records {
			// add the results to the csv array
			csvRecords = append(csvRecords, []string{
				report.ClientEnv,
				report.ServerEnv,
				fmt.Sprintf("%d", record.ObjectSizeBytes),
				fmt.Sprintf("%d", record.Threads),
				fmt.Sprintf("%.3f", record.ThroughputMBps()),
				fmt.Sprintf("%.1f", record.TimeToFirstByte["avg"]),
				fmt.Sprintf("%.1f", record.TimeToFirstByte["min"]),
				fmt.Sprintf("%.1f", record.TimeToFirstByte["p25"]),
				fmt.Sprintf("%.1f", record.TimeToFirstByte["p50"]),
				fmt.Sprintf("%.1f", record.TimeToFirstByte["p75"]),
				fmt.Sprintf("%.1f", record.TimeToFirstByte["p90"]),
				fmt.Sprintf("%.1f", record.TimeToFirstByte["p99"]),
				fmt.Sprintf("%.1f", record.TimeToFirstByte["max"]),
				fmt.Sprintf("%.2f", record.TimeToLastByte["avg"]),
				fmt.Sprintf("%.2f", record.TimeToLastByte["min"]),
				fmt.Sprintf("%.1f", record.TimeToLastByte["p25"]),
				fmt.Sprintf("%.1f", record.TimeToLastByte["p50"]),
				fmt.Sprintf("%.1f", record.TimeToLastByte["p75"]),
				fmt.Sprintf("%.1f", record.TimeToLastByte["p90"]),
				fmt.Sprintf("%.1f", record.TimeToLastByte["p99"]),
				fmt.Sprintf("%.1f", record.TimeToLastByte["max"]),
			})
		}

		b := &bytes.Buffer{}
		w := csv.NewWriter(b)
		_ = w.WriteAll(csvRecords)

		// create the object key based on the prefix argument and instance type
		key := "results/" + csvResults

		// do the PutObject request
		err := client.PutObject(bucketName, key, bytes.NewReader(b.Bytes()))

		// if the request fails, exit
		if err != nil {
			panic("Failed to put object: " + err.Error())
		}

		fmt.Printf("CSV results uploaded to %s/%s/%s\n", endpoint, bucketName, key)
	}
}

func execTest(threadCount int, payloadSize uint64, runNumber int) {
	// a channel to submit the test tasks
	testTasks := make(chan int, threadCount)

	// a channel to receive results from the test tasks back on the main thread
	results := make(chan latency, samples)

	// create the workers for all the threads in this test
	for w := 1; w <= threadCount; w++ {
		go func(o int, tasks <-chan int, results chan<- latency) {
			for range tasks {
				var firstByte, lastByte time.Duration
				if operationToTest == "write" {
					// generate an object key from the sha hash of the current run id, thread index, and object size
					key := generateObjectKey(string(runNumber), o, payloadSize)
					firstByte, lastByte = measureWritePerformanceForSingleObject(key, payloadSize)
				} else {
					// generate an object key from the sha hash of the hostname, thread index, and object size
					key := generateObjectKey(hostname, o, payloadSize)
					firstByte, lastByte = measureReadPerformanceForSingleObject(key, payloadSize)
				}

				// add the latency result to the results channel
				results <- latency{FirstByte: firstByte, LastByte: lastByte}
			}
		}(w, testTasks, results)
	}

	// construct a new benchmark record
	dataPoints := []latency{}
	sumFirstByte := int64(0)
	sumLastByte := int64(0)

	record := sbmark.Record{
		Operation:       operationToTest,
		Threads:         threadCount,
		TimeToFirstByte: make(map[string]float64),
		TimeToLastByte:  make(map[string]float64),
	}

	// start the timer for this benchmark
	benchmarkTimer := time.Now()

	// submit all the test tasks
	for j := 1; j <= samples; j++ {
		testTasks <- j
	}

	// close the channel
	close(testTasks)

	// wait for all the results to come and collect the individual datapoints
	for s := 1; s <= samples; s++ {
		timing := <-results
		dataPoints = append(dataPoints, timing)
		sumFirstByte += timing.FirstByte.Nanoseconds()
		sumLastByte += timing.LastByte.Nanoseconds()
		record.ObjectSizeBytes += payloadSize
	}

	// stop the timer for this benchmark
	totalTime := time.Now().Sub(benchmarkTimer)
	record.DurationSeconds = totalTime.Seconds()

	// calculate the summary statistics for the first byte latencies
	sort.Sort(ByFirstByte(dataPoints))
	record.TimeToFirstByte["avg"] = (float64(sumFirstByte) / float64(samples)) / 1000000
	record.TimeToFirstByte["min"] = float64(dataPoints[0].FirstByte.Nanoseconds()) / 1000000
	record.TimeToFirstByte["max"] = float64(dataPoints[len(dataPoints)-1].FirstByte.Nanoseconds()) / 1000000
	record.TimeToFirstByte["p25"] = float64(dataPoints[int(float64(samples)*float64(0.25))-1].FirstByte.Nanoseconds()) / 1000000
	record.TimeToFirstByte["p50"] = float64(dataPoints[int(float64(samples)*float64(0.5))-1].FirstByte.Nanoseconds()) / 1000000
	record.TimeToFirstByte["p75"] = float64(dataPoints[int(float64(samples)*float64(0.75))-1].FirstByte.Nanoseconds()) / 1000000
	record.TimeToFirstByte["p90"] = float64(dataPoints[int(float64(samples)*float64(0.90))-1].FirstByte.Nanoseconds()) / 1000000
	record.TimeToFirstByte["p99"] = float64(dataPoints[int(float64(samples)*float64(0.99))-1].FirstByte.Nanoseconds()) / 1000000

	// calculate the summary statistics for the last byte latencies
	sort.Sort(ByLastByte(dataPoints))
	record.TimeToLastByte["avg"] = (float64(sumLastByte) / float64(samples)) / 1000000
	record.TimeToLastByte["min"] = float64(dataPoints[0].LastByte.Nanoseconds()) / 1000000
	record.TimeToLastByte["max"] = float64(dataPoints[len(dataPoints)-1].LastByte.Nanoseconds()) / 1000000
	record.TimeToLastByte["p25"] = float64(dataPoints[int(float64(samples)*float64(0.25))-1].LastByte.Nanoseconds()) / 1000000
	record.TimeToLastByte["p50"] = float64(dataPoints[int(float64(samples)*float64(0.5))-1].LastByte.Nanoseconds()) / 1000000
	record.TimeToLastByte["p75"] = float64(dataPoints[int(float64(samples)*float64(0.75))-1].LastByte.Nanoseconds()) / 1000000
	record.TimeToLastByte["p90"] = float64(dataPoints[int(float64(samples)*float64(0.90))-1].LastByte.Nanoseconds()) / 1000000
	record.TimeToLastByte["p99"] = float64(dataPoints[int(float64(samples)*float64(0.99))-1].LastByte.Nanoseconds()) / 1000000

	// determine what to put in the first column of the results
	c := record.Threads
	if throttlingMode {
		c = runNumber
	}

	// print the results to stdout
	fmt.Printf("| %7d | %9.3f MB/s |%5.0f %5.0f %5.0f %5.0f %5.0f %5.0f %5.0f %5.0f |%5.0f %5.0f %5.0f %5.0f %5.0f %5.0f %5.0f %5.0f |\n",
		c, record.ThroughputMBps(),
		record.TimeToFirstByte["avg"], record.TimeToFirstByte["min"], record.TimeToFirstByte["p25"], record.TimeToFirstByte["p50"], record.TimeToFirstByte["p75"], record.TimeToFirstByte["p90"], record.TimeToFirstByte["p99"], record.TimeToFirstByte["max"],
		record.TimeToLastByte["avg"], record.TimeToLastByte["min"], record.TimeToLastByte["p25"], record.TimeToLastByte["p50"], record.TimeToLastByte["p75"], record.TimeToLastByte["p90"], record.TimeToLastByte["p99"], record.TimeToLastByte["max"])

	// append the record to the report
	report.Records = append(report.Records, record)
}

func measureReadPerformanceForSingleObject(key string, payloadSize uint64) (firstByte time.Duration, lastByte time.Duration) {
	// start the timer to measure the first byte and last byte latencies
	latencyTimer := time.Now()
	// do the GetObject request
	dataStream, err := client.GetObject(bucketName, key)
	// if a request fails, exit
	if err != nil {
		panic("Failed to get object: " + err.Error())
	}
	// measure the first byte latency
	firstByte = time.Now().Sub(latencyTimer)
	// create a buffer to copy the object body to
	var buf = make([]byte, payloadSize)
	// read the object body into the buffer
	size := 0
	for {
		n, err := dataStream.Read(buf)

		size += n

		if err == io.EOF {
			break
		}

		// if the streaming fails, exit
		if err != nil {
			panic("Error reading object body: " + err.Error())
		}
	}
	_ = dataStream.Close()
	// measure the last byte latency
	lastByte = time.Now().Sub(latencyTimer)
	return firstByte, lastByte
}

func measureWritePerformanceForSingleObject(key string, payloadSize uint64) (firstByte time.Duration, lastByte time.Duration) {
	i := 0
	for true {
		// generate empty payload
		payload := make([]byte, payloadSize)
		if i > 0 {
			key = generateObjectKey(string(time.Now().Nanosecond()), rand.Intn(1000), payloadSize)
			logger.Println(" Info: Generate new key with value '" + key + "'")
		}
		// start the timer to measure the first byte and last byte latencies
		latencyTimer := time.Now()

		// measure the first byte latency
		firstByte = time.Now().Sub(latencyTimer)

		// do a PutObject request to create the object
		err := client.PutObject(bucketName, key, bytes.NewReader(payload))

		// measure the last byte latency
		lastByte = time.Now().Sub(latencyTimer)
		switch err {
		case nil:
			return
		default:
			logger.Println(" Error during request:" + err.Error())
			i++
		}
	}
	return firstByte, lastByte
}

// prints the table header for the test results
func printHeader(objectSize uint64) {
	// print the table header
	testTitle := "Read performance from"
	if operationToTest == "write" {
		testTitle = "Write performance to"
	}
	fmt.Printf("%s %s with %-s objects\n", testTitle, endpoint, byteFormat(float64(objectSize)))
	fmt.Println("                           +-------------------------------------------------------------------------------------------------+")
	fmt.Println("                           |            Time to First Byte (ms)             |            Time to Last Byte (ms)              |")
	fmt.Println("+---------+----------------+------------------------------------------------+------------------------------------------------+")
	if !throttlingMode {
		fmt.Println("| Threads |     Throughput |  avg   min   p25   p50   p75   p90   p99   max |  avg   min   p25   p50   p75   p90   p99   max |")
	} else {
		fmt.Println("|       # |     Throughput |  avg   min   p25   p50   p75   p90   p99   max |  avg   min   p25   p50   p75   p90   p99   max |")
	}
	fmt.Println("+---------+----------------+------------------------------------------------+------------------------------------------------+")
}

// generates an object key from the sha hash of the hostname, thread index, and object size
func generateObjectKey(host string, threadIndex int, payloadSize uint64) string {
	var key string
	keyHash := sha1.Sum([]byte(fmt.Sprintf("%s-%03d-%012d", host, threadIndex, payloadSize)))
	folder := strconv.Itoa(int(payloadSize))
	if operationToTest == "write" && infiniteMode {
		key = folder +
			"/" + generateRandomString(threadIndex) +
			"/" + generateRandomString(threadIndex) +
			"/" + (fmt.Sprintf("%x", keyHash))
	} else {
		key = folder + "/" + (fmt.Sprintf("%x", keyHash))
	}
	return key
}

func generateRandomString(seed int) string {
	rand.Seed(time.Now().UnixNano())
	chars := []rune("1234")
	length := 4
	var b strings.Builder
	for i := 0; i < length; i++ {
		b.WriteRune(chars[rand.Intn(len(chars))])
	}
	return b.String()
}

// cleans up the uploaded objects for this test (but doesn't remove the bucket)
func cleanup() {
	fmt.Print("\n--- CLEANUP ------------------------------------------------------------------------------------------------------------------\n\n")

	fmt.Printf("Deleting any objects uploaded from %s to %s\n", hostname, endpoint)

	// create a progress bar
	bar := progressbar.NewOptions(payloadsMax*threadsMax-1, progressbar.OptionSetRenderBlankState(true))

	// an object size iterator that starts from 1 KB and doubles the size on every iteration
	generatePayload := payloadSizeGenerator()

	runNumber := 0

	// loop over every payload size
	for p := 1; p <= payloadsMax; p++ {
		// get an object size from the iterator
		payloadSize := generatePayload()

		// loop over each possible thread to clean up objects from any previous test execution
		for t := 1; t <= threadsMax; t++ {
			// increment the progress bar
			_ = bar.Add(1)
			runNumber++
			if operationToTest == "write" {
				for n := 1; n <= t; n++ {
					deleteSingleObject(string(n), t, payloadSize)
				}
			} else {
				deleteSingleObject(hostname, t, payloadSize)
			}
		}
	}
	fmt.Print("\n\n")
}

func deleteSingleObject(runNumber string, threadIdx int, payloadSize uint64) {
	// generate an object key from the sha hash of the hostname, thread index, and object size
	key := generateObjectKey(runNumber, threadIdx, payloadSize)

	// make a DeleteObject request
	err := client.DeleteObject(bucketName, key)

	// if the object doesn't exist, ignore the error
	if err != nil && !strings.HasPrefix(err.Error(), "NotFound: Not Found") {
		panic("Failed to delete object: " + err.Error())
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

// formats bytes to KB or MB
func byteFormat(bytes float64) string {
	if bytes >= 1024*1024 {
		return fmt.Sprintf("%.f MB", bytes/1024/1024)
	}
	return fmt.Sprintf("%.f KB", bytes/1024)
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

// comparator to sort by first byte latency
type ByFirstByte []latency

func (a ByFirstByte) Len() int           { return len(a) }
func (a ByFirstByte) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByFirstByte) Less(i, j int) bool { return a[i].FirstByte < a[j].FirstByte }

// comparator to sort by last byte latency
type ByLastByte []latency

func (a ByLastByte) Len() int           { return len(a) }
func (a ByLastByte) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByLastByte) Less(i, j int) bool { return a[i].LastByte < a[j].LastByte }
