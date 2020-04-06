package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/dvassallo/s3-benchmark/obmark"
	"io"
	"io/ioutil"
	log2 "log"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/schollz/progressbar/v2"
)

// represents the duration from making an GetObject/Read request to getting the first byte and last byte
type latency struct {
	FirstByte time.Duration
	LastByte  time.Duration
}

// summary statistics used to summarize first byte and last byte latencies
type stat int

const (
	min stat = iota + 1
	max
	avg
	p25
	p50
	p75
	p90
	p99
)

// a benchmark record for one object size and thread count
type benchmark struct {
	objectSize uint64
	threads    int
	firstByte  map[stat]float64
	lastByte   map[stat]float64
	dataPoints []latency
}

// absolute limits
const maxPayload = 18
const maxThreads = 64

// default settings
const defaultRegion = "us-west-2"
const bucketNamePrefix = "object-benchmark"

// the hostname or EC2 instance id
var hostname = getHostname()

// the EC2 instance region if available
var region = getRegion()

// the endpoint URL if applicable
var endpoint string

// the EC2 instance type if available
var instanceType = getInstanceType()

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
var client obmark.ObjectClient

// operations might be "read" or "write. Default is "read".
var operationToTest string

var createBucket bool

// path for log file
var logPath string

// add a logging
var logger *log2.Logger

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
	samplesArg := flag.Int("samples", 1000, "The number of samples to collect for each test of a single object size and thread count.")
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
	samples = *samplesArg
	cleanupOnly = *cleanupArg
	csvResults = *csvResultsArg
	createBucket = *createBucketArg

	if *logPathArg == "" {
		logPath,_ = os.Getwd()
	}else{
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

	if *operationArg != "" {
		if *operationArg != "read" && *operationArg != "write" {
			panic("Unknown operation '"+*operationArg +"'. Please use 'read' or 'write'")
		}
		operationToTest = *operationArg
	}
}

func setupLogger(){
	file, _ := os.OpenFile(filepath.FromSlash(logPath+"/") + "s3-benchmark.log", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	logger = log2.New(file,"s3-benchmark",log2.Ldate + log2.Ltime + log2.Lshortfile + log2.Lmsgprefix)
}

func setupClient() {
	if !strings.HasPrefix(strings.ToLower(endpoint), "http") {
		client = obmark.NewFsClient(&obmark.ObjectClientConfig{
			Region:   region,
			Endpoint: endpoint,		})
	} else {
		client = obmark.NewS3Client(&obmark.ObjectClientConfig{
			Region:   region,
			Endpoint: endpoint,
		})
	}
}

func createBenchmarkBucket() {
	fmt.Print("\n--- \033[1;32mSETUP\033[0m --------------------------------------------------------------------------------------------------------------------\n\n")

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

		fmt.Printf("Uploading \033[1;33m%-s\033[0m objects\n", byteFormat(float64(objectSize)))

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
	fmt.Print("\n--- \033[1;32mBENCHMARK\033[0m ----------------------------------------------------------------------------------------------------------------\n\n")

	// array of csv records used to upload the results when the test is finished
	var csvRecords [][]string

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
				writeRunNumber++
				csvRecords = execTest(t, payload, writeRunNumber, csvRecords)
			} else {
				// if throttling mode, loop forever
				for n := 1; true; n++ {
					csvRecords = execTest(t, payload, n, csvRecords)
					if !throttlingMode {
						break
					}
				}
			}
		}
		fmt.Print("+---------+----------------+------------------------------------------------+------------------------------------------------+\n\n")
	}

	// if the csv option is true, upload the csv results
	if csvResults != "" {
		b := &bytes.Buffer{}
		w := csv.NewWriter(b)
		_ = w.WriteAll(csvRecords)

		// create the object key based on the prefix argument and instance type
		key := "results/" + csvResults + "-" + instanceType

		// do the PutObject request
		err := client.PutObject(bucketName, key, bytes.NewReader(b.Bytes()))

		// if the request fails, exit
		if err != nil {
			panic("Failed to put object: " + err.Error())
		}

		fmt.Printf("CSV results uploaded to \033[1;33mthe store://%s/%s\033[0m\n", bucketName, key)
	}
}

func execTest(threadCount int, payloadSize uint64, runNumber int, csvRecords [][]string) [][]string {
	// this overrides the sample count on small hosts that can get overwhelmed by a large throughput
	samples := getTargetSampleCount(threadCount, samples)

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

	// start the timer for this benchmark
	benchmarkTimer := time.Now()

	// submit all the test tasks
	for j := 1; j <= samples; j++ {
		testTasks <- j
	}

	// close the channel
	close(testTasks)

	// construct a new benchmark record
	benchmarkRecord := benchmark{
		firstByte: make(map[stat]float64),
		lastByte:  make(map[stat]float64),
	}
	sumFirstByte := int64(0)
	sumLastByte := int64(0)
	benchmarkRecord.threads = threadCount

	// wait for all the results to come and collect the individual datapoints
	for s := 1; s <= samples; s++ {
		timing := <-results
		benchmarkRecord.dataPoints = append(benchmarkRecord.dataPoints, timing)
		sumFirstByte += timing.FirstByte.Nanoseconds()
		sumLastByte += timing.LastByte.Nanoseconds()
		benchmarkRecord.objectSize += payloadSize
	}

	// stop the timer for this benchmark
	totalTime := time.Now().Sub(benchmarkTimer)

	// calculate the summary statistics for the first byte latencies
	sort.Sort(ByFirstByte(benchmarkRecord.dataPoints))
	benchmarkRecord.firstByte[avg] = (float64(sumFirstByte) / float64(samples)) / 1000000
	benchmarkRecord.firstByte[min] = float64(benchmarkRecord.dataPoints[0].FirstByte.Nanoseconds()) / 1000000
	benchmarkRecord.firstByte[max] = float64(benchmarkRecord.dataPoints[len(benchmarkRecord.dataPoints)-1].FirstByte.Nanoseconds()) / 1000000
	benchmarkRecord.firstByte[p25] = float64(benchmarkRecord.dataPoints[int(float64(samples)*float64(0.25))-1].FirstByte.Nanoseconds()) / 1000000
	benchmarkRecord.firstByte[p50] = float64(benchmarkRecord.dataPoints[int(float64(samples)*float64(0.5))-1].FirstByte.Nanoseconds()) / 1000000
	benchmarkRecord.firstByte[p75] = float64(benchmarkRecord.dataPoints[int(float64(samples)*float64(0.75))-1].FirstByte.Nanoseconds()) / 1000000
	benchmarkRecord.firstByte[p90] = float64(benchmarkRecord.dataPoints[int(float64(samples)*float64(0.90))-1].FirstByte.Nanoseconds()) / 1000000
	benchmarkRecord.firstByte[p99] = float64(benchmarkRecord.dataPoints[int(float64(samples)*float64(0.99))-1].FirstByte.Nanoseconds()) / 1000000

	// calculate the summary statistics for the last byte latencies
	sort.Sort(ByLastByte(benchmarkRecord.dataPoints))
	benchmarkRecord.lastByte[avg] = (float64(sumLastByte) / float64(samples)) / 1000000
	benchmarkRecord.lastByte[min] = float64(benchmarkRecord.dataPoints[0].LastByte.Nanoseconds()) / 1000000
	benchmarkRecord.lastByte[max] = float64(benchmarkRecord.dataPoints[len(benchmarkRecord.dataPoints)-1].LastByte.Nanoseconds()) / 1000000
	benchmarkRecord.lastByte[p25] = float64(benchmarkRecord.dataPoints[int(float64(samples)*float64(0.25))-1].LastByte.Nanoseconds()) / 1000000
	benchmarkRecord.lastByte[p50] = float64(benchmarkRecord.dataPoints[int(float64(samples)*float64(0.5))-1].LastByte.Nanoseconds()) / 1000000
	benchmarkRecord.lastByte[p75] = float64(benchmarkRecord.dataPoints[int(float64(samples)*float64(0.75))-1].LastByte.Nanoseconds()) / 1000000
	benchmarkRecord.lastByte[p90] = float64(benchmarkRecord.dataPoints[int(float64(samples)*float64(0.90))-1].LastByte.Nanoseconds()) / 1000000
	benchmarkRecord.lastByte[p99] = float64(benchmarkRecord.dataPoints[int(float64(samples)*float64(0.99))-1].LastByte.Nanoseconds()) / 1000000

	// calculate the throughput rate
	rate := (float64(benchmarkRecord.objectSize)) / (totalTime.Seconds()) / 1024 / 1024

	// determine what to put in the first column of the results
	c := benchmarkRecord.threads
	if throttlingMode {
		c = runNumber
	}

	// print the results to stdout
	fmt.Printf("| %7d | \033[1;31m%9.1f MB/s\033[0m |%5.0f %5.0f %5.0f %5.0f %5.0f %5.0f %5.0f %5.0f |%5.0f %5.0f %5.0f %5.0f %5.0f %5.0f %5.0f %5.0f |\n",
		c, rate,
		benchmarkRecord.firstByte[avg], benchmarkRecord.firstByte[min], benchmarkRecord.firstByte[p25], benchmarkRecord.firstByte[p50], benchmarkRecord.firstByte[p75], benchmarkRecord.firstByte[p90], benchmarkRecord.firstByte[p99], benchmarkRecord.firstByte[max],
		benchmarkRecord.lastByte[avg], benchmarkRecord.lastByte[min], benchmarkRecord.lastByte[p25], benchmarkRecord.lastByte[p50], benchmarkRecord.lastByte[p75], benchmarkRecord.lastByte[p90], benchmarkRecord.lastByte[p99], benchmarkRecord.lastByte[max])

	// add the results to the csv array
	csvRecords = append(csvRecords, []string{
		fmt.Sprintf("%s", hostname),
		fmt.Sprintf("%s", instanceType),
		fmt.Sprintf("%d", payloadSize),
		fmt.Sprintf("%d", benchmarkRecord.threads),
		fmt.Sprintf("%.3f", rate),
		fmt.Sprintf("%.1f", benchmarkRecord.firstByte[avg]),
		fmt.Sprintf("%.1f", benchmarkRecord.firstByte[min]),
		fmt.Sprintf("%.1f", benchmarkRecord.firstByte[p25]),
		fmt.Sprintf("%.1f", benchmarkRecord.firstByte[p50]),
		fmt.Sprintf("%.1f", benchmarkRecord.firstByte[p75]),
		fmt.Sprintf("%.1f", benchmarkRecord.firstByte[p90]),
		fmt.Sprintf("%.1f", benchmarkRecord.firstByte[p99]),
		fmt.Sprintf("%.1f", benchmarkRecord.firstByte[max]),
		fmt.Sprintf("%.2f", benchmarkRecord.lastByte[avg]),
		fmt.Sprintf("%.2f", benchmarkRecord.lastByte[min]),
		fmt.Sprintf("%.1f", benchmarkRecord.lastByte[p25]),
		fmt.Sprintf("%.1f", benchmarkRecord.lastByte[p50]),
		fmt.Sprintf("%.1f", benchmarkRecord.lastByte[p75]),
		fmt.Sprintf("%.1f", benchmarkRecord.lastByte[p90]),
		fmt.Sprintf("%.1f", benchmarkRecord.lastByte[p99]),
		fmt.Sprintf("%.1f", benchmarkRecord.lastByte[max]),
	})

	return csvRecords
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
			logger.Println(" Info: Generate new key with value '" +  key + "'" )
		}
		// start the timer to measure the first byte and last byte latencies
		latencyTimer := time.Now()

		// measure the first byte latency
		firstByte = time.Now().Sub(latencyTimer)

		// do a PutObject request to create the object
		err := client.PutObject(bucketName,  key , bytes.NewReader(payload))

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
	// instance type string used to render results to stdout
	instanceTypeString := ""

	if instanceType != "" {
		instanceTypeString = " (" + instanceType + ")"
	}

	// print the table header
	testType := "Download"
	if operationToTest == "write" {
		testType = "Upload"
	}
	fmt.Printf("%s performance with \033[1;33m%-s\033[0m objects%s\n", testType, byteFormat(float64(objectSize)), instanceTypeString)
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
	key = folder + "/" + (fmt.Sprintf("%x", keyHash))
	return key
}

// cleans up the uploaded objects for this test (but doesn't remove the bucket)
func cleanup() {
	fmt.Print("\n--- \033[1;32mCLEANUP\033[0m ------------------------------------------------------------------------------------------------------------------\n\n")

	fmt.Printf("Deleting any objects uploaded from %s\n", hostname)

	// create a progress bar
	bar := progressbar.NewOptions(maxPayload*maxThreads-1, progressbar.OptionSetRenderBlankState(true))

	// an object size iterator that starts from 1 KB and doubles the size on every iteration
	generatePayload := payloadSizeGenerator()

	runNumber := 0

	// loop over every payload size
	for p := 1; p <= maxPayload; p++ {
		// get an object size from the iterator
		payloadSize := generatePayload()

		// loop over each possible thread to clean up objects from any previous test execution
		for t := 1; t <= maxThreads; t++ {
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

// gets the hostname or the EC2 instance ID
func getHostname() string {
	instanceId := getInstanceId()
	if instanceId != "" {
		return instanceId
	}

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

// gets the EC2 region from the instance metadata
func getRegion() string {
	httpClient := &http.Client{
		Timeout: time.Second,
	}

	link := "http://169.254.169.254/latest/meta-data/placement/availability-zone"
	response, err := httpClient.Get(link)
	if err != nil {
		return defaultRegion
	}

	content, _ := ioutil.ReadAll(response.Body)
	_ = response.Body.Close()

	az := string(content)

	return az[:len(az)-1]
}

// gets the EC2 instance type from the instance metadata
func getInstanceType() string {
	httpClient := &http.Client{
		Timeout: time.Second,
	}

	link := "http://169.254.169.254/latest/meta-data/instance-type"
	response, err := httpClient.Get(link)
	if err != nil {
		return ""
	}

	content, _ := ioutil.ReadAll(response.Body)
	_ = response.Body.Close()

	return string(content)
}

// gets the EC2 instance ID from the instance metadata
func getInstanceId() string {
	httpClient := &http.Client{
		Timeout: time.Second,
	}

	link := "http://169.254.169.254/latest/meta-data/instance-id"
	response, err := httpClient.Get(link)
	if err != nil {
		return ""
	}

	content, _ := ioutil.ReadAll(response.Body)
	_ = response.Body.Close()

	return string(content)
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

// adjust the sample count for small instances and for low thread counts (so that the test doesn't take forever)
func getTargetSampleCount(threads int, tasks int) int {
	if instanceType == "" {
		return minimumOf(50, tasks)
	}
	if !strings.Contains(instanceType, "xlarge") && !strings.Contains(instanceType, "metal") {
		return minimumOf(50, tasks)
	}
	if threads <= 4 {
		return minimumOf(100, tasks)
	}
	if threads <= 8 {
		return minimumOf(250, tasks)
	}
	if threads <= 16 {
		return minimumOf(500, tasks)
	}
	return tasks
}

// go doesn't seem to have a min function in the std lib!
func minimumOf(x, y int) int {
	if x < y {
		return x
	}
	return y
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
