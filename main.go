package main

import (
	"crypto/sha1"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	log2 "log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/iternity-dotcom/storage-benchmark/sbmark"
	uuid "github.com/satori/go.uuid"
)

const bucketNamePrefix = "storage-benchmark"

var defaultBucketName = fmt.Sprintf("%s-%x", bucketNamePrefix, sha1.Sum([]byte(getHostname())))

// use go build -ldflags "-X main.buildstamp=`date -u '+%Y-%m-%dT%I:%M:%S'` -X main.githash=`git rev-parse HEAD`"
var buildstamp = "TODAY"
var githash = "DEV"

// wether to display the version information
var showVersion bool

// if true, all given .json output files will be parsed and fixed to match the current version of the model.
var fix bool

// if true, all given .json output files will be parsed and printed to the console using the standard console output format.
var print bool

// if not empty, the results of the test are saved as .csv file
var csvFileName string

// holds a path to a .json file or a path where to find multiple .json files
var jsonPath string

// whether to create a bucket on startup
var createBucket bool

// path for log file
var logPath string

// the context for this benchmark includes everything that's needed to run the benchmark
var ctx *sbmark.BenchmarkContext

// program entry point
func main() {
	parseFlags()
	if showVersion {
		displayVersion()
		return
	}
	if fix || print {
		fixJsonFiles()
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
	modeArg := flag.String("mode", "latency", "What do you want to measure? Choose 'latency' or 'burst'. Default is 'latency'")
	fixArg := flag.Bool("fix", false, "If set all .json reports given with the -json option will be parsed and fixed, so that they match the current version of storage-benchmark.")
	printArg := flag.Bool("print", false, "If set all .json reports given with -json option will be printed using the standard console output format.")
	keepAliveArg := flag.String("keepalive", "mode", "Use 'enabled' or 'disabled' to explicitly enable or dissable connection pooling to your endpoint. Use 'mode' to use the default of the specified benchmark mode. Default is 'mode'.")

	// parse the arguments and set all the global variables accordingly
	flag.Parse()

	showVersion = *versionArg
	fix = *fixArg
	print = *printArg
	jsonPath = *jsonArg

	// Stop parsing flags if -version or -fix-json arguments are there
	if showVersion || fix || print {
		return
	}

	csvFileName = *csvArg
	createBucket = *createBucketArg
	if *logPathArg == "" {
		logPath, _ = os.Getwd()
	} else {
		logPath = *logPathArg
	}

	ctx = &sbmark.BenchmarkContext{
		Description:   *descriptionArg,
		ModeName:      *modeArg,
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
		InfoLogger:    createLogger("INFO "),
		WarningLogger: createLogger("WARNING "),
		ErrorLogger:   createLogger("ERROR "),
		KeepAlive:     *keepAliveArg,
	}

	err := ctx.Start()
	if err != nil {
		panic(err)
	}
}

func displayVersion() {
	fmt.Printf("%s version %s", getAppName(), getVersion())
}

// Try to transform existing storage-benchmark .json files to the current model.
// The function creates a new .json file, if the existing file can be transformed.
// The function is idempotent, so it can be safely executed multiple times.
// It will ignore files that does not end with .json or that doesn't seem to be a valid storage-benchmark result.
func fixJsonFiles() {
	fileList := make([]string, 0)
	e := filepath.Walk(jsonPath, func(path string, f os.FileInfo, err error) error {
		if filepath.Ext(path) == ".json" && !strings.HasSuffix(path, githash+".json") {
			fileList = append(fileList, path)
		}
		return err
	})

	if e != nil {
		panic(e)
	}

	for _, file := range fileList {
		// Parse in a generic way and try to transform the model
		var genericModel map[string]interface{}
		jsonData, err := ioutil.ReadFile(file)
		if err != nil {
			fmt.Printf("Couldn't read file %s. Error: %v\n", file, err)
			continue
		}
		err = json.Unmarshal(jsonData, &genericModel)
		if err != nil {
			fmt.Printf("Couldn't unmarshal file %s. Error: %v\n", file, err)
			continue
		}

		// Verify if this is a storage-benchmark result file
		_, hostnameExists := genericModel["hostname"]
		_, endpointExists := genericModel["endpoint"]
		_, operationExists := genericModel["operation"]
		_, reportExists := genericModel["report"]
		if !hostnameExists || !endpointExists || !operationExists || !reportExists {
			fmt.Printf("Skipping %s. This doesn't seem to be a storage-benchmark result file.\n", file)
			continue
		}

		// Remove ignored interface{} values that can conflict with primitive values
		if genericModel["Mode"] != nil {
			delete(genericModel, "Mode")
		}
		if genericModel["Operation"] != nil {
			delete(genericModel, "Operation")
		}

		// Marshal fixed model to []byte
		jsonData, err = json.Marshal(genericModel)
		if err != nil {
			fmt.Printf("Couldn't marshal content of file %s. Error: %v\n", file, err)
			continue
		}

		// Unmarshal to existing BenchmarkContext model
		ctx, err := sbmark.FromJsonByteArray(jsonData)
		ctx.Start()
		if err != nil {
			fmt.Printf("Couldn't unmarshal modified content of file %s. Error: %v\n", file, err)
			continue
		}

		if fix {
			fixJson(ctx, file)
		}

		if print {
			printJson(ctx)
		}
	}
}

func fixJson(ctx *sbmark.BenchmarkContext, file string) {
	// Create Uuid if missing
	if ctx.Report.Uuid == "" {
		uuid := uuid.NewV4().String()
		// Try to get a UUID from an already transformed .json file
		fixedJsonFile := getFixedJsonFileName(file)
		fixedJsonData, err := ioutil.ReadFile(fixedJsonFile)
		if err == nil {
			fb, err := sbmark.FromJsonByteArray(fixedJsonData)
			if err == nil {
				uuid = fb.Report.Uuid
			}
		}
		ctx.Report.Uuid = uuid
	}

	// Move description from BenchmarkContext to BenchmarkReport
	if ctx.Description != "" && ctx.Report.Description == "" {
		ctx.Report.Description = ctx.Description
	}

	// Transform Records
	for i := range ctx.Report.Records {
		if ctx.Report.Records[i].ObjectSizeBytes > 0 && ctx.Report.Records[i].TotalBytes == 0 {
			ctx.Report.Records[i].TotalBytes = ctx.Report.Records[i].ObjectSizeBytes
		}
		if ctx.Report.Records[i].ObjectsCount == 0 {
			ctx.Report.Records[i].ObjectsCount = uint64(ctx.Samples)
		}
		if ctx.Report.Records[i].SingleObjectSize == 0 && ctx.Report.Records[i].ObjectSizeBytes > 0 && ctx.Samples > 0 {
			ctx.Report.Records[i].SingleObjectSize = ctx.Report.Records[i].ObjectSizeBytes / uint64(ctx.Samples)
		}
	}
	jsonData, err := sbmark.ToJson(ctx)
	if err != nil {
		fmt.Printf("Couldn't marshal modified content of file %s. Error: %v\n", file, err)
		return
	}
	// Create a new file containing the transformed model
	newFile := getFixedJsonFileName(file)
	err = os.WriteFile(newFile, jsonData, 0644)
	if err != nil {
		fmt.Printf("Couldn't create file %s. Error: %v\n", newFile, err)
		return
	}
	fmt.Printf("Processed %s. Created %s\n", file, newFile)
}

func printJson(ctx *sbmark.BenchmarkContext) {
	ctx.PrintSettings()
	ctx.Mode.PrintHeader(ctx.OperationName)
	payloadSize := uint64(0)
	for _, r := range ctx.Report.Records {
		if payloadSize != 0 && payloadSize != r.SingleObjectSize {
			ctx.Mode.PrintPayloadFooter()
		}
		if payloadSize != r.SingleObjectSize {
			payloadSize = r.SingleObjectSize
			ctx.Mode.PrintPayloadHeader(payloadSize, ctx.OperationName)
		}
		ctx.Mode.PrintRecord(r)
	}
	ctx.Mode.PrintPayloadFooter()
	ctx.Mode.PrintFooter()
}

func getFixedJsonFileName(origFile string) string {
	origFileWithoutExt := strings.Replace(origFile, filepath.Ext(origFile), "", 1)
	return fmt.Sprintf("%s.%s.json", origFileWithoutExt, githash)
}

func createLogger(prefix string) *log2.Logger {
	file, _ := os.OpenFile(filepath.FromSlash(logPath+"/")+"storage-benchmark.log", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
	return log2.New(file, prefix, log2.Ldate+log2.Ltime+log2.Lshortfile+log2.Lmsgprefix)
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

	// Init the final report
	ctx.Report = sbmark.Report{
		Uuid:        uuid.NewV4().String(),
		Description: ctx.Description,
		ClientEnv:   fmt.Sprintf("Application: %s, Version: %s, Host: %s, OS: %s", getAppName(), getVersion(), getHostname(), runtime.GOOS),
		ServerEnv:   ctx.Endpoint, // How can we get some informations about the ServerEnv? Or should this be a CLI param?
		DateTimeUTC: time.Now().UTC().String(),
		Records:     []sbmark.Record{},
	}

	// an object size iterator that starts from 1 KB and doubles the size on every iteration
	generatePayload := payloadSizeGenerator()

	ctx.PrintSettings()
	ctx.Mode.PrintHeader(ctx.OperationName)

	// loop over every payload size (we need to start at p := 1 because of the generatePayload() function)
	for p := 1; p <= ctx.PayloadsMax; p++ {
		// get an object size from the iterator
		payloadSize := generatePayload()

		// ignore payloads smaller than the min argument
		if p < ctx.PayloadsMin {
			continue
		}

		ctx.Mode.EnsureTestdata(ctx, payloadSize)
		ctx.Mode.PrintPayloadHeader(payloadSize, ctx.OperationName)
		ctx.Mode.ExecuteBenchmark(ctx, payloadSize)
		ctx.Mode.PrintPayloadFooter()
		ctx.Mode.CleanupTestdata(ctx, payloadSize)
	}

	ctx.Mode.PrintFooter()

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
	if jsonPath != "" {
		jsonReport, err := sbmark.ToJson(ctx)
		if err != nil {
			panic("Failed to create .json output: " + err.Error())
		}
		err = os.WriteFile(jsonPath, jsonReport, 0644)
		if err != nil {
			panic("Failed to create .json output: " + err.Error())
		}
		fmt.Printf("JSON results were written to %s\n", jsonPath)
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

func getVersion() string {
	return fmt.Sprintf("%s.%s", githash, buildstamp)
}

func getAppName() string {
	return filepath.Base(os.Args[0])
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
