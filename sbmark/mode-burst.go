package sbmark

import (
	"fmt"
	"time"
)

type BurstBenchmarkMode struct {
}

func (m *BurstBenchmarkMode) DisableKeepAlives() bool {
	// We want to keep the latency as low as possible for measuring the max throughput.
	// We enable keepalives to force connection pooling so that we don't need to reconnect to the server on any object operation.
	return false
}

func (m *BurstBenchmarkMode) IsFinished(numberOfRuns int) bool {
	return numberOfRuns > 10
}

func (m *BurstBenchmarkMode) PrintHeader(operationToTest string) {
	fmt.Print("\n--- BENCHMARK - Burst ---------------------------------------------------------------------------------------------------------\n\n")

	// prints the table header for the test results
	fmt.Printf("Max throughput for operation '%s' \n", operationToTest)
	fmt.Println("+-------------+---------+-----------------+")
	fmt.Println("| Object Size | Threads | Max. Throughput |")
	fmt.Println("+-------------+---------+-----------------+")
}

func (m *BurstBenchmarkMode) PrintPayloadHeader(objectSize uint64, operationToTest string) {
}

func (m *BurstBenchmarkMode) PrintRecord(record Record) {
	// print the results to stdout
	fmt.Printf("| %11s | %7d | %10.3f MB/s |\n",
		ByteFormat(float64(record.SingleObjectSize)), record.Threads, record.ThroughputMBps())
}

func (m *BurstBenchmarkMode) PrintPayloadFooter() {
}

func (m *BurstBenchmarkMode) PrintFooter() {
	fmt.Print("+-------------+---------+-----------------+\n\n")
	fmt.Printf("\n\n\n\n")
}

func (m *BurstBenchmarkMode) EnsureTestdata(ctx *BenchmarkContext, payloadSize uint64) {
	ctx.Operation.EnsureTestdata(ctx, payloadSize, &NilTicker{})
}

func (m *BurstBenchmarkMode) CleanupTestdata(ctx *BenchmarkContext, payloadSize uint64) {
	ctx.Operation.CleanupTestdata(ctx, &NilTicker{})
}

func (m *BurstBenchmarkMode) ExecuteBenchmark(ctx *BenchmarkContext, payloadSize uint64) {
	// run a test per thread count and object size combination
	maxRecord := Record{
		TotalBytes:      0,
		DurationSeconds: 1,
	}
	for t := ctx.ThreadsMin; t <= ctx.ThreadsMax; t++ {
		numberOfRuns := 0
		for {
			ctx.NumberOfRuns++
			numberOfRuns++
			record := m.execTest(ctx, t, payloadSize, ctx.NumberOfRuns)
			// append the record to the report
			if record.ThroughputBps() > maxRecord.ThroughputBps() {
				maxRecord = record
			}
			//fmt.Printf("%d %d %4.3f %4.3f\n", record.Threads, record.ObjectsCount, record.ThroughputMBps(), maxRecord.ThroughputMBps())
			if m.IsFinished(numberOfRuns) {
				break
			}
		}
	}
	m.PrintRecord(maxRecord)
	ctx.Report.Records = append(ctx.Report.Records, maxRecord)
}

func (m *BurstBenchmarkMode) execTest(ctx *BenchmarkContext, threadCount int, payloadSize uint64, runId int) Record {
	// a channel to submit the test tasks
	testTasks := make(chan int, ctx.Samples)

	// a channel to receive results from the test tasks back on the main thread
	results := make(chan Latency, ctx.Samples)

	// create the workers for all the threads in this test
	for t := 1; t <= threadCount; t++ {
		go func(threadId int, tasks <-chan int, results chan<- Latency) {
			for sampleId := range tasks {
				// execute operation and add the latency result to the results channel
				results <- ctx.Operation.Execute(ctx, sampleId, payloadSize)
			}
		}(t, testTasks, results)
	}

	// construct a new benchmark record
	var sumConnectionOverhead time.Duration

	record := Record{
		TotalBytes:       0,
		SingleObjectSize: payloadSize,
		ObjectsCount:     0,
		Operation:        ctx.OperationName,
		Threads:          threadCount,
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
		// Only take results from samples without errors
		if timing.Errors != nil {
			continue
		}
		record.TotalBytes += payloadSize
		record.ObjectsCount += 1
		sumConnectionOverhead += timing.DNSLookup + timing.TCPConnection + timing.TLSHandshake
	}

	// stop the timer for this benchmark
	totalTime := time.Since(benchmarkTimer)
	record.DurationSeconds = totalTime.Seconds() // - sumConnectionOverhead.Seconds()

	return record
}
