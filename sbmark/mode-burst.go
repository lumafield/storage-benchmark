package sbmark

import (
	"fmt"
	"sort"
	"time"

	"github.com/montanaflynn/stats"
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
	fmt.Println("                                  +---------------------------------------------------------------------------------------+")
	fmt.Println("                                  |                                   Throughput (MB/s)                                   |")
	fmt.Println("+-------------+---------+---------+---------------------------------------------------------------------------------------+")
	fmt.Println("| Object Size | Threads | Samples |      avg        min        p25        p50        p75        p90        p99        max |")
	fmt.Println("+-------------+---------+---------+---------------------------------------------------------------------------------------+")
}

func (m *BurstBenchmarkMode) PrintPayloadHeader(objectSize uint64, operationToTest string) {
}

func (m *BurstBenchmarkMode) PrintRecord(record Record) {
	// print the results to stdout
	fmt.Printf("| %11s | %7d | %7d | %8.2f   %8.2f   %8.2f   %8.2f   %8.2f   %8.2f   %8.2f   %8.2f |\n",
		ByteFormat(float64(record.SingleObjectSize)), record.Threads, len(record.Throughput),
		record.Throughput["avg"], record.Throughput["min"],
		record.Throughput["p25"], record.Throughput["p50"], record.Throughput["p75"], record.Throughput["p90"], record.Throughput["p99"],
		record.Throughput["max"],
	)
}

func (m *BurstBenchmarkMode) PrintPayloadFooter() {
}

func (m *BurstBenchmarkMode) PrintFooter() {
	fmt.Println("+-------------+---------+---------+---------------------------------------------------------------------------------------+")
	fmt.Printf("\n\n\n\n\n")
}

func (m *BurstBenchmarkMode) EnsureTestdata(ctx *BenchmarkContext, payloadSize uint64) {
	ctx.Operation.EnsureTestdata(ctx, payloadSize, &NilTicker{})
}

func (m *BurstBenchmarkMode) CleanupTestdata(ctx *BenchmarkContext, payloadSize uint64) {
	ctx.Operation.CleanupTestdata(ctx, &NilTicker{})
}

func (m *BurstBenchmarkMode) ExecuteBenchmark(ctx *BenchmarkContext, payloadSize uint64) {
	var records []Record
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
			records = append(records, record)
			if m.IsFinished(numberOfRuns) {
				break
			}
		}
	}

	// calculate the summary statistics for the first byte latencies
	sort.Sort(ByThroughputBps(records))
	maxRecord = records[len(records)-1]
	maxRecord.Throughput = make(map[string]float64)
	dataPoints := Mapf(records, func(r Record) float64 {
		return r.ThroughputMBps()
	})

	quartiles, _ := stats.Quartile(dataPoints)
	maxRecord.Throughput["q1"] = quartiles.Q1
	maxRecord.Throughput["q2"] = quartiles.Q2
	maxRecord.Throughput["q3"] = quartiles.Q3
	maxRecord.Throughput["stdev"], _ = stats.StandardDeviation(dataPoints)
	maxRecord.Throughput["var"], _ = stats.Variance(dataPoints)
	maxRecord.Throughput["min"], _ = stats.Min(dataPoints)
	maxRecord.Throughput["avg"], _ = stats.Mean(dataPoints)
	maxRecord.Throughput["p25"], _ = stats.Percentile(dataPoints, 25)
	maxRecord.Throughput["p50"], _ = stats.Percentile(dataPoints, 50)
	maxRecord.Throughput["p75"], _ = stats.Percentile(dataPoints, 75)
	maxRecord.Throughput["p90"], _ = stats.Percentile(dataPoints, 90)
	maxRecord.Throughput["p99"], _ = stats.Percentile(dataPoints, 99)
	maxRecord.Throughput["max"], _ = stats.Max(dataPoints)

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
