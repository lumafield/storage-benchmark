package sbmark

import (
	"fmt"
	"time"
)

type ThroughputBenchmarkMode struct {
}

func (m *ThroughputBenchmarkMode) IsFinished(numberOfRuns int) bool {
	return numberOfRuns >= 1
}

func (m *ThroughputBenchmarkMode) PrintHeader(operationToTest string) {
	fmt.Print("\n--- BENCHMARK - Throughput ---------------------------------------------------------------------------------------------------\n\n")

	// prints the table header for the test results
	fmt.Printf("Max throughput for operation '%s' \n", operationToTest)
	fmt.Println("+-------------+---------+-----------------+")
	fmt.Println("| Object Size | Threads | Max. Throughput |")
	fmt.Println("+-------------+---------+-----------------+")
}

func (m *ThroughputBenchmarkMode) PrintPayloadHeader(objectSize uint64, operationToTest string) {
}

func (m *ThroughputBenchmarkMode) PrintRecord(record Record) {
	// print the results to stdout
	fmt.Printf("| %11s | %7d | %10.3f MB/s |\n",
		ByteFormat(float64(record.ObjectSizeBytes)), record.Threads, record.ThroughputMBps())
}

func (m *ThroughputBenchmarkMode) PrintPayloadFooter() {
}

func (m *ThroughputBenchmarkMode) PrintFooter() {
	fmt.Print("+-------------+---------+-----------------+\n\n")
	fmt.Printf("\n\n\n\n")
}

func (m *ThroughputBenchmarkMode) EnsureTestdata(ctx *BenchmarkContext, payloadSize uint64) {
	ctx.Operation.EnsureTestdata(ctx, payloadSize, &NilTicker{})
}

func (m *ThroughputBenchmarkMode) CleanupTestdata(ctx *BenchmarkContext, payloadSize uint64) {
	ctx.Operation.CleanupTestdata(ctx, &NilTicker{})
}

func (m *ThroughputBenchmarkMode) ExecuteBenchmark(ctx *BenchmarkContext, payloadSize uint64) {
	// increase thread count until total throughput is not increasing anymore

	// a channel to submit the test tasks
	testTasks := make(chan int)

	// a channel to receive results from the test tasks back on the main thread
	results := make(chan Latency)

	// create the workers for all the threads in this test
	for t := 1; t <= ctx.ThreadsMax; t++ {
		go func(threadId int, tasks <-chan int, results chan<- Latency) {
			for sampleId := range tasks {
				// execute operation and add the latency result to the results channel
				results <- ctx.Operation.Execute(ctx, sampleId, payloadSize)
			}
		}(t, testTasks, results)
	}

	sumPayloadSize := uint64(0)

	// start the timer for this benchmark
	benchmarkTimer := time.Now()

	// submit all the test tasks
	sampleId := 0
	for {
		sampleId++
		for t := 1; t <= ctx.ThreadsMax; t++ {
			testTasks <- sampleId
		}
		for t := 1; t <= ctx.ThreadsMax; t++ {
			timing := <-results
			// Only take results from samples without errors
			if timing.Errors != nil {
				continue
			}
			sumPayloadSize += payloadSize

			mb := float64(sumPayloadSize) / 1024 / 1024
			secs := time.Since(benchmarkTimer).Seconds()
			mbPerSec := mb / secs
			fmt.Printf("%d: %.3f MB in %.3f sec = %.3f MB/s\n", sampleId, mb, secs, mbPerSec)
		}
	}

	// close the channel
	close(testTasks)
}
