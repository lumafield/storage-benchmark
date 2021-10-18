package sbmark

import (
	"fmt"
	"sort"
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
	// increase thread count until throughput is not increasing anymore
	t := 0
	for {
		t++
		m.execTest(ctx, t, payloadSize, ctx.NumberOfRuns)
		if m.IsFinished(ctx.NumberOfRuns) {
			break
		}
	}
}

func (m *ThroughputBenchmarkMode) execTest(ctx *BenchmarkContext, threadCount int, payloadSize uint64, runId int) {
	// a channel to submit the test tasks
	testTasks := make(chan int, threadCount)

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
	dataPoints := []Latency{}
	sumFirstByte := int64(0)
	sumLastByte := int64(0)
	sumDNSLookup := int64(0)
	sumTCPConnection := int64(0)
	sumTLSHandshake := int64(0)
	sumServerProcessing := int64(0)
	sumUnassigned := int64(0)

	record := Record{
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
		// Only take results from samples without errors
		if timing.Errors != nil {
			continue
		}
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
	sort.Sort(ByFirstByte(dataPoints))
	record.TimeToFirstByte["avg"] = (float64(sumFirstByte) / float64(ctx.Samples)) / 1000000
	record.TimeToFirstByte["min"] = float64(dataPoints[0].FirstByte.Nanoseconds()) / 1000000
	record.TimeToFirstByte["max"] = float64(dataPoints[len(dataPoints)-1].FirstByte.Nanoseconds()) / 1000000
	record.TimeToFirstByte["p25"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.25))-1].FirstByte.Nanoseconds()) / 1000000
	record.TimeToFirstByte["p50"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.5))-1].FirstByte.Nanoseconds()) / 1000000
	record.TimeToFirstByte["p75"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.75))-1].FirstByte.Nanoseconds()) / 1000000
	record.TimeToFirstByte["p90"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.90))-1].FirstByte.Nanoseconds()) / 1000000
	record.TimeToFirstByte["p99"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.99))-1].FirstByte.Nanoseconds()) / 1000000

	// calculate the summary statistics for the last byte latencies
	sort.Sort(ByLastByte(dataPoints))
	record.TimeToLastByte["avg"] = (float64(sumLastByte) / float64(ctx.Samples)) / 1000000
	record.TimeToLastByte["min"] = float64(dataPoints[0].LastByte.Nanoseconds()) / 1000000
	record.TimeToLastByte["max"] = float64(dataPoints[len(dataPoints)-1].LastByte.Nanoseconds()) / 1000000
	record.TimeToLastByte["p25"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.25))-1].LastByte.Nanoseconds()) / 1000000
	record.TimeToLastByte["p50"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.5))-1].LastByte.Nanoseconds()) / 1000000
	record.TimeToLastByte["p75"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.75))-1].LastByte.Nanoseconds()) / 1000000
	record.TimeToLastByte["p90"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.90))-1].LastByte.Nanoseconds()) / 1000000
	record.TimeToLastByte["p99"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.99))-1].LastByte.Nanoseconds()) / 1000000

	// calculate the summary statistics for the DNS lookup latencies
	sort.Sort(ByDNSLookup(dataPoints))
	record.DNSLookup["avg"] = (float64(sumDNSLookup) / float64(ctx.Samples)) / 1000000
	record.DNSLookup["min"] = float64(dataPoints[0].DNSLookup.Nanoseconds()) / 1000000
	record.DNSLookup["max"] = float64(dataPoints[len(dataPoints)-1].DNSLookup.Nanoseconds()) / 1000000
	record.DNSLookup["p25"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.25))-1].DNSLookup.Nanoseconds()) / 1000000
	record.DNSLookup["p50"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.5))-1].DNSLookup.Nanoseconds()) / 1000000
	record.DNSLookup["p75"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.75))-1].DNSLookup.Nanoseconds()) / 1000000
	record.DNSLookup["p90"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.90))-1].DNSLookup.Nanoseconds()) / 1000000
	record.DNSLookup["p99"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.99))-1].DNSLookup.Nanoseconds()) / 1000000

	// calculate the summary statistics for the TCP connection latencies
	sort.Sort(ByTCPConnection(dataPoints))
	record.TCPConnection["avg"] = (float64(sumTCPConnection) / float64(ctx.Samples)) / 1000000
	record.TCPConnection["min"] = float64(dataPoints[0].TCPConnection.Nanoseconds()) / 1000000
	record.TCPConnection["max"] = float64(dataPoints[len(dataPoints)-1].TCPConnection.Nanoseconds()) / 1000000
	record.TCPConnection["p25"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.25))-1].TCPConnection.Nanoseconds()) / 1000000
	record.TCPConnection["p50"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.5))-1].TCPConnection.Nanoseconds()) / 1000000
	record.TCPConnection["p75"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.75))-1].TCPConnection.Nanoseconds()) / 1000000
	record.TCPConnection["p90"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.90))-1].TCPConnection.Nanoseconds()) / 1000000
	record.TCPConnection["p99"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.99))-1].TCPConnection.Nanoseconds()) / 1000000

	// calculate the summary statistics for the TLS handshake latencies
	sort.Sort(ByTLSHandshake(dataPoints))
	record.TLSHandshake["avg"] = (float64(sumTLSHandshake) / float64(ctx.Samples)) / 1000000
	record.TLSHandshake["min"] = float64(dataPoints[0].TLSHandshake.Nanoseconds()) / 1000000
	record.TLSHandshake["max"] = float64(dataPoints[len(dataPoints)-1].TLSHandshake.Nanoseconds()) / 1000000
	record.TLSHandshake["p25"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.25))-1].TLSHandshake.Nanoseconds()) / 1000000
	record.TLSHandshake["p50"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.5))-1].TLSHandshake.Nanoseconds()) / 1000000
	record.TLSHandshake["p75"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.75))-1].TLSHandshake.Nanoseconds()) / 1000000
	record.TLSHandshake["p90"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.90))-1].TLSHandshake.Nanoseconds()) / 1000000
	record.TLSHandshake["p99"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.99))-1].TLSHandshake.Nanoseconds()) / 1000000

	// calculate the summary statistics for the server processing latencies
	sort.Sort(ByServerProcessing(dataPoints))
	record.ServerProcessing["avg"] = (float64(sumServerProcessing) / float64(ctx.Samples)) / 1000000
	record.ServerProcessing["min"] = float64(dataPoints[0].ServerProcessing.Nanoseconds()) / 1000000
	record.ServerProcessing["max"] = float64(dataPoints[len(dataPoints)-1].ServerProcessing.Nanoseconds()) / 1000000
	record.ServerProcessing["p25"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.25))-1].ServerProcessing.Nanoseconds()) / 1000000
	record.ServerProcessing["p50"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.5))-1].ServerProcessing.Nanoseconds()) / 1000000
	record.ServerProcessing["p75"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.75))-1].ServerProcessing.Nanoseconds()) / 1000000
	record.ServerProcessing["p90"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.90))-1].ServerProcessing.Nanoseconds()) / 1000000
	record.ServerProcessing["p99"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.99))-1].ServerProcessing.Nanoseconds()) / 1000000

	// calculate the summary statistics for the unassigned latencies
	sort.Sort(ByUnassigned(dataPoints))
	record.Unassigned["avg"] = (float64(sumUnassigned) / float64(ctx.Samples)) / 1000000
	record.Unassigned["min"] = float64(dataPoints[0].Unassigned().Nanoseconds()) / 1000000
	record.Unassigned["max"] = float64(dataPoints[len(dataPoints)-1].Unassigned().Nanoseconds()) / 1000000
	record.Unassigned["p25"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.25))-1].Unassigned().Nanoseconds()) / 1000000
	record.Unassigned["p50"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.5))-1].Unassigned().Nanoseconds()) / 1000000
	record.Unassigned["p75"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.75))-1].Unassigned().Nanoseconds()) / 1000000
	record.Unassigned["p90"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.90))-1].Unassigned().Nanoseconds()) / 1000000
	record.Unassigned["p99"] = float64(dataPoints[int(float64(ctx.Samples)*float64(0.99))-1].Unassigned().Nanoseconds()) / 1000000

	// append the record to the report
	ctx.Report.Records = append(ctx.Report.Records, record)

	m.PrintRecord(record)
}
