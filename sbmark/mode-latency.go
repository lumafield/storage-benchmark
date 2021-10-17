package sbmark

import "fmt"

type LatencyMode struct {
}

func (m *LatencyMode) IsFinished(numberOfRuns int) bool {
	return numberOfRuns >= 1
}

func (m *LatencyMode) PrintHeader(objectSize uint64, operationToTest string) {
	// prints the table header for the test results
	fmt.Printf("Latencies for operation '%s' of %s objects\n", operationToTest, ByteFormat(float64(objectSize)))
	fmt.Println("                           +-------------------------------------------------------------------------------------------------+----------------------------------+")
	fmt.Println("                           |            Time to First Byte (ms)             |            Time to Last Byte (ms)              | Latency Distribution (avg in ms) |")
	fmt.Println("+---------+----------------+------------------------------------------------+------------------------------------------------+----------------------------------+")
	fmt.Println("| Threads |     Throughput |  avg   min   p25   p50   p75   p90   p99   max |  avg   min   p25   p50   p75   p90   p99   max |    dns   tcp   tls   srv   rest  |")
	fmt.Println("+---------+----------------+------------------------------------------------+------------------------------------------------+----------------------------------+")
}
