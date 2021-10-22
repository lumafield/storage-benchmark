package sbmark

type Report struct {
	Uuid        string   `json:"uuid"`       // A unique id for this benchmark report.
	ClientEnv   string   `json:"client_env"` // Description of the environment from which the benchmark has been executed.
	ServerEnv   string   `json:"server_env"` // Description of the environment of the test target.
	DateTimeUTC string   `json:"datetime_utc"`
	Records     []Record `json:"records"`
}

type Record struct {
	Operation        string             `json:"operation"` // read, write, ...
	TotalBytes       uint64             `json:"total_bytes"`
	SingleObjectSize uint64             `json:"single_object_size_bytes"`
	ObjectsCount     uint64             `json:"objects_count"`
	DurationSeconds  float64            `json:"duration_secs"`
	Threads          int                `json:"threads"`
	TimeToFirstByte  map[string]float64 `json:"ttfb_latency_ms"`
	TimeToLastByte   map[string]float64 `json:"ttlb_latency_ms"`
	DNSLookup        map[string]float64 `json:"dns_latency_ms"`
	TCPConnection    map[string]float64 `json:"tcp_latency_ms"`
	TLSHandshake     map[string]float64 `json:"tls_latency_ms"`
	ServerProcessing map[string]float64 `json:"server_latency_ms"`
	Unassigned       map[string]float64 `json:"unassigned_latency_ms"`
	Throughput       map[string]float64 `json:"throughput"`
}

func (r *Record) ThroughputMBps() float64 {
	return r.ThroughputBps() / 1024 / 1024
}

func (r *Record) ThroughputBps() float64 {
	return (float64(r.TotalBytes)) / r.DurationSeconds
}

// comparator to sort by throughput
type ByThroughputBps []Record

func (a ByThroughputBps) Len() int           { return len(a) }
func (a ByThroughputBps) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByThroughputBps) Less(i, j int) bool { return a[i].ThroughputBps() < a[j].ThroughputBps() }

func Mapf(vs []Record, f func(Record) float64) []float64 {
	vsm := make([]float64, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}
