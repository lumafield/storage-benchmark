package sbmark

type Report struct {
	Endpoint    string   `json:"endpoint"`
	Path        string   `json:"path"`
	ClientEnv   string   `json:"client_env"`   // Description of the environment from which the benchmark has been executed.
	ServerEnv   string   `json:"server_env"`   // Description of the environment of the test target.
	DateTimeUTC string   `json:"datetime_utc"` //
	Records     []Record `json:"records"`
}

type Record struct {
	Operation       string             `json:"operation"` // read, write, ...
	ObjectSizeBytes uint64             `json:"object_size_bytes"`
	DurationSeconds float64            `json:"duration_secs"`
	Threads         int                `json:"threads"`
	TimeToFirstByte map[string]float64 `json:"ttfb_latency"`
	TimeToLastByte  map[string]float64 `json:"ttlb_latency"`
}

func (r *Record) ThroughputMBps() float64 {
	return r.ThroughputBps() / 1024 / 1024
}

func (r *Record) ThroughputBps() float64 {
	return (float64(r.ObjectSizeBytes)) / r.DurationSeconds
}
