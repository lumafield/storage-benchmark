package sbmark

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"
)

// Abstraction of storage operations (file, object, ...)
type StorageInterface interface {
	CreateBucket(bucketName string) (Latency, error)
	HeadObject(bucket string, key string) (Latency, error)
	PutObject(bucket string, key string, reader *bytes.Reader) (Latency, error)
	GetObject(bucket string, key string) (Latency, io.ReadCloser, error)
	DeleteObject(bucket string, key string) (Latency, error)
}

// Represents the duration from making different parts of an operation including the time to first byte (TTFB) and the time to last byte (TTLB).
type Latency struct {
	FirstByte        time.Duration
	LastByte         time.Duration
	DNSLookup        time.Duration
	TCPConnection    time.Duration
	TLSHandshake     time.Duration
	ServerProcessing time.Duration
}

func (lat *Latency) Unassigned() time.Duration {
	return lat.LastByte - lat.DNSLookup - lat.TCPConnection - lat.TLSHandshake - lat.ServerProcessing
}

func (lat *Latency) ToString() string {

	lines := []string{
		fmt.Sprintf("DNS lookup: %d ms", lat.DNSLookup.Milliseconds()),
		fmt.Sprintf("TCP connection: %d ms", lat.TCPConnection.Milliseconds()),
		fmt.Sprintf("TLS handshake: %d ms", lat.TLSHandshake.Milliseconds()),
		fmt.Sprintf("Server processing: %d ms", lat.ServerProcessing.Milliseconds()),
		fmt.Sprintf("Unassigned: %d ms", lat.Unassigned().Milliseconds()),
		fmt.Sprintf("TTFB: %d ms", lat.FirstByte.Milliseconds()),
		fmt.Sprintf("TTLB: %d ms", lat.LastByte.Milliseconds()),
	}

	return strings.Join(lines[:], "\n")
}

// comparator to sort by first byte latency
type ByFirstByte []Latency

func (a ByFirstByte) Len() int           { return len(a) }
func (a ByFirstByte) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByFirstByte) Less(i, j int) bool { return a[i].FirstByte < a[j].FirstByte }

// comparator to sort by last byte latency
type ByLastByte []Latency

func (a ByLastByte) Len() int           { return len(a) }
func (a ByLastByte) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByLastByte) Less(i, j int) bool { return a[i].LastByte < a[j].LastByte }

// comparator to sort by DNS lookup latency
type ByDNSLookup []Latency

func (a ByDNSLookup) Len() int           { return len(a) }
func (a ByDNSLookup) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByDNSLookup) Less(i, j int) bool { return a[i].DNSLookup < a[j].DNSLookup }

// comparator to sort by TCP connection latency
type ByTCPConnection []Latency

func (a ByTCPConnection) Len() int           { return len(a) }
func (a ByTCPConnection) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTCPConnection) Less(i, j int) bool { return a[i].TCPConnection < a[j].TCPConnection }

// comparator to sort by TLS handshake latency
type ByTLSHandshake []Latency

func (a ByTLSHandshake) Len() int           { return len(a) }
func (a ByTLSHandshake) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTLSHandshake) Less(i, j int) bool { return a[i].TLSHandshake < a[j].TLSHandshake }

// comparator to sort by server processing latency
type ByServerProcessing []Latency

func (a ByServerProcessing) Len() int           { return len(a) }
func (a ByServerProcessing) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByServerProcessing) Less(i, j int) bool { return a[i].ServerProcessing < a[j].ServerProcessing }

// comparator to sort by unassigned latency
type ByUnassigned []Latency

func (a ByUnassigned) Len() int           { return len(a) }
func (a ByUnassigned) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByUnassigned) Less(i, j int) bool { return a[i].Unassigned() < a[j].Unassigned() }
