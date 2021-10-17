package sbmark

import (
	"bytes"
	"time"
)

type OperationWrite struct {
	keys []string
}

func (op *OperationWrite) EnsureTestdata(ctx *BenchmarkContext, payloadSite uint64, ticker Ticker) {
	// Nothing to prepare for write operations
	ticker.Add(ctx.Samples - 1)
}

func (op *OperationWrite) Execute(ctx *BenchmarkContext, sampleId int, payloadSize uint64) Latency {
	key := generateObjectKey(sampleId, payloadSize)
	reader := bytes.NewReader(make([]byte, payloadSize))

	// start the timer
	latencyTimer := time.Now()

	// do a PutObject request to create the object and init the Latency struct
	latency, err := ctx.Client.PutObject(ctx.Path, key, reader)

	// measure the last byte latency
	latency.LastByte = time.Since(latencyTimer)

	// if a request fails, exit
	if err != nil {
		panic("Failed to put object: " + err.Error())
	}

	op.keys = append(op.keys, key)

	return latency
}

func (op *OperationWrite) CleanupTestdata(ctx *BenchmarkContext, ticker Ticker) {
	for _, key := range op.keys {
		ticker.Add(1)
		ctx.Client.DeleteObject(ctx.Path, key)
	}
}