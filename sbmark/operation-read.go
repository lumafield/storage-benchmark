package sbmark

import (
	"bytes"
	"io"
	"strings"
	"time"
)

type OperationRead struct {
	keys []string
}

func (op *OperationRead) EnsureTestdata(ctx *BenchmarkContext, payloadSize uint64) {
	// upload the objects for this run (if needed)
	//fmt.Printf("Uploading %d x %s objects\n", ctx.NumberOfObjectsPerPayload(), ByteFormat(float64(payload)))
	//uploadBar := progressbar.NewOptions(ctx.Samples-1, progressbar.OptionSetRenderBlankState(true))

	// create an object for every thread, so that different threads don'sampleId download the same object
	for sampleId := 1; sampleId <= ctx.Samples; sampleId++ {
		// increment the progress bar for each object
		//_ = bar.Add(1)

		// generate an object key from the sha hash of the hostname, thread index, and object size
		key := generateObjectKey(sampleId, payloadSize)

		// do a HeadObject request to avoid uploading the object if it already exists from a previous test run
		_, err := ctx.Client.HeadObject(ctx.Path, key)

		// if no error, then the object exists, so skip this one
		if err == nil {
			continue
		}

		// if other error, exit
		if err != nil && !strings.Contains(err.Error(), "NotFound:") {
			panic("Failed to head object: " + err.Error())
		}

		// generate reader
		reader := bytes.NewReader(make([]byte, payloadSize))

		// make sure that the object exists
		_, err = ctx.Client.PutObject(ctx.Path, key, reader)
		op.keys = append(op.keys, key)

		// if the put fails, exit
		if err != nil {
			panic("Failed to put object: " + err.Error())
		}
	}

	//fmt.Print("\n\n")
}

func (op *OperationRead) Execute(ctx *BenchmarkContext, sampleId int, payloadSize uint64) Latency {
	key := generateObjectKey(sampleId, payloadSize)
	// start the timer to measure the first byte and last byte latencies
	latencyTimer := time.Now()

	// do the GetObject request
	latency, dataStream, err := ctx.Client.GetObject(ctx.Path, key)

	// if a request fails, exit
	if err != nil {
		panic("Failed to get object: " + err.Error())
	}

	// measure the first byte latency
	latency.FirstByte = time.Now().Sub(latencyTimer)

	// create a buffer to copy the object body to
	var buf = make([]byte, payloadSize)

	// read the object body into the buffer
	size := 0
	for {
		n, err := dataStream.Read(buf)

		size += n

		if err == io.EOF {
			break
		}

		// if the streaming fails, exit
		if err != nil {
			panic("Error reading object body: " + err.Error())
		}
	}

	err = dataStream.Close()

	// measure the last byte latency
	latency.LastByte = time.Now().Sub(latencyTimer)

	// if the datastream can't be closed, exit
	if err != nil {
		panic("Error closing the datastream: " + err.Error())
	}

	return latency
}

func (op *OperationRead) CleanupTestdata(ctx *BenchmarkContext) {
	for _, key := range op.keys {
		ctx.Client.DeleteObject(ctx.Path, key)
	}
}
