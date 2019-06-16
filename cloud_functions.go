package tfl

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/tomwphillips/request"
	"github.com/tomwphillips/request/bigquery"
)

type pubsubMessage struct {
	Data []byte `json:"data"`
}

// GetURL receives PubSub message to get a URL and write to GCS
func GetURL(ctx context.Context, m pubsubMessage) error {
	i, err := request.DecodeInstruction(m.Data)
	if err != nil {
		return err
	}
	_, err = request.Execute(ctx, i)
	return err
}

type tflPrediction struct {
	ID                  string `json:"id"`
	OperationType       int    `json:"operationType"`
	VehicleID           string `json:"vehicleId"`
	NaptanID            string `json:"naptanId"`
	StationName         string `json:"stationName"`
	LineID              string `json:"lineId"`
	LineName            string `json:"lineName"`
	PlatformName        string `json:"platformName"`
	Direction           string `json:"direction"`
	Bearing             string `json:"bearing"`
	DestinationNaptanID string `json:"destinationNaptanId"`
	DestinationName     string `json:"destinationName"`
	Timestamp           string `json:"timestamp"`
	TimeToStation       int    `json:"timeToStation"`
	CurrentLocation     string `json:"currentLocation"`
	Towards             string `json:"towards"`
	ExpectedArrival     string `json:"expectedArrival"`
	TimeToLive          string `json:"timeToLive"`
	ModeName            string `json:"modeName"`
	Timing              tflTiming
}

type tflTiming struct {
	CountdownServerAdjustment string `json:"countdownServerAdjustment"`
	Source                    string `json:"source"`
	Insert                    string `json:"insert"`
	Read                      string `json:"read"`
	Sent                      string `json:"sent"`
	Received                  string `json:"received"`
}

var lineStatusProject = os.Getenv("LINE_STATUS_PROJECT")
var lineStatusDataset = os.Getenv("LINE_STATUS_DATASET")
var lineStatusTable = os.Getenv("LINE_STATUS_TABLE")
var lineStatusErrorBucket = os.Getenv("LINE_STATUS_ERROR_BUCKET")
var lineStatusSuccessBucket = os.Getenv("LINE_STATUS_SUCCESS_BUCKET")

// ProcessLineStatus consumes PubSub events about a GCS bucket containing files
// describing the tube line status written by ConsumeRequest
func ProcessLineStatus(ctx context.Context, e bigquery.GCSEvent) error {
	if !bigquery.GCSWriteEvent(ctx, e) {
		return nil
	}

	f, err := bigquery.Read(ctx, e.Bucket, e.Name)
	if err != nil {
		bigquery.SwitchBucket(ctx, e.Bucket, lineStatusErrorBucket, e.Name)
		return fmt.Errorf("Reading file from %+v: %v", e, err)
	}

	var p []tflPrediction
	if err = json.Unmarshal(f, &p); err != nil {
		bigquery.SwitchBucket(ctx, e.Bucket, lineStatusErrorBucket, e.Name)
		return err
	}

	client, err := bigquery.InitializeClient(ctx, lineStatusProject)
	defer client.Close()
	if err != nil {
		bigquery.SwitchBucket(ctx, e.Bucket, lineStatusErrorBucket, e.Name)
		return err
	}

	dataset, err := bigquery.InitializeDataset(ctx, client, lineStatusDataset)
	if err != nil {
		bigquery.SwitchBucket(ctx, e.Bucket, lineStatusErrorBucket, e.Name)
		return err
	}

	table, err := bigquery.InitializeTable(ctx, dataset, lineStatusTable, tflPrediction{})
	if err != nil {
		bigquery.SwitchBucket(ctx, e.Bucket, lineStatusErrorBucket, e.Name)
		return err
	}

	if err = bigquery.StreamRecords(ctx, table, p); err != nil {
		bigquery.SwitchBucket(ctx, e.Bucket, lineStatusErrorBucket, e.Name)
		return err
	}

	return bigquery.SwitchBucket(ctx, e.Bucket, lineStatusSuccessBucket, e.Name)
}
