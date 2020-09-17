package stream

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/indexes/avm"
	"github.com/ava-labs/ortelius/services/indexes/params"
	"github.com/ava-labs/ortelius/testhelperlib"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/health"
	"github.com/mattn/go-sqlite3"
	"os"
	"testing"
	"time"
)

type EventReceiverTest struct {
}

func (*EventReceiverTest) Event(eventName string)                          {}
func (*EventReceiverTest) EventKv(eventName string, kvs map[string]string) {}
func (*EventReceiverTest) EventErr(eventName string, err error) error      { return nil }
func (*EventReceiverTest) EventErrKv(eventName string, err error, kvs map[string]string) error {
	return nil
}
func (*EventReceiverTest) Timing(eventName string, nanoseconds int64)                          {}
func (*EventReceiverTest) TimingKv(eventName string, nanoseconds int64, kvs map[string]string) {}

func TestParse(t *testing.T) {
	sqlite3.Version()

	var eventReceiver EventReceiverTest
	tmpdir := t.TempDir()
	defer os.RemoveAll(tmpdir)

	c, err := dbr.Open("sqlite3", fmt.Sprintf("%s/test.db", tmpdir), &eventReceiver)
	if err != nil {
		t.Errorf("open db %s", err.Error())
	}

	tm := testhelperlib.SqlliteModels{}
	err = tm.CreateModels(c)
	if err != nil {
		t.Errorf("create model %s", err.Error())
	}

	h := health.NewStream()

	co := services.NewConnections(h, c, nil)

	outputsAggregateOverride := func(ctx context.Context, sess *dbr.Session, aggregateTs time.Time) (*sql.Rows, error) {
		if sess == nil {
			return nil, fmt.Errorf("")
		}
		aggregateColumns = []string{
			"avm_outputs.created_at as aggregate_ts",
			"avm_outputs.asset_id",
			"CAST(COALESCE(SUM(avm_outputs.amount), 0) AS CHAR) AS transaction_volume",
			"COUNT(DISTINCT(avm_outputs.transaction_id)) AS transaction_count",
			"COUNT(DISTINCT(avm_output_addresses.address)) AS address_count",
			"COUNT(DISTINCT(avm_outputs.asset_id)) AS asset_count",
			"COUNT(avm_outputs.id) AS output_count",
		}

		return sess.
			Select(aggregateColumns...).
			From("avm_outputs").
			LeftJoin("avm_output_addresses", "avm_output_addresses.output_id = avm_outputs.id").
			GroupBy("aggregate_ts", "avm_outputs.asset_id").
			RowsContext(ctx)
	}

	aggregateMap := map[string]*avm.AvmAggregratesModel{}

	insertAvm := func(ctx context.Context, sess *dbr.Session, aggregates avm.AvmAggregratesModel) (sql.Result, error) {
		aggregateMap[aggregates.AggregateTs.String()+":"+aggregates.AssetId] = &aggregates
		return nil, nil
	}

	updateAvm := func(ctx context.Context, sess *dbr.Session, aggregates avm.AvmAggregratesModel) (sql.Result, error) {
		aggregateMap[aggregates.AggregateTs.String()+":"+aggregates.AssetId] = &aggregates
		return nil, nil
	}

	// produce an expected timestamp to test..
	timenow := time.Now().Round(1 * time.Minute)
	timeProducerFunc := func() time.Time {
		return timenow
	}

	tasker := ProducerTasker{connections: co,
		avmOutputsCursor:   outputsAggregateOverride,
		insertAvmAggregate: insertAvm,
		updateAvmAggregate: updateAvm,
		timeStampProducer:  timeProducerFunc,
	}

	// override function to call my tables
	tasker.avmOutputsCursor = outputsAggregateOverride

	ctx := context.Background()

	job := co.Stream().NewJob("producertasker")
	sess := co.DB().NewSession(job)

	pasttime := time.Now().Add(-5 * time.Hour).Round(1 * time.Minute).Add(1 * time.Second)

	sess.InsertInto("avm_outputs").
		Pair("id", "id1").
		Pair("chain_id", "cid").
		Pair("output_index", 1).
		Pair("output_type", 1).
		Pair("locktime", 1).
		Pair("threshold", 1).
		Pair("created_at", pasttime).
		Pair("asset_id", "testasset").
		Pair("amount", 100).
		Pair("transaction_id", 1).
		Exec()

	sess.InsertInto("avm_outputs").
		Pair("id", "id2").
		Pair("chain_id", "cid").
		Pair("output_index", 1).
		Pair("output_type", 1).
		Pair("locktime", 1).
		Pair("threshold", 1).
		Pair("created_at", pasttime).
		Pair("asset_id", "testasset").
		Pair("amount", 100).
		Pair("transaction_id", 1).
		Exec()

	aggregates := avm.AvmAggregratesModel{}
	aggregates.AggregateTs = time.Now().Add(time.Duration(int64(tasker.ConstAggregateDeleteFrame().Milliseconds()+1)) * time.Millisecond)
	aggregates.AssetId = "futureasset"
	avm.InsertAvmAssetAggregation(ctx, sess, aggregates)
	avm.UpdateAvmAssetAggregation(ctx, sess, aggregates)

	err = tasker.RefreshAggregates()
	if err != nil {
		t.Errorf("refresh failed %s", err.Error())
	}

	backupAggregationState, _ := avm.SelectAvmAssetAggregationState(ctx, sess, params.StateBackupId)
	liveAggregationState, _ := avm.SelectAvmAssetAggregationState(ctx, sess, params.StateLiveId)
	if liveAggregationState.Id != params.StateLiveId {
		t.Errorf("state live not created")
	}
	if !liveAggregationState.CreatedAt.Equal(timenow.Add(additionalHours)) {
		t.Errorf("state live createdat not reset to the future")
	}
	if backupAggregationState.Id != 0 {
		t.Errorf("state backup not removed")
	}

	var count uint64
	count = 999999
	sess.Select("count(*)").From("avm_asset_aggregation").
		Where("aggregate_ts < ?", time.Now().Add(tasker.ConstAggregateDeleteFrame())).
		Load(&count)
	if count != 0 {
		t.Errorf("future avm_asset not removed")
	}

	if len(aggregateMap) != 1 {
		t.Errorf("aggregate map row not created")
	}

	for _, aggregateMapValue := range aggregateMap {
		if aggregateMapValue.AssetId != "testasset" &&
			aggregateMapValue.TransactionVolume != "200" &&
			aggregateMapValue.TransactionCount != 1 &&
			aggregateMapValue.AssetCount != 2 {
			t.Errorf("aggregate map invalid")
		}
	}
}
