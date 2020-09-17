package stream

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ava-labs/ortelius/services"
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

	tasker := ProducerTasker{connections: co}

	outputsAggregateOverride := func(ctx context.Context, sess *dbr.Session, aggregateTs time.Time) (*sql.Rows, error) {
		if sess == nil {
			return nil, fmt.Errorf("")
		}
		aggregateColumns = []string{
			"avm_outputs.created_at as aggregate_ts",
			"avm_outputs.asset_id",
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

	// override function to call my tables
	tasker.avmOutputs = outputsAggregateOverride

	ctx := context.Background()

	job := co.Stream().NewJob("producertasker")
	sess := co.DB().NewSession(job)

	aggregates := Aggregrates{}
	aggregates.AggregateTs = time.Now().Add(time.Duration(int64(tasker.ConstAggregateDeleteFrame().Milliseconds()+1)) * time.Millisecond)
	aggregates.AssetId = "futureasset"
	tasker.InsertAvmAssetAggregation(ctx, sess, aggregates)

	err = tasker.RefreshAggregates()
	if err != nil {
		t.Errorf("refresh failed %s", err.Error())
	}

	var transactionTsBackup TransactionTs
	sess.
		Select("id", "created_at", "current_created_at").
		From("avm_asset_aggregation_state").
		Where("id = ?", params.StateBackupId).
		LoadOneContext(ctx, &transactionTsBackup)

	var transactionTs TransactionTs
	sess.
		Select("id", "created_at", "current_created_at").
		From("avm_asset_aggregation_state").
		Where("id = ?", params.StateLiveId).
		LoadOneContext(ctx, &transactionTs)

	if transactionTs.Id != params.StateLiveId {
		t.Errorf("state live not created")
	}

	if transactionTsBackup.Id != 0 {
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
}
