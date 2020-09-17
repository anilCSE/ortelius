package stream

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/db"
	"github.com/ava-labs/ortelius/services/indexes/params"
	"github.com/gocraft/dbr/v2"
	"sync"
	"time"

	"github.com/ava-labs/avalanchego/utils/logging"
	"github.com/ava-labs/ortelius/cfg"
)

var (
	aggregationTick      = 20 * time.Second
	aggregateDeleteFrame = (-1 * 24 * 366) * time.Hour
	timestampRollup      = 60
	aggregateColumns     = []string{
		fmt.Sprintf("FROM_UNIXTIME(floor(UNIX_TIMESTAMP(avm_outputs.created_at) / %d) * %d) as aggregate_ts", timestampRollup, timestampRollup),
		"avm_outputs.asset_id",
		"CAST(COALESCE(SUM(avm_outputs.amount), 0) AS CHAR) AS transaction_volume",
		"COUNT(DISTINCT(avm_outputs.transaction_id)) AS transaction_count",
		"COUNT(DISTINCT(avm_output_addresses.address)) AS address_count",
		"COUNT(DISTINCT(avm_outputs.asset_id)) AS asset_count",
		"COUNT(avm_outputs.id) AS output_count",
	}
)

type ProducerTasker struct {
	initlock    sync.RWMutex
	connections *services.Connections
	log         *logging.Log
	plock       sync.Mutex
	avmOutputs  func(ctx context.Context, sess *dbr.Session, aggregateTs time.Time) (*sql.Rows, error)
}

var producerTaskerInstance ProducerTasker

func initializeProducerTasker(conf cfg.Config, log *logging.Log) error {
	producerTaskerInstance.initlock.Lock()
	defer producerTaskerInstance.initlock.Unlock()

	if producerTaskerInstance.connections != nil {
		return nil
	}

	connections, err := services.NewConnectionsFromConfig(conf.Services)
	if err != nil {
		return err
	}

	producerTaskerInstance.connections = connections
	producerTaskerInstance.log = log
	producerTaskerInstance.Start()
	return nil
}

func (t *ProducerTasker) Start() {
	go initRefreshAggregatesTick(t)
}

type Aggregrates struct {
	AggregateTs       time.Time `json:"aggregateTs"`
	AssetId           string    `json:"assetId"`
	TransactionVolume string    `json:"transactionVolume"`
	TransactionCount  uint64    `json:"transactionCount"`
	AddressCount      uint64    `json:"addresCount"`
	AssetCount        uint64    `json:"assetCount"`
	OutputCount       uint64    `json:"outputCount"`
}

type TransactionTs struct {
	Id               uint64    `json:"id"`
	CreatedAt        time.Time `json:"createdAt"`
	CurrentCreatedAt time.Time `json:"currentCreatedAt"`
}

func (t *ProducerTasker) RefreshAggregates() error {
	t.plock.Lock()
	defer t.plock.Unlock()

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Minute)

	job := t.connections.Stream().NewJob("producertasker")
	sess := t.connections.DB().NewSession(job)

	var err error
	var transactionTs TransactionTs

	// initialize the assset_aggregation_state table with id=stateLiveId row.
	// if the row has not been created..
	// created at and current created at set to time(0), so the first run will re-build aggregates for the entire db.
	sess.
		InsertInto("avm_asset_aggregation_state").
		Pair("id", params.StateLiveId).
		Pair("created_at", time.Unix(1, 0)).
		Pair("current_created_at", time.Unix(1, 0)).
		ExecContext(ctx)

	// check if the backup row exists, if found we crashed from a previous run.
	var transactionTsBackup TransactionTs
	sess.
		Select("id", "created_at", "current_created_at").
		From("avm_asset_aggregation_state").
		Where("id = ?", params.StateBackupId).
		LoadOneContext(ctx, &transactionTsBackup)

	if transactionTsBackup.Id == uint64(params.StateBackupId) {
		// re-process from backup row..
		transactionTs.Id = transactionTsBackup.Id
		transactionTs.CreatedAt = transactionTsBackup.CreatedAt
		transactionTs.CurrentCreatedAt = transactionTsBackup.CurrentCreatedAt
	} else {
		// make a copy of the last created_at, and reset to now + 1 years in the future
		// we are using the db as an atomic swap...
		// current_created_at is set to the newest aggregation timestamp from the message queue.
		// and in the same update we reset created_at to a time in the future.
		// when we get new messages from the queue, they will execute the sql _after_ this update, and set created_at to an earlier date.
		_, err = sess.ExecContext(ctx, "update avm_asset_aggregation_state "+
			"set current_created_at=created_at, created_at=? "+
			"where id=?", params.StateLiveId, time.Now().Add((365*24)*time.Hour))
		if err != nil {
			t.log.Error("atomic swap %s", err.Error())
			return err
		}

		sess.
			Select("id", "created_at", "current_created_at").
			From("avm_asset_aggregation_state").
			Where("id = ?", params.StateLiveId).
			LoadOneContext(ctx, &transactionTs)

		// this is really bad, the state live row was not created..  we cannot proceed safely.
		if transactionTs.Id != params.StateLiveId {
			t.log.Error("unable to find live state")
			return err
		}

		// id=stateBackupId backup row - for crash recovery
		sess.
			InsertInto("avm_asset_aggregation_state").
			Pair("id", params.StateBackupId).
			Pair("created_at", transactionTs.CreatedAt).
			Pair("current_created_at", transactionTs.CurrentCreatedAt).
			ExecContext(ctx)

		// setup the transactionBackup so that it can be removed.
		// copy of the live forw.
		transactionTsBackup = transactionTs
		transactionTsBackup.Id = params.StateBackupId

		var transactionTsBackupCheck TransactionTs
		sess.
			Select("id", "created_at", "current_created_at").
			From("avm_asset_aggregation_state").
			Where("id = ?", params.StateBackupId).
			LoadOneContext(ctx, &transactionTsBackupCheck)

		// so for some reason the backup row was _not_ created.
		// it could be ours or others, but we still don't have one, which is bad.
		// so punt.
		if transactionTsBackupCheck.Id != params.StateBackupId {
			t.log.Error("unable to find a backup state")
			return err
		}
	}

	aggregateTs := transactionTs.CurrentCreatedAt

	// round to the nearest minute..
	roundedAggregateTs := aggregateTs.Round(1 * time.Minute)

	// if we rounded half up, then lets just step back 1 minute to avoid losing anything.
	// better to redo a minute than lose one.
	if roundedAggregateTs.After(aggregateTs) {
		aggregateTs = roundedAggregateTs.Add(-1 * time.Minute)
	} else {
		aggregateTs = roundedAggregateTs
	}

	var rows *sql.Rows
	if t.avmOutputs != nil {
		rows, err = t.avmOutputs(ctx, sess, aggregateTs)
	} else {
		rows, err = t.AvmOutputsAggregate(ctx, sess, aggregateTs)
	}

	if err != nil {
		t.log.Error("error query %s", err.Error())
		return err
	}

	for ok := rows.Next(); ok; ok = rows.Next() {
		var aggregates Aggregrates
		err = rows.Scan(&aggregates.AggregateTs,
			&aggregates.AssetId,
			&aggregates.TransactionVolume,
			&aggregates.TransactionCount,
			&aggregates.AddressCount,
			&aggregates.AssetCount,
			&aggregates.OutputCount)

		if aggregates.AggregateTs.After(aggregateTs) {
			aggregateTs = aggregates.AggregateTs
		}

		if err != nil {
			t.log.Error("row fetch %s", err.Error())
			return err
		}

		_, err := t.InsertAvmAssetAggregation(ctx, sess, aggregates)
		if db.ErrIsDuplicateEntryError(err) {
			_, err := t.UpdateAvmAssetAggregation(sess, ctx, aggregates)
			// the update failed.  (could be truncation?)... Punt..
			if err != nil {
				t.log.Error("update %s", err.Error())
				return err
			}
		} else
		// the insert failed, not a duplicate.  (could be truncation?)... Punt..
		if err != nil {
			t.log.Error("insert %s", err.Error())
			return err
		}
	}

	// everything worked, so we can wipe id=stateBackupId backup row
	// lets make sure our run created this row ..  so check for current_created_at match..
	// if we didn't create the row, the creator would delete it..
	// if things go really bad, then when the process restarts the row will be re-selected and deleted then..
	_, err = sess.
		DeleteFrom("avm_asset_aggregation_state").
		Where("id = ? and current_created_at = ?", params.StateBackupId, transactionTsBackup.CurrentCreatedAt).
		ExecContext(ctx)

	// delete aggregate data before aggregateDeleteFrame
	sess.
		DeleteFrom("avm_asset_aggregation").
		Where("aggregate_ts < ?", aggregateTs.Add(aggregateDeleteFrame)).
		ExecContext(ctx)

	t.log.Info("processed up to %s", aggregateTs.String())

	return nil
}

func (t *ProducerTasker) UpdateAvmAssetAggregation(sess *dbr.Session, ctx context.Context, aggregates Aggregrates) (sql.Result, error) {
	return sess.ExecContext(ctx, "update avm_asset_aggregation "+
		"set "+
		" transaction_volume=CONVERT(?,DECIMAL(65)),"+
		" transaction_count=?,"+
		" address_count=?,"+
		" asset_count=?,"+
		" output_count=? "+
		"where aggregate_ts = ? AND asset_id = ?",
		aggregates.TransactionVolume, // string -> converted to decimal in db
		aggregates.TransactionCount,
		aggregates.AddressCount,
		aggregates.OutputCount,
		aggregates.AssetCount,
		aggregates.AggregateTs,
		aggregates.AssetId)
}

func (t *ProducerTasker) InsertAvmAssetAggregation(ctx context.Context, sess *dbr.Session, aggregates Aggregrates) (sql.Result, error) {
	return sess.ExecContext(ctx, "insert into avm_asset_aggregation "+
		"(aggregate_ts,asset_id,transaction_volume,transaction_count,address_count,asset_count,output_count) "+
		"values (?,?,CONVERT(?,DECIMAL(65)),?,?,?,?)",
		aggregates.AggregateTs,
		aggregates.AssetId,
		aggregates.TransactionVolume, // string -> converted to decimal in db
		aggregates.TransactionCount,
		aggregates.AddressCount,
		aggregates.AssetCount,
		aggregates.OutputCount)
}

func (t *ProducerTasker) AvmOutputsAggregate(ctx context.Context, sess *dbr.Session, aggregateTs time.Time) (*sql.Rows, error) {
	rows, err := sess.
		Select(aggregateColumns...).
		From("avm_outputs").
		LeftJoin("avm_output_addresses", "avm_output_addresses.output_id = avm_outputs.id").
		GroupBy("aggregate_ts", "avm_outputs.asset_id").
		Where("avm_outputs.created_at >= ?", aggregateTs).
		RowsContext(ctx)
	return rows, err
}

func (t *ProducerTasker) ConstAggregateDeleteFrame() time.Duration {
	return aggregateDeleteFrame
}

func initRefreshAggregatesTick(t *ProducerTasker) {
	timer := time.NewTicker(aggregationTick)
	defer timer.Stop()

	t.RefreshAggregates()
	for range timer.C {
		t.RefreshAggregates()
	}
}
