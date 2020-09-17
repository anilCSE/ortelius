package stream

import (
	"context"
	"errors"
	"fmt"
	"github.com/ava-labs/ortelius/services"
	"github.com/ava-labs/ortelius/services/db"
	"github.com/ava-labs/ortelius/services/params"
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

	err = producerTaskerInstance.Init(connections)
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

func (t *ProducerTasker) Init(connections *services.Connections) error {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	job := connections.Stream().NewJob("producertasker")
	sess := connections.DB().NewSession(job)

	// initialize the assset_aggregation_state table with id=stateLiveId row.
	sess.
		InsertInto("asset_aggregation_state").
		Pair("id", params.StateLiveId).
		Pair("created_at", time.Unix(1, 0)).
		Pair("current_created_at", time.Unix(1, 0)).
		ExecContext(ctx)

	// just make sure the row was created..
	// this happens once at boot up..
	var resp uint64
	_, err := sess.
		Select("count(*)").
		From("asset_aggregation_state").
		Where("id = ?", params.StateLiveId).
		LoadContext(ctx, &resp)
	if err != nil {
		return err
	}

	if resp < 1 {
		return errors.New("asset_aggregation_state failed")
	}

	return nil
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

func (t *ProducerTasker) RefreshAggregates() {
	t.plock.Lock()
	defer t.plock.Unlock()

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Minute)

	job := t.connections.Stream().NewJob("producertasker")
	sess := t.connections.DB().NewSession(job)

	var err error

	// make a copy of the last created_at, and reset to now + 1 years in the future
	// we are using the db as an atomic swap...
	// current_created_at is set to the newest aggregation timestamp from the message queue.
	// and in the same update we reset created_at to a future event.
	// when we get new messages from the queue, they will event _after_ this update, and set created_at to an earlier date.
	_, err = sess.ExecContext(ctx, "update asset_aggregation_state "+
		"set current_created_at=created_at, created_at=(CURRENT_TIMESTAMP()+INTERVAL 1 YEAR) "+
		"where id=?", params.StateLiveId)
	if err != nil {
		t.log.Error("atomic swap %s", err.Error())
		return
	}

	var transactionTs TransactionTs
	sess.
		Select("id", "created_at", "current_created_at").
		From("asset_aggregation_state").
		Where("id = ?", params.StateLiveId).
		LoadOneContext(ctx, &transactionTs)
	var transactionTs1 TransactionTs
	sess.
		Select("id", "created_at", "current_created_at").
		From("asset_aggregation_state").
		Where("id = ?", params.StateBackupId).
		LoadOneContext(ctx, &transactionTs1)

	// so see a state backup row (id=stateBackupId), which means another run is in process, or crashed.
	// take over the previous work...
	if transactionTs1.Id == uint64(params.StateBackupId) {
		transactionTs.Id = transactionTs1.Id
		transactionTs.CreatedAt = transactionTs1.CreatedAt
		transactionTs.CurrentCreatedAt = transactionTs1.CurrentCreatedAt
	} else {
		// id=stateBackupId backup row - for crash recovery
		sess.
			InsertInto("asset_aggregation_state").
			Pair("id", params.StateBackupId).
			Pair("created_at", transactionTs.CreatedAt).
			Pair("current_created_at", transactionTs.CurrentCreatedAt).
			ExecContext(ctx)
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

	rows, err := sess.
		Select(aggregateColumns...).
		From("avm_outputs").
		LeftJoin("avm_output_addresses", "avm_output_addresses.output_id = avm_outputs.id").
		GroupBy("aggregate_ts", "avm_outputs.asset_id").
		Where("avm_outputs.created_at >= ?", aggregateTs).
		RowsContext(ctx)

	if err != nil {
		t.log.Error("error query %s", err.Error())
		return
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
			return
		}

		_, err := sess.ExecContext(ctx, "insert into asset_aggregation "+
			"(aggregate_ts,asset_id,transaction_volume,transaction_count,address_count,asset_count,output_count) "+
			"values (?,?,CONVERT(?,DECIMAL(65)),?,?,?,?)",
			aggregates.AggregateTs,
			aggregates.AssetId,
			aggregates.TransactionVolume, // string -> converted to decimal in db
			aggregates.TransactionCount,
			aggregates.AddressCount,
			aggregates.AssetCount,
			aggregates.OutputCount)
		if db.ErrIsDuplicateEntryError(err) {
			_, err := sess.ExecContext(ctx, "update asset_aggregation "+
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
			if err != nil {
				t.log.Error("update %s", err.Error())
				return
			}
		} else if err != nil {
			t.log.Error("insert %s", err.Error())
			return
		}
	}

	// everything worked, so we can wipe id=stateBackupId backup row
	// lets make sure we created this row ..  so check for current_created_at.
	// if we didn't create the row, the creator would delete it..
	// if things go really bad, then when the process restarts the row will be re-selected and deleted then..
	sess.
		DeleteFrom("asset_aggregation_state").
		Where("id = ? and current_created_at = ?", params.StateBackupId, transactionTs1.CurrentCreatedAt).
		ExecContext(ctx)

	// delete aggregate data before aggregateDeleteFrame
	sess.
		DeleteFrom("asset_aggregation").
		Where("aggregate_ts < ?", aggregateTs.Add(aggregateDeleteFrame)).
		ExecContext(ctx)

	t.log.Info("processed up to %s", aggregateTs.String())
}

func initRefreshAggregatesTick(t *ProducerTasker) {
	timer := time.NewTicker(aggregationTick)
	defer timer.Stop()

	t.RefreshAggregates()
	for range timer.C {
		t.RefreshAggregates()
	}
}
