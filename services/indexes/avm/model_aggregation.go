package avm

import (
	"context"
	"database/sql"
	"github.com/ava-labs/ortelius/services/indexes/params"
	"github.com/gocraft/dbr/v2"
	"time"
)

type AvmAggregratesModel struct {
	AggregateTs       time.Time `json:"aggregateTs"`
	AssetId           string    `json:"assetId"`
	TransactionVolume string    `json:"transactionVolume"`
	TransactionCount  uint64    `json:"transactionCount"`
	AddressCount      uint64    `json:"addresCount"`
	AssetCount        uint64    `json:"assetCount"`
	OutputCount       uint64    `json:"outputCount"`
}

func PurgeOldAvmAssetAggregation(ctx context.Context, sess *dbr.Session, time time.Time) (sql.Result, error) {
	return sess.
		DeleteFrom("avm_asset_aggregation").
		Where("aggregate_ts < ?", time).
		ExecContext(ctx)
}

func UpdateAvmAssetAggregation(ctx context.Context, sess *dbr.Session, aggregates AvmAggregratesModel) (sql.Result, error) {
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

func InsertAvmAssetAggregation(ctx context.Context, sess *dbr.Session, aggregates AvmAggregratesModel) (sql.Result, error) {
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

func UpdateAvmAssetAggregationLiveStateTimestamp(ctx context.Context, sess dbr.SessionRunner, time time.Time) (sql.Result, error) {
	return sess.
		Update("avm_asset_aggregation_state").
		Set("created_at", time).
		Where("id = ? and created_at > ?", params.StateLiveId, time).
		ExecContext(ctx)
}

type AvmAssetAggregationState struct {
	Id               uint64    `json:"id"`
	CreatedAt        time.Time `json:"createdAt"`
	CurrentCreatedAt time.Time `json:"currentCreatedAt"`
}

func SelectAvmAssetAggregationState(ctx context.Context, sess *dbr.Session, id int) (AvmAssetAggregationState, error) {
	var tTx AvmAssetAggregationState
	err := sess.
		Select("id", "created_at", "current_created_at").
		From("avm_asset_aggregation_state").
		Where("id = ?", id).
		LoadOneContext(ctx, &tTx)
	return tTx, err
}

func InsertAvmAggregationState(ctx context.Context, sess *dbr.Session, transactionTs AvmAssetAggregationState) (sql.Result, error) {
	return sess.
		InsertInto("avm_asset_aggregation_state").
		Pair("id", params.StateBackupId).
		Pair("created_at", transactionTs.CreatedAt).
		Pair("current_created_at", transactionTs.CurrentCreatedAt).
		ExecContext(ctx)
}

func InsertAvmAssetAggregationState(ctx context.Context, sess *dbr.Session, id int, createdAt time.Time, currentCreatedAt time.Time) (sql.Result, error) {
	return sess.
		InsertInto("avm_asset_aggregation_state").
		Pair("id", id).
		Pair("created_at", createdAt).
		Pair("current_created_at", currentCreatedAt).
		ExecContext(ctx)
}
