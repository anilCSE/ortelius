package testhelperlib

import "github.com/gocraft/dbr/v2"

type SqliteTestModels struct {
}

func (t *SqliteTestModels) CreateModels(c *dbr.Connection) error {
	_, err := c.Exec("create table avm_asset_aggregation_state( " +
		"id bigint unsigned not null primary key," +
		"created_at timestamp not null default current_timestamp," +
		"current_created_at timestamp not null default current_timestamp" +
		")")
	if err != nil {
		return err
	}

	_, err = c.Exec(
		"create table avm_asset_aggregation (" +
			"aggregate_ts             timestamp         not null," +
			"asset_id                 varchar(50)       not null," +
			"transaction_volume       DECIMAL(65)       default 0," +
			"transaction_count        bigint unsigned   default 0," +
			"address_count            bigint unsigned   default 0," +
			"asset_count              bigint unsigned   default 0," +
			"output_count             bigint unsigned   default 0," +
			"PRIMARY KEY(aggregate_ts DESC, asset_id)" +
			")",
	)
	if err != nil {
		return err
	}

	_, err = c.Exec(
		"create table avm_outputs" +
			"(" +
			"id                       varchar(50)       not null primary key," +
			"chain_id                 varchar(50)       not null," +
			"transaction_id           varchar(50)       not null," +
			"output_index             smallint unsigned not null," +
			"asset_id                 varchar(50)       not null," +
			"output_type              int unsigned      not null," +
			"amount                   bigint unsigned   not null," +
			"locktime                 int unsigned      not null," +
			"threshold                int unsigned      not null," +
			"redeemed_at              timestamp         null," +
			"redeeming_transaction_id varchar(50)       not null default \"\"," +
			"created_at               timestamp         not null default current_timestamp" +
			")")
	if err != nil {
		return err
	}

	_, err = c.Exec(
		"create table avm_output_addresses" +
			"(" +
			"output_id           varchar(50)    not null," +
			"address             varchar(50)    not null," +
			"redeeming_signature varbinary(128) null," +
			"created_at          timestamp      not null default current_timestamp" +
			")")
	if err != nil {
		return err
	}

	return nil
}
