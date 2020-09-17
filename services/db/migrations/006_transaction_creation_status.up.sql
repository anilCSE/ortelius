create table avm_asset_aggregation_state
(
    id                       bigint unsigned not null primary key,
    created_at               timestamp         not null default current_timestamp,
    current_created_at       timestamp         not null default current_timestamp
);

create table avm_asset_aggregation (
   aggregate_ts             timestamp         not null,
   asset_id                 varchar(50)       not null,
   transaction_volume       DECIMAL(65)       default 0,
   transaction_count        bigint unsigned   default 0,
   address_count            bigint unsigned   default 0,
   asset_count              bigint unsigned   default 0,
   output_count             bigint unsigned   default 0,
   PRIMARY KEY(aggregate_ts DESC, asset_id)
);

create index avm_asset_aggregation_asset_id_created_at on avm_asset_aggregation (asset_id, aggregate_ts DESC);