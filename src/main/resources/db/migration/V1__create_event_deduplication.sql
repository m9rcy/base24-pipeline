create table if not exists event_deduplication (
    domain          varchar(100) not null,
    dedupe_key      varchar(255) not null,
    hash_version    varchar(20)  not null,
    last_hash       varchar(128) not null,
    last_event_time timestamp    null,
    updated_at      timestamp    not null,
    primary key (domain, dedupe_key)
);
