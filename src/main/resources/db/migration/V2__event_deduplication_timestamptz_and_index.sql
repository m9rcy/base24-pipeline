alter table event_deduplication
    alter column last_event_time type timestamptz using last_event_time at time zone 'UTC',
    alter column updated_at      type timestamptz using updated_at      at time zone 'UTC';

create index idx_event_deduplication_updated_at on event_deduplication (updated_at);
