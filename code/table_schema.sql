USE iceberg.db1;

CREATE TABLE kol_profile_stream (
  name            varchar,
  url             varchar,
  followers_count bigint,
  category        varchar,
  email_contact   varchar,
  phone_contact   varchar,
  web_contact     varchar,
  webpage         varchar,
  tiktok          varchar,
  youtube         varchar,
  instagram       varchar,
  threads         varchar,
  page_id         varchar,
  create_date     date,
  record_ts       timestamp,
  kafka_ts        timestamp
)
WITH (
  location = 'hdfs://namenode:9000/user/hive/warehouse/db1.db/kol_profile_stream'
);
