# mkpipe-loader-clickhouse

ClickHouse loader plugin for [MkPipe](https://github.com/mkpipe-etl/mkpipe). Writes Spark DataFrames into ClickHouse tables using the **native `clickhouse-spark` connector**, which uses ClickHouse's binary HTTP protocol for efficient columnar inserts.

## Documentation

For more detailed documentation, please visit the [GitHub repository](https://github.com/mkpipe-etl/mkpipe).

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

---

## Connection Configuration

```yaml
connections:
  clickhouse_target:
    variant: clickhouse
    host: localhost
    port: 8123
    database: target_db
    user: default
    password: mypassword
```

---

## Table Configuration

```yaml
pipelines:
  - name: pg_to_clickhouse
    source: pg_source
    destination: clickhouse_target
    tables:
      - name: public.events
        target_name: stg_events
        replication_method: full
        batchsize: 50000
```

---

## Write Parallelism & Throughput

ClickHouse loader inherits from `JdbcLoader`. Two parameters control write performance:

```yaml
      - name: public.events
        target_name: stg_events
        replication_method: full
        batchsize: 50000        # rows per JDBC batch insert (default: 10000)
        write_partitions: 4     # coalesce DataFrame to N partitions before writing
```

### How they work

- **`batchsize`**: number of rows buffered before sending a single `INSERT` to ClickHouse. ClickHouse benefits greatly from large batches — use 50,000–500,000 for best throughput.
- **`write_partitions`**: calls `coalesce(N)` on the DataFrame before writing, reducing the number of concurrent JDBC connections. Useful when you have many Spark partitions and want to limit load on ClickHouse.

### Performance Notes

- ClickHouse is optimized for large bulk inserts. **`batchsize` is the most impactful parameter** — increase it as much as your driver memory allows.
- Avoid many small `write_partitions` (e.g. 1) as it reduces parallelism. A value of 4–8 balances load and throughput.
- ClickHouse's MergeTree engine merges parts in the background; very frequent small inserts create many parts and degrade query performance. Prefer fewer large batches.

---

## All Table Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Source table name |
| `target_name` | string | required | ClickHouse destination table name |
| `replication_method` | `full` / `incremental` | `full` | Replication strategy |
| `batchsize` | int | `10000` | Rows per JDBC batch insert |
| `write_partitions` | int | — | Coalesce DataFrame to N partitions before writing |
| `dedup_columns` | list | — | Columns used for `mkpipe_id` hash deduplication |
| `tags` | list | `[]` | Tags for selective pipeline execution |
| `pass_on_error` | bool | `false` | Skip table on error instead of failing |