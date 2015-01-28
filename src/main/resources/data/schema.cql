CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

CREATE TABLE test.data (
    first int,
    second int,
    PRIMARY KEY (first, second)
) WITH CLUSTERING ORDER BY (second ASC);
