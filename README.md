# cassandra-spark

Test application to upload data into cassandra using spark, example using either the [spark-cassandra-connector](https://github.com/datastax/spark-cassandra-connector) or the [CqlBulkOutputFormat](https://github.com/apache/cassandra/blob/cassandra-2.0.11/src/java/org/apache/cassandra/hadoop/cql3/CqlBulkOutputFormat.java). The code in this example is taken from Cassandra 2.0.11, but has the [patch](https://github.com/apache/cassandra/commit/708adca7d702ce2466e80c4bf83f3c5b7cecca46) by [Jonathan Ellis](https://github.com/jbellis) to a copy of the output format and record writer within this project.

## Notes

Sample data and a schema.cql is available under src/main/resources/data

## Usage

To use the datastax cassandra connector for spark

    spark-submit --master <master> --class uk.co.pinpointlabs.App --input <path> --host <host> --keyspace <keyspace> --table <table>

To use the datastax cassandra connector using the CqlBulkOutputFormat (currently not working)

    spark-submit --master <master> --class uk.co.pinpointlabs.App --input <path> --host <host> --keyspace <keyspace> --table <table> --bulk
