# cassandra-spark
Test application to upload data into cassandra using spark

## Usage

To use the datastax cassandra connector for spark

    spark-submit --master <master> --class uk.co.pinpointlabs.App --input <path> --host <host> --keyspace <keyspace> --table <table>

To use the datastax cassandra connector using the CqlBulkOutputFormat (currently not working)

    spark-submit --master <master> --class uk.co.pinpointlabs.App --input <path> --host <host> --keyspace <keyspace> --table <table> --bulk
