/**
 * Copyright 2015
 */
package uk.co.pinpointlabs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlBulkOutputFormat;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import uk.co.pinpointlabs.function.MapDataToCQLBytesPair;
import uk.co.pinpointlabs.function.MapTextToData;
import uk.co.pinpointlabs.model.Data;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;

/**
 * Test application
 * 
 * @author Robert Bruce <rob@pinpointlabs.co.uk>
 */
public class App {
  /**
   * Command line options
   */
  private static final String INPUT_PATH = "i";
  private static final String OUTPUT_HOST = "h";
  private static final String OUTPUT_KEYSPACE = "k";
  private static final String OUTPUT_TABLE = "t";
  private static final String OUTPUT_BULK = "b";

  /**
   * Our insert statement
   */
  private static final String INSERT_STATEMENT = "INSERT INTO %s.%s (first, second) VALUES (?, ?) USING TIMESTAMP ?;";

  /**
   * Application entry point
   * 
   * @param args the command line arguments
   * @return status
   * @throws Exception
   */
  public static int main(String[] args) throws Exception {
    return new App().run(args);
  }

  /**
   * Run the application
   * 
   * @param args the command line arguments
   * @return status
   * @throws Exception
   */
  public int run(String[] args) throws Exception {
    CommandLine cli = parseArgs(args);

    SparkConf conf = new SparkConf(true);

    if (!cli.hasOption(OUTPUT_BULK)) {
      conf.set("spark.cassandra.connection.host", cli.getOptionValue(OUTPUT_HOST));
      conf.set("spark.cassandra.connection.rpc.port", "9160");
      conf.set("spark.cassandra.connection.native.port", "9042");
    }

    JavaSparkContext context = new JavaSparkContext(conf);

    if (cli.hasOption(OUTPUT_BULK)) {
      this.bulkSaveToCassandra(context, cli);
    } else {
      this.saveToCassandra(context, cli);
    }

    context.stop();

    return 0;
  }

  /**
   * Save the data to cassandra using the spark cassandra connector
   * 
   * @param context the spark context
   * @param cli the command line options
   */
  @SuppressWarnings("unchecked")
  protected void saveToCassandra(JavaSparkContext context, CommandLine cli) {
    JavaRDD<Data> data = context.textFile(cli.getOptionValue(INPUT_PATH))
        .flatMap(new MapTextToData());

    javaFunctions(data).writerBuilder(cli.getOptionValue(OUTPUT_KEYSPACE),
        cli.getOptionValue(OUTPUT_TABLE),
        mapToRow(Data.class)).saveToCassandra();
  }

  /**
   * Bulk save the data to cassandra using CqlBulkOutputFormat
   * 
   * @param context the spark context
   * @param cli the command line options
   * @throws Exception
   */
  protected void bulkSaveToCassandra(JavaSparkContext context, CommandLine cli)
      throws Exception {
    String host = cli.getOptionValue(OUTPUT_HOST);
    String keyspace = cli.getOptionValue(OUTPUT_KEYSPACE);
    String colFamily = cli.getOptionValue(OUTPUT_TABLE);
    
    // connect to the cluster to get metadata
    Cluster.Builder clusterBuilder = Cluster.builder();
    clusterBuilder.addContactPoints(host);
    Cluster cluster = clusterBuilder.build();
    
    Metadata clusterMetadata = cluster.getMetadata();
    KeyspaceMetadata keyspaceMetadata = clusterMetadata.getKeyspace(keyspace);
    TableMetadata tableMetadata = keyspaceMetadata.getTable(colFamily);
    
    String cqlSchema = tableMetadata.asCQLQuery();
    
    String partitionerClass = clusterMetadata.getPartitioner();
    Class.forName(partitionerClass);
      
    cluster.close();
    
    // setup the hadoop job
    Job job = Job.getInstance();
    
    ConfigHelper.setOutputInitialAddress(job.getConfiguration(), cli.getOptionValue(OUTPUT_HOST));
    ConfigHelper.setOutputRpcPort(job.getConfiguration(), "9160");
    ConfigHelper.setOutputColumnFamily(job.getConfiguration(), cli.getOptionValue(OUTPUT_KEYSPACE), cli.getOptionValue(OUTPUT_TABLE));
    
    job.getConfiguration().set("mapreduce.output.bulkoutputformat.buffersize", "64");

    CqlBulkOutputFormat.setColumnFamilySchema(job.getConfiguration(), colFamily, cqlSchema);
    CqlBulkOutputFormat.setColumnFamilyInsertStatement(job.getConfiguration(),
        colFamily,
        this.buildPreparedInsertStatement(cli));
    ConfigHelper.setOutputPartitioner(job.getConfiguration(), partitionerClass);
    
    job.setOutputKeyClass(ByteBuffer.class);
    job.setOutputValueClass(List.class);
    job.setOutputFormatClass(CqlBulkOutputFormat.class);
    
    context.textFile(cli.getOptionValue(INPUT_PATH))
        .flatMap(new MapTextToData())
        .mapToPair(new MapDataToCQLBytesPair())
        .saveAsNewAPIHadoopDataset(job.getConfiguration());
  }
  
  /**
   * Build a prepared statement
   * 
   * @param cli the command line options
   * @return the statement
   */
  protected String buildPreparedInsertStatement(CommandLine cli) {
    return String.format(
        INSERT_STATEMENT,
        cli.getOptionValue(OUTPUT_KEYSPACE),
        cli.getOptionValue(OUTPUT_TABLE));
  }

  /**
   * Parse the command line options
   * 
   * @param args
   * @throws ParseException
   */
  @SuppressWarnings("static-access")
  protected CommandLine parseArgs(String[] args) throws ParseException {
    Options options = new Options();

    options.addOption(OptionBuilder.withDescription("Path to input data; data is expected to be X,Y data")
        .withArgName("path")
        .withLongOpt("input")
        .hasArg()
        .isRequired()
        .create(INPUT_PATH));

    options.addOption(OptionBuilder.withDescription("The initial host to connect to")
        .withArgName("ip")
        .withLongOpt("host")
        .hasArg()
        .isRequired()
        .create(OUTPUT_HOST));

    options.addOption(OptionBuilder.withDescription("The keyspace to store data in")
        .withArgName("keyspace")
        .withLongOpt("keyspace")
        .hasArg()
        .isRequired()
        .create(OUTPUT_KEYSPACE));

    options.addOption(OptionBuilder.withDescription("The table to store data in")
        .withArgName("table")
        .withLongOpt("table")
        .hasArg()
        .isRequired()
        .create(OUTPUT_TABLE));

    options.addOption(OptionBuilder.withDescription("Load the data in bulk")
        .withLongOpt("bulk")
        .create(OUTPUT_BULK));

    GnuParser parser = new GnuParser();
    CommandLine commandLine = null;

    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException ex) {
      new HelpFormatter().printHelp("Usage:",
          ex.getMessage(),
          options,
          "",
          true);
      throw ex;
    }

    return commandLine;
  }
}
