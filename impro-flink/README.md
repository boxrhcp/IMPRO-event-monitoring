# IMPRO-Event-Monitoring
## Configure setup
To connect Flink with Elastic Stack the host and the port of Elastic Search are needed in the following file: https://github.com/boxrhcp/IMPRO-event-monitoring/blob/fa7981662878c51fca7a3b50a98e8dc81c55a1d8/impro-flink/src/main/java/impro/connectors/sinks/ElasticsearchStoreSink.java#L26

## Compile project
To compile the apache Flink process execute:
 
`mvn clean package`

## Run project
To run the process execute compiled package compiled (under the created dir "target") with the flag `--input` which needs a path to the directory where the files to process are.
A monitoring screen for Flink can be accessed via "localhost:8081".

## Useful links:
### Stream examples (Apache Flink github repo)
https://github.com/apache/flink/tree/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples



