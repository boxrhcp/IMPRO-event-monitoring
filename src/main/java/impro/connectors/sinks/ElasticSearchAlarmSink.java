package impro.connectors.sinks;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

public class ElasticSearchAlarmSink {
    /*
    TODO: Make common parent class for AlarmSink and StoreSink
     */
    public static Logger log = Logger.getGlobal();

    private final static String ES_HOST = "localhost";
    private final static int ES_PORT = 9200;
    private final static String ES_PROTOCOL = "http";

    private final static String sinkName = "alarm";
    private ElasticsearchSink<Tuple3<Date,Date,Integer>> eventsSink;

    public ElasticSearchAlarmSink(String indexSubname) {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost(ES_HOST, ES_PORT, ES_PROTOCOL));

        ElasticsearchSink.Builder<Tuple3<Date,Date,Integer>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                (Tuple3<Date,Date,Integer>  element, RuntimeContext ctx, RequestIndexer indexer) -> {
                    indexer.add(createIndexRequest(indexSubname, element));
                });

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);

        this.eventsSink = esSinkBuilder.build();
    }

    private static IndexRequest createIndexRequest(String indexSubname, Tuple3<Date, Date, Integer> alarm) {
        Map<String, String> json = new HashMap<>();
        json.put("startDate", formatDate(alarm.f0));
        json.put("endDate", formatDate(alarm.f1));
        json.put("count", alarm.f2.toString());

        return Requests.indexRequest()
                .index(sinkName + "-" + indexSubname)
                .type("events")
                .source(json);
    }

    public ElasticsearchSink<Tuple3<Date, Date, Integer>> getEventsSink() {
        return eventsSink;
    }

    public static boolean isOnline () {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(ES_HOST, ES_PORT, ES_PROTOCOL)));

        ClusterHealthRequest request = new ClusterHealthRequest();

        try {
            log.info("Saving filtered events to the ES cluster.");
            client.cluster().health(request, RequestOptions.DEFAULT);
            return true;
        } catch (IOException e) {
            log.warning("Could not connect to the ES cluster. No data will be stored in ES.");
            return false;
        }
    }

    private static String formatDate(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.applyPattern("yyyy/MM/dd hh:mm:ss");

        return sdf.format(date);
    }
}
