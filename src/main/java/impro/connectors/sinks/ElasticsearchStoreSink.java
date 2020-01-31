package impro.connectors.sinks;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
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

public class ElasticsearchStoreSink {
    public static Logger log = Logger.getGlobal();

    private final static String ES_HOST = "localhost";
    private final static int ES_PORT = 9200;
    private final static String ES_PROTOCOL = "http";

    private ElasticsearchSink<Tuple6<Date, String, String, String, String, String>> eventsSink;

    public ElasticsearchStoreSink(String indexSubname) {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost(ES_HOST, ES_PORT, ES_PROTOCOL));

        ElasticsearchSink.Builder<Tuple6<Date, String, String, String, String, String>> esSinkBuilder = new ElasticsearchSink.Builder<>(
        httpHosts,
        (Tuple6<Date, String, String, String, String, String> element, RuntimeContext ctx, RequestIndexer indexer) -> {
            indexer.add(createIndexRequest(indexSubname, element));
        });

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);

        this.eventsSink = esSinkBuilder.build();
    }

    private static IndexRequest createIndexRequest(String indexSubname, Tuple6<Date, String, String, String, String, String> event) {
        Map<String, String> json = new HashMap<>();
        json.put("date", formatDate(event.f0));
        json.put("recordId", event.f1);
        json.put("section", event.f2);
        json.put("locations", event.f3);
        json.put("organizations", event.f4);
        json.put("themes", event.f5);

        return Requests.indexRequest()
                .index("supply-chain-events-" + indexSubname)
                .type("events")
                .id(event.f1)
                .source(json);
    }

    public ElasticsearchSink<Tuple6<Date, String, String, String, String, String>> getEventsSink() {
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
