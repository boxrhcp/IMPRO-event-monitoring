package impro.connectors.sinks;

import org.apache.flink.api.common.functions.RuntimeContext;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticsearchStoreSink {
    private final static String ES_HOST = "localhost";
    private final static int ES_PORT = 9200;
    private final static String ES_PROTOCOL = "http";

    private ElasticsearchSink<Tuple5<String, String, String, String, String>> esSink;

    public ElasticsearchStoreSink() {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost(ES_HOST, ES_PORT, ES_PROTOCOL));

        ElasticsearchSink.Builder<Tuple5<String, String, String, String, String>> esSinkBuilder = new ElasticsearchSink.Builder<>(
        httpHosts,
        (Tuple5<String, String, String, String, String> element, RuntimeContext ctx, RequestIndexer indexer) -> {
            indexer.add(createIndexRequest(element));
        });

//        ElasticsearchSink.Builder<Tuple4<String, String, String, String>> esSinkBuilder;
//        esSinkBuilder = new ElasticsearchSink.Builder<>(
//                httpHosts,
//                new ElasticsearchSinkFunction<Tuple4<String,String, String,String>>() {
//                    public IndexRequest createIndexRequest(Tuple4<String,String, String,String> event) {
//                        Map<String, String> json = new HashMap<>();
//                        json.put("event-type", event.f0);
//                        json.put("date", event.f1);
//                        json.put("organizations", event.f2);
//                        json.put("themes", event.f3);
//
//                        return Requests.indexRequest()
//                                .index("cpu-related-events")
//                                .source(json);
//                    }
//
//                    @Override
//                    public void process(Tuple4<String,String, String,String> event, RuntimeContext ctx, RequestIndexer indexer) {
//                        indexer.add(createIndexRequest(event));
//                    }
//                });

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);

        this.esSink = esSinkBuilder.build();
    }

    private static IndexRequest createIndexRequest(Tuple5<String, String, String, String, String> event) {
        Map<String, String> json = new HashMap<>();
        json.put("date", event.f0);
        json.put("recordId", event.f1);
        json.put("organizations", event.f2);
        json.put("themes", event.f3);
        json.put("section", event.f4);

        return Requests.indexRequest()
                .index("supply-chain-events")
                .type("disruption-events")
                .id(event.f1)
                .source(json);
    }

//    public class EventInserter implements ElasticsearchSinkFunction<Tuple4<String, String, String, String>> {
//
//        public IndexRequest createIndexRequest(Tuple4<String, String, String, String> event) {
//            Map<String, String> json = new HashMap<>();
//            json.put("event-type", event.f0);
//            json.put("date", event.f1);
//            json.put("organizations", event.f2);
//            json.put("themes", event.f3);
//
//            return Requests.indexRequest()
//                    .index("cpu-related-events")
//                    .type("impactful-events")
//                    .source(json);
//        }
//
//        @Override
//        public void process(Tuple4<String, String, String, String> event, RuntimeContext ctx, RequestIndexer indexer) {
//            indexer.add(createIndexRequest(event));
//        }
//    }

    public ElasticsearchSink<Tuple5<String, String, String, String, String>> getEsSink() {
        return esSink;
    }

    public boolean isOnline () {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(ES_HOST, ES_PORT, ES_PROTOCOL)));

        ClusterHealthRequest request = new ClusterHealthRequest();

        try {
            client.cluster().health(request, RequestOptions.DEFAULT);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

}
