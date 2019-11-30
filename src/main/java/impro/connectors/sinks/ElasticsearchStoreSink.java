package impro.connectors.sinks;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
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
    private final static String ES_HOST = "ec2-52-59-250-80.eu-central-1.compute.amazonaws.com";
    private final static int ES_PORT = 9200;
    private final static String ES_PROTOCOL = "http";

    private ElasticsearchSink<Tuple4<String, String, String ,String>> esSink;

    public ElasticsearchStoreSink() {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost(ES_HOST, ES_PORT, ES_PROTOCOL));

//        RestHighLevelClient client = new RestHighLevelClient(
//                RestClient.builder(new HttpHost(ES_HOST, 9200, "http")));

//        RequestOptions.Builder reqOptBuilder = RequestOptions.DEFAULT.toBuilder();
//        RequestOptions COMMON_OPTIONS = reqOptBuilder.build();


        ElasticsearchSink.Builder<Tuple4<String, String, String, String>> esSinkBuilder = new ElasticsearchSink.Builder<>(
        httpHosts,
        (Tuple4<String, String, String, String> element, RuntimeContext ctx, RequestIndexer indexer) -> {
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

        //esSink = new ElasticsearchSink<>(config, transports, new EventInserter()).name("ES_Sink");


        // provide a RestClientFactory for custom configuration on the internally created REST client
//        esSinkBuilder.setRestClientFactory(
//                restClientBuilder -> {
//                    restClientBuilder.setDefaultHeaders(...)
//                    restClientBuilder.setMaxRetryTimeoutMillis(...)
//                    restClientBuilder.setPathPrefix(...)
//                    restClientBuilder.setHttpClientConfigCallback(...)
//                }
//        );
    }

    private static IndexRequest createIndexRequest(Tuple4<String, String, String, String> event) {
        Map<String, String> json = new HashMap<>();
        json.put("event-type", event.f0);
        json.put("date", event.f1);
        json.put("organizations", event.f2);
        json.put("themes", event.f3);

        return Requests.indexRequest()
                .index("cpu-related-events")
                .type("impactful-events")
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

    public ElasticsearchSink<Tuple4<String, String, String, String>> getEsSink() {
        return esSink;
    }

}
