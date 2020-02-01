package impro.connectors.sinks;

import impro.util.Location;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
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
import java.time.ZoneId;
import java.util.*;
import java.util.logging.Logger;

public class ElasticsearchStoreSink {
    public static Logger log = Logger.getGlobal();

    private final static String ES_HOST = "localhost";
    private final static int ES_PORT = 9200;
//    private final static int ES_PORT = 9201;

    private final static String ES_PROTOCOL = "http";

    private ElasticsearchSink<Tuple7<Date, String, String, Double, Location[], String[], String[]>> eventsSink;
    private ElasticsearchSink<Tuple3<Date,Date,Integer>> alarmSink;

    public ElasticsearchStoreSink(String indexSubname) {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost(ES_HOST, ES_PORT, ES_PROTOCOL));

        this.eventsSink = buildEventSink(httpHosts);
        this.alarmSink = buildAlarmSink(httpHosts, indexSubname);
    }

    public ElasticsearchSink<Tuple7<Date, String, String, Double, Location[], String[], String[]>> getEventsSink() {
        return eventsSink;
    }

    public ElasticsearchSink<Tuple3<Date,Date,Integer>> getAlarmSink() {
        return alarmSink;
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

    private static IndexRequest createEventIndexRequest(Tuple7<Date, String, String, Double, Location[], String[], String[]> event) {
        Map<String, Object> json = new HashMap<>();
        json.put("date", event.f0.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());
        json.put("recordId", event.f1);
        json.put("section", event.f2);
        json.put("tone", event.f3);
        json.put("locations", Location.getCountryCodes(Arrays.asList(event.f4)));
        json.put("organizations", event.f5);
        json.put("themes", event.f6);

        return Requests.indexRequest()
                .index("supply-chain-events")
                .type("events")
                .id(event.f1)
                .source(json);
    }

    private static IndexRequest createAlarmIndexRequest(String indexSubname, Tuple3<Date, Date, Integer> alarm) {
        Map<String, String> json = new HashMap<>();
        json.put("startDate", formatDate(alarm.f0));
        json.put("endDate", formatDate(alarm.f1));
        json.put("count", alarm.f2.toString());

        return Requests.indexRequest()
                .index("alarm" + "-" + indexSubname)
                .type("events")
                .source(json);
    }


    private ElasticsearchSink<Tuple7<Date, String, String, Double, Location[], String[], String[]>> buildEventSink(List<HttpHost> httpHosts) {

        ElasticsearchSink.Builder<Tuple7<Date, String, String, Double, Location[], String[], String[]>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                (Tuple7<Date, String, String, Double, Location[], String[], String[]> element, RuntimeContext ctx, RequestIndexer indexer) -> {
                    indexer.add(createEventIndexRequest(element));
                });

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);

        return esSinkBuilder.build();
    }

    private ElasticsearchSink<Tuple3<Date, Date, Integer> > buildAlarmSink(List<HttpHost> httpHosts, String indexSubname) {
        ElasticsearchSink.Builder<Tuple3<Date, Date, Integer> > esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                (Tuple3<Date, Date, Integer>  element, RuntimeContext ctx, RequestIndexer indexer) -> {
                    indexer.add(createAlarmIndexRequest(indexSubname, element));
                });

        // configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);

        return esSinkBuilder.build();
    }

}
