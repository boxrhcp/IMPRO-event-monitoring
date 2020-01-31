package impro.main;

import impro.connectors.sinks.ElasticsearchStoreSink;
import impro.data.GDELTGkgData;
import impro.util.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.*;
import java.util.logging.Logger;


/**
 * --input ./src/main/resources/gkg_example_50.csv
 */

public class StreamingCEPMonitoringJob {
    private static Logger log = Logger.getGlobal();

    private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /* TODO If within 3 days there are more than 5 pieces of news about qualcomm, raise a warning in a new index.
     * Use CEP aggregation to do this. Explain it in the report. As Future work, we should justify why 3 days and
     * why 5 pieces of news.
     * */

    /* TODO Show a graph relating companies involved in a series of news and the bigger the involvement, the bigger
    * the circle in the graph or something like that. Measure involvement by number of appearances in the events.
    * */

    private static String rawChainSectionLabel = "raw-section";
    private static String[] rawChainLocationFilter = {
            "russia", "canada", "china", "united states", "norway", "france", "brazil", "india"
    };
    private static String[] rawChainThemesFilter = {
            "ARMEDCONFLICT","BAN","BLACK_MARKET","BLOCKADE","CEASEFIRE","CLOSURE","CORRUPTION","DELAY",
            "ECON_BANKRUPTCY","ECON_BOYCOTT","ECON_FREETRADE","ECON_NATIONALIZE","ECON_PRICECONTROL","ECON_SUBSIDIES",
            "ECON_TAXATION","ECON_TRADE_DISPUTE","ENV_CLIMATECHANGE","ENV_GREEN","ENV_METALS","ENV_MINING","FUELPRICES",
            "GRIEVANCES","HEALTH_PANDEMIC","INFO_HOAX","INFO_RUMOR","INFRASTRUCTURE_BAD_ROADS","LEGALIZE","LEGISLATION",
            "MANMADE_DISASTER" ,"MANMADE_DISASTER_IMPLIED","MOVEMENT_ENVIRONMENTAL","MOVEMENT_GENERAL","MOVEMENT_OTHER",
            "NATURAL_DISASTER","NEGOTIATIONS","NEW_CONSTRUCTION","ORGANIZED_CRIME","PIPELINE_INCIDENT",
            "POLITICAL_TURMOIL","POWER_OUTAGE","PRIVATIZATION","PROPERTY_RIGHTS","PROTEST","REBELLION","REBELS",
            "ROAD_INCIDENT","SANCTIONS","SEIGE","SELF_IDENTIFIED_ENVIRON_DISASTER","SELF_IDENTIFIED_HUMAN_RIGHTS",
            "SLFID_MINERAL_RESOURCES","SLFID_NATURAL_RESOURCES","SOC_SUSPICIOUSACTIVITIES","STATE_OF_EMERGENCY",
            "STRIKE","UNREST_CLOSINGBORDER","UNSAFE_WORK_ENVIRONMENT","VETO"
    };

    private static String intermediateChainSectionLabel = "intermediate-section";
    private static String[] intermediateChainOrganizationsFilter = {
            "samsung","tsmc","taiwan semiconductor manufacturing company","qualcomm"
    };
    private static String[] intermediateChainLocationFilter = {
            "korea", "taiwan", "china"
    };
    private static String[] intermediateChainThemesFilter = {
            "BAN","BLOCKADE","CLOSURE","CORRUPTION","DELAY","ECON_BANKRUPTCY","ECON_BOYCOTT","ECON_DEBT",
            "ECON_EARNINGSREPORT","ECON_ENTREPRENEURSHIP","ECON_FREETRADE","ECON_MONOPOLY","ECON_NATIONALIZE",
            "ECON_PRICECONTROL","ECON_STOCKMARKET","ECON_SUBSIDIES","ECON_TAXATION","ECON_TRADE_DISPUTE","ECON_UNIONS",
            "ENV_GREEN","FUELPRICES","GRIEVANCES","HEALTH_PANDEMIC","INFO_HOAX","INFO_RUMOR","INFRASTRUCTURE_BAD_ROADS",
            "LEGALIZE","LEGISLATION","MANMADE_DISASTER","MANMADE_DISASTER_IMPLIED","MOVEMENT_ENVIRONMENTAL",
            "MOVEMENT_GENERAL","MOVEMENT_OTHER","NATURAL_DISASTER","NEGOTIATIONS","NEW_CONSTRUCTION",
            "PIPELINE_INCIDENT","POLITICAL_TURMOIL","POWER_OUTAGE","PRIVATIZATION","PROPERTY_RIGHTS","PROTEST",
            "REBELLION","REBELS","ROAD_INCIDENT","SANCTIONS","SCANDAL","SEIGE","SELF_IDENTIFIED_ENVIRON_DISASTER",
            "SELF_IDENTIFIED_HUMAN_RIGHTS","SOC_SUSPICIOUSACTIVITIES","STATE_OF_EMERGENCY","STRIKE","TRANSPARENCY",
            "TRIAL","UNREST_CLOSINGBORDER","UNSAFE_WORK_ENVIRONMENT","VETO","WHISTLEBLOWER", "KILL", "WOUND",
            "GLOBAL_HEALTH"
    };

    private static String endChainSectionLabel = "end-section";
    private static String[] endChainOrganizationsFilter = { "qualcomm" };
    private static String[] endChainThemesFilter = {
            "BAN","CORRUPTION","CYBER_ATTACK","DELAY","ECON_BANKRUPTCY","ECON_BOYCOTT","ECON_DEBT",
            "ECON_EARNINGSREPORT","ECON_ENTREPRENEURSHIP","ECON_FREETRADE","ECON_MONOPOLY",
            "ECON_PRICECONTROL","ECON_STOCKMARKET","ECON_SUBSIDIES","ECON_TAXATION","ECON_TRADE_DISPUTE",
            "ECON_UNIONS","GRIEVANCES","INFO_HOAX","INFO_RUMOR","INTERNET_BLACKOUT","LEGALIZE",
            "LEGISLATION","NEGOTIATIONS","NEW_CONSTRUCTION","POLITICAL_TURMOIL","POWER_OUTAGE",
            "PROPERTY_RIGHTS","RESIGNATION","SANCTIONS","SCANDAL","STRIKE","TRANSPARENCY","TRIAL",
            "UNSAFE_WORK_ENVIRONMENT","VETO","WHISTLEBLOWER"
    };

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String inputPath = params.get("input");
        inputPath = inputPath == null || inputPath.equals("") ? "/home/impro/impro-gdelt-downloader/csv" : inputPath;

//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // GKG Table
        DataStream<GDELTGkgData> gdeltGkgData = env.readTextFile(inputPath)
                .map(new ParseGdeltGkgDataToBin())
                .assignTimestampsAndWatermarks(new GkgDataAssigner());

        ChainSection rawChain = new ChainSection();
        rawChain.setSectionLabel(rawChainSectionLabel);
        rawChain.setThemeFilter(rawChainThemesFilter);
        rawChain.setLocationFilter(rawChainLocationFilter);
        processChainSection(gdeltGkgData, rawChain);

        ChainSection intermediateChain = new ChainSection();
        intermediateChain.setSectionLabel(intermediateChainSectionLabel);
        intermediateChain.setOrganizationFilter(intermediateChainOrganizationsFilter);
        intermediateChain.setThemeFilter(intermediateChainThemesFilter);
        intermediateChain.setLocationFilter(intermediateChainLocationFilter);
        processChainSection(gdeltGkgData, intermediateChain);

        ChainSection endChain = new ChainSection();
        endChain.setSectionLabel(endChainSectionLabel);
        endChain.setOrganizationFilter(endChainOrganizationsFilter);
        endChain.setThemeFilter(endChainThemesFilter);
        processChainSection(gdeltGkgData, endChain);

        env.execute("CPU Events processing and ES storing");
    }

    private static void processChainSection(DataStream<GDELTGkgData> gdeltGkgData, ChainSection chainSection) {
        DataStream<GDELTGkgData> processedData = gdeltGkgData;

        // Filter organizations
        if (chainSection.getOrganizationFilter() != null) {
            processedData = gdeltGkgData.filter(new FilterOrganisations(chainSection.getOrganizationFilter()));
        }

        // Filter locations
        if (chainSection.getLocationFilter() != null) {
            processedData = gdeltGkgData.filter(new FilterLocation(chainSection.getLocationFilter()));
        }

        // Filter themes with CEP
        /* We have to extract the themes into an array first. Otherwise, Flink will throw an exception complaining that
        * the object chainSection is not serializable.
        * */
        String[] themesFilter = chainSection.getThemeFilter();
        Pattern<GDELTGkgData, ?> pattern = Pattern.<GDELTGkgData>begin("first")
            .where(new IterativeCondition<GDELTGkgData>() {

                @Override
                public boolean filter(GDELTGkgData event, Context<GDELTGkgData> context) {
                    if (themesFilter == null) {
                        return true;
                    } else {
                        String themes = event.getV1Themes();
                        return Arrays.stream(themesFilter).parallel().anyMatch(themes::contains)
                                && event.getV15Tone() <= 0;
                    }
                }
            });

        // Apply the defined CEP pattern to the data
        PatternStream<GDELTGkgData> relevantEvents = CEP.pattern(processedData, pattern);


        RelevantFields relevantFields = new RelevantFields(chainSection.getSectionLabel());
        DataStream<Tuple7<Date, String, String, Double, Location[], String[], String[]>> finalResults =
                relevantEvents.select(relevantFields);

        final OutputTag<Tuple3<Date, String, String>> locationsOutputTag = new OutputTag<Tuple3<Date, String, String>>("location-output"){};
        final OutputTag<Tuple3<Date, String, String>> organizationsOutputTag = new OutputTag<Tuple3<Date, String, String>>("organizations-output"){};
        final OutputTag<Tuple3<Date, String, String>> themesOutputTag = new OutputTag<Tuple3<Date, String, String>>("themes-output"){};

        SingleOutputStreamOperator<Void> voidStream = finalResults.process(sideOutput(locationsOutputTag, organizationsOutputTag, themesOutputTag));

        ElasticsearchStoreSink esStoreSink = new ElasticsearchStoreSink(chainSection.getSectionLabel());
        if (ElasticsearchStoreSink.isOnline()) {
            finalResults.addSink(esStoreSink.getEventsSink());
            voidStream.getSideOutput(locationsOutputTag).addSink(esStoreSink.getLocationsSink());
            voidStream.getSideOutput(organizationsOutputTag).addSink(esStoreSink.getOrganizationsSink());
            voidStream.getSideOutput(themesOutputTag).addSink(esStoreSink.getThemesSink());
        }
    }

    private static class RelevantFields implements PatternSelectFunction<GDELTGkgData, Tuple7<Date, String, String, Double, Location[], String[], String[]>> {
        private String section;

        public RelevantFields(String section) {
            this.section = section;
        }

        @Override
        public Tuple7<Date, String, String, Double, Location[], String[], String[]> select(Map<String, List<GDELTGkgData>> pattern) {
            GDELTGkgData first = pattern.get("first").get(0);

            return new Tuple7<Date, String, String, Double, Location[], String[], String[]>(
                    first.getV21Date(),
                    first.getGkgRecordId(),
                    this.section,
                    first.getV15Tone(),
                    Location.formatLocations(first.getV1Locations()),
                    Organization.formatOrganizations(first.getV1Organizations()),
                    Theme.formatThemes(first.getV1Themes()));
        }
    }

    public static class FilterOrganisations implements FilterFunction<GDELTGkgData> {
        private String[] organizations;

        public FilterOrganisations(String[] organizations) {
            this.organizations = organizations;
        }

        @Override
        public boolean filter(GDELTGkgData event) {
            String orgList = event.getV1Organizations();

            return Arrays.stream(this.organizations).parallel().anyMatch(orgList.toLowerCase()::contains);
        }
    }

    public static class FilterLocation implements FilterFunction<GDELTGkgData> {
        private String[] locations;

        public FilterLocation(String[] locations) {
            this.locations = locations;
        }

        @Override
        public boolean filter(GDELTGkgData event) {
            String orgList = event.getV1Locations();

            return Arrays.stream(this.locations).parallel().anyMatch(orgList.toLowerCase()::contains);
        }
    }
    
    private static ProcessFunction<Tuple7<Date, String, String, Double, Location[], String[], String[]>, Void> sideOutput(
            OutputTag<Tuple3<Date, String, String>> locationsOutputTag,
            OutputTag<Tuple3<Date, String, String>> organizationsOutputTag,
            OutputTag<Tuple3<Date, String, String>> themesOutputTag) {

        return new ProcessFunction<Tuple7<Date, String, String, Double, Location[], String[], String[]>, Void>() {
            @Override
            public void processElement(Tuple7<Date, String, String, Double, Location[], String[], String[]> event, Context context, Collector<Void> collector) throws Exception {
                // f4 locations
                Arrays.asList(event.f4).forEach(location -> {
                    Tuple3<Date, String, String> locationTuple = new Tuple3<>();
                    locationTuple.setField(event.f0, 0);
                    locationTuple.setField(event.f1, 1);
                    locationTuple.setField(location.getCode(), 2);

                    context.output(locationsOutputTag, locationTuple);
                });

                // f5 organizations
                Arrays.asList(event.f5).forEach(organization -> {
                    Tuple3<Date, String, String> organizationTuple = new Tuple3<>();
                    organizationTuple.setField(event.f0, 0);
                    organizationTuple.setField(event.f1, 1);
                    organizationTuple.setField(organization, 2);
                    context.output(organizationsOutputTag, organizationTuple);
                });

                // f6 themes
                Arrays.asList(event.f6).forEach(theme -> {
                    Tuple3<Date, String, String> themeTuple = new Tuple3<>();
                    themeTuple.setField(event.f0, 0);
                    themeTuple.setField(event.f1, 1);
                    themeTuple.setField(theme, 2);
                    context.output(themesOutputTag, themeTuple);
                });
            }
        };
    }

    static class GkgDataAssigner implements AssignerWithPunctuatedWatermarks<GDELTGkgData> {
        @Override
        public long extractTimestamp(GDELTGkgData event, long previousElementTimestamp) {
            return event.getTimeStampMs();
        }

        @Override
        public Watermark checkAndGetNextWatermark(GDELTGkgData event, long extractedTimestamp) {
            return new Watermark(extractedTimestamp - 20000);
//            return new Watermark(event.getV21Date().getTime());
        }
    }
}
