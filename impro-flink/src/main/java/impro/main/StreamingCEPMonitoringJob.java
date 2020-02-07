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
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class StreamingCEPMonitoringJob {
    private static Logger log = Logger.getGlobal();

    private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        // check for alarms
        env.execute("CPU Events processing and ES storing");
    }

    private static void processChainSection(DataStream<GDELTGkgData> gdeltGkgData, ChainSection chainSection) {
        DataStream<GDELTGkgData> processedData = gdeltGkgData;

        // Filter
        processedData = gdeltGkgData.filter(
                new Filter(
                        chainSection.getOrganizationFilter(),
                        chainSection.getLocationFilter()));

//        // Filter organizations
//        if (chainSection.getOrganizationFilter() != null) {
//            processedData = gdeltGkgData.filter(new FilterOrganisations(chainSection.getOrganizationFilter()));
//        }
//
//        // Filter locations
//        if (chainSection.getLocationFilter() != null) {
//            processedData = gdeltGkgData.filter(new FilterLocation(chainSection.getLocationFilter()));
//        }

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

        AllWindowedStream windowedFinal = finalResults.windowAll(TumblingEventTimeWindows.of(Time.days(5)));
        DataStream<Tuple3<Date,Date,Integer>> countFinal = windowedFinal.apply(new CountFunction());


        ElasticsearchStoreSink esStoreSink = new ElasticsearchStoreSink(chainSection.getSectionLabel());

        if (ElasticsearchStoreSink.isOnline()) {
            finalResults.addSink(esStoreSink.getEventsSink());
            countFinal.addSink(esStoreSink.getAlarmSink());
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

    public static class Filter implements FilterFunction<GDELTGkgData> {
        private String[] organizations;
        private String[] locations;

        public Filter(String[] organizations, String[] locations) {
            this.organizations = organizations;
            this.locations = locations;
        }

        @Override
        public boolean filter(GDELTGkgData event) {
            String orgList = event.getV1Organizations();
            String locList = event.getV1Locations();

            boolean org = true;
            boolean loc = true;

            if (this.organizations != null) {
                org = Arrays.stream(this.organizations).parallel().anyMatch(orgList.toLowerCase()::contains);
            }

            if (this.locations != null) {
                loc = Arrays.stream(this.locations).parallel().anyMatch(locList.toLowerCase()::contains);
            }

            return org && loc;
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

    public static class CountFunction implements AllWindowFunction<Tuple7<Date, String, String, Double, Location[], String[], String[]>,
            Tuple3<Date,Date,Integer>, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<Tuple7<Date, String, String, Double, Location[], String[], String[]>> input, Collector<Tuple3<Date,Date,Integer>> out) {
            int count = 0;

            for (Tuple7<Date, String, String, Double, Location[], String[], String[]> in: input) {
                count++;
            }

            if(count >= 5)
            {
                Tuple3<Date, Date, Integer> resultCount = new Tuple3<Date,Date,Integer>(new Date(window.getStart()),
                        new Date(window.getEnd()),
                        count);

                out.collect(resultCount);
            }
        }
    }
}
