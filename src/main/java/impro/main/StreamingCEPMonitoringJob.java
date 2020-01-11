package impro.main;

import impro.connectors.sinks.ElasticsearchStoreSink;
import impro.data.GDELTGkgData;
import impro.util.ChainSection;
import impro.util.ParseGdeltGkgDataToBin;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;


/**
 * --input ./src/main/resources/gkg_example_50.csv
 */

public class StreamingCEPMonitoringJob {
    private static Logger log = Logger.getGlobal();

    private static String rawChainSectionLabel = "RAW SECTION";
    private static String[] rawChainOrganizationsFilter = {};
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

    private static String intermediateChainSectionLabel = "INTERMEDIATE SECTION";
    private static String[] intermediateChainOrganizationsFilter = {
            "samsung","tsmc","taiwan semiconductor manufacturing company","qualcomm"
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
            "TRIAL","UNREST_CLOSINGBORDER","UNSAFE_WORK_ENVIRONMENT","VETO","WHISTLEBLOWER"
    };

    private static String endChainSectionLabel = "END SECTION";
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

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // GKG Table
        DataStream<GDELTGkgData> gdeltGkgData = env.readTextFile(inputPath)
                .map(new ParseGdeltGkgDataToBin())
                .assignTimestampsAndWatermarks(new GkgDataAssigner());

        ChainSection rawChain = new ChainSection(
                rawChainSectionLabel,
                rawChainOrganizationsFilter,
                rawChainThemesFilter);
        processChainSection(gdeltGkgData, rawChain);

        ChainSection intermediateChain = new ChainSection(
                intermediateChainSectionLabel,
                intermediateChainOrganizationsFilter,
                intermediateChainThemesFilter);
        processChainSection(gdeltGkgData, intermediateChain);

        ChainSection endChain = new ChainSection(
                endChainSectionLabel,
                endChainOrganizationsFilter,
                endChainThemesFilter);
        processChainSection(gdeltGkgData, endChain);

        env.execute("CPU Events processing and ES storing");
    }

    private static void processChainSection(DataStream<GDELTGkgData> gdeltGkgData, ChainSection chainSection) {
        String[] themesFilter = chainSection.getChainThemesFilter();

        // Filter organizations
        DataStream<GDELTGkgData> GkgOrganizationsData =
                gdeltGkgData.filter(new FilterOrganisations(chainSection.getChainOrganizationsFilter()));

        // Filter themes with CEP
        Pattern<GDELTGkgData, ?> pattern = Pattern.<GDELTGkgData>begin("first")
                .where(new IterativeCondition<GDELTGkgData>() {

            @Override
            public boolean filter(GDELTGkgData event, Context<GDELTGkgData> context) {

                String themes = event.getV1Themes();
                return Arrays.stream(themesFilter).parallel().anyMatch(themes::contains)
                        && event.getV15Tone() <= 0;
            }
        });

        // Apply the defined pattern to the data
        PatternStream<GDELTGkgData> relevantEvents = CEP.pattern(GkgOrganizationsData, pattern);
        DataStream<Tuple5<String, String, String, String, String>> finalResults =
                relevantEvents.select(new RelevantFields(chainSection.getChainSectionLabel()));

        // Store the final results in Elasticsearch
        ElasticsearchStoreSink esStoreSink = new ElasticsearchStoreSink();
        if (esStoreSink.isOnline()) {
            finalResults.addSink(esStoreSink.getEsSink());
        }
    }

    private static class RelevantFields implements PatternSelectFunction<GDELTGkgData, Tuple5<String, String, String, String, String>> {
        private String section;

        public RelevantFields(String section) {
            this.section = section;
        }

        @Override
        public Tuple5<String, String, String, String, String> select(Map<String, List<GDELTGkgData>> pattern) {
            GDELTGkgData first = pattern.get("first").get(0);

            return new Tuple5<>(
                    first.getV21Date().toString(),
                    first.getGkgRecordId(),
                    first.getV1Organizations(),
                    first.getV1Themes(),
                    this.section/*, first.getV21AllNames(), first.getV21Amounts()*/);
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

    static class GkgDataAssigner implements AssignerWithPunctuatedWatermarks<GDELTGkgData> {
        @Override
        public long extractTimestamp(GDELTGkgData event, long previousElementTimestamp) {
            return event.getTimeStampMs();
        }

        @Override
        public Watermark checkAndGetNextWatermark(GDELTGkgData event, long extractedTimestamp) {
            // simply emit a watermark with every event
            return new Watermark(extractedTimestamp - 20000   );
            //return new Watermark(extractedTimestamp);
        }
    }

}