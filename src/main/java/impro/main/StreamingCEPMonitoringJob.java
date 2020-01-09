package impro.main;

import impro.connectors.sinks.ElasticsearchStoreSink;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import impro.data.GDELTGkgData;
import impro.util.ParseGdeltGkgDataToBin;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * --input ./src/main/resources/gkg_example_50.csv
 */

public class StreamingCEPMonitoringJob {
    public static String startDate = "201902150000";
    public static String endDate = "201902172359";

    public static Logger log = Logger.getGlobal();

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // GKG Table
        DataStream<GDELTGkgData> gdeltGkgData = env.readTextFile(params.get("input"))
                .map(new ParseGdeltGkgDataToBin())
                .assignTimestampsAndWatermarks(new GkgDataAssigner());
        rawMaterials(gdeltGkgData);
        intermediateChain(gdeltGkgData);
        endChain(gdeltGkgData);
        /*DataStream<GDELTGkgData> GkgOrganizationsData = gdeltGkgData.filter(new FilterOrganisations());
        //GkgOrganizationsData.print();

        Pattern<GDELTGkgData, ?> patternRaw = Pattern.<GDELTGkgData>begin("first")
                .where(new IterativeCondition<GDELTGkgData>() {
                    @Override
                    public boolean filter(GDELTGkgData event, Context<GDELTGkgData> context) throws Exception {
                        String themes = event.getV1Themes();
<<<<<<< HEAD
                        Date eventDate = event.getV21Date();
                        Date start = new SimpleDateFormat("yyyyMMddHHmm").parse("201911290000");
                        Date end = new SimpleDateFormat("yyyyMMddHHmm").parse("201912012359");
                        if (( themes.contains("DELAY")
=======
                        //if (themes.contains("ECON_") ) {
                        return themes.contains("ECON_") || themes.contains("ENV_") || themes.contains("DELAY")
>>>>>>> master
                                || themes.contains("BAN") || themes.contains("CORRUPTION") || (themes.contains("FUELPRICES"))
                                || themes.contains("GRIEVANCES") || themes.contains("INFO_HOAX") || themes.contains("INFO_RUMOR")
                                || themes.contains("LEGALIZE") || themes.contains("LEGISLATION") || themes.contains("MOVEMENT_OTHER")
                                || themes.contains("POWER_OUTAGE") || themes.contains("PROTEST") || themes.contains("SANCTIONS")
                                || themes.contains("SCANDAL") || themes.contains("SLFID_MINERAL_RESOURCES") || themes.contains("SLFID_NATURAL_RESOURCES")
                                || themes.contains("TRANSPARENCY") || themes.contains("TRIAL") || themes.contains("UNSAFE_WORK_ENVIRONMENT")
                                || themes.contains("WHISTLEBLOWER") || themes.equals("ECON_BANKRUPTCY") || themes.contains("ECON_BOYCOTT")
                                || themes.contains("ECON_EARNINGSREPORT") || themes.contains("ECON_ENTREPRENEURSHIP") || themes.contains("ECON_FREETRADE")
                                || themes.contains("ECON_MONOPOLY") || themes.contains("ECON_PRICECONTROL") || themes.contains("ECON_STOCKMARKET")
                                || themes.contains("ECON_SUBSIDIES") || themes.contains("ECON_TAXATION") || themes.contains("ECON_TRADE_DISPUTE")
                                || themes.contains("ECON_UNIONS") || themes.contains("ENV_METALS") || themes.contains("ENV_MINING")
<<<<<<< HEAD
                                || themes.contains("NEGOTIATIONS")) && event.getV15Tone() <= 0 && start.compareTo(eventDate)<0
                                && end.compareTo(eventDate) > 0) {
                            //if (themes.contains("ECON_") ) {
                            return true;
                        } else {
                            return false;
                        }
=======
                                || themes.contains("NEGOTIATIONS");
>>>>>>> master
                    }
                });


        PatternStream<GDELTGkgData> patternMessageTypeWarning = CEP.pattern(GkgOrganizationsData, patternRaw);

        DataStream<Tuple5<String, String, String, String, String>> warnings = patternMessageTypeWarning.select(new GenerateMessageTypeWarning());

        //warnings.print();
        ElasticsearchStoreSink esStoreSink = new ElasticsearchStoreSink();
        warnings.addSink(esStoreSink.getEsSink());


        warnings.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);*/
        env.execute("CPU Events processing and storing");
    }

    private static void rawMaterials(DataStream<GDELTGkgData> gdeltGkgData) {
        DataStream<GDELTGkgData> GkgOrganizationsData = gdeltGkgData;
        //GkgOrganizationsData.print();

        Pattern<GDELTGkgData, ?> patternRaw = Pattern.<GDELTGkgData>begin("first")
                .where(new IterativeCondition<GDELTGkgData>() {
                    @Override
                    public boolean filter(GDELTGkgData event, Context<GDELTGkgData> context) throws Exception {
                        String themes = event.getV1Themes();
                        Date eventDate = event.getV21Date();
                        Date start = new SimpleDateFormat("yyyyMMddHHmm").parse(startDate);
                        Date end = new SimpleDateFormat("yyyyMMddHHmm").parse(endDate);
                        if ((themes.contains("ARMEDCONFLICT") || themes.contains("BAN") || (themes.contains("BLACK_MARKET"))
                                || themes.contains("BLOCKADE") || themes.contains("CEASEFIRE") || themes.contains("CLOSURE")
                                || themes.contains("CORRUPTION") || themes.contains("DELAY") || themes.contains("ECON_BANKRUPTCY")
                                || themes.contains("ECON_BOYCOTT") || themes.contains("ECON_FREETRADE") || themes.contains("ECON_NATIONALIZE")
                                || themes.contains("ECON_PRICECONTROL") || themes.contains("ECON_SUBSIDIES") || themes.contains("ECON_TAXATION")
                                || themes.contains("ECON_TRADE_DISPUTE") || themes.contains("ENV_CLIMATECHANGE") || themes.contains("ENV_GREEN")
                                || themes.contains("ENV_METALS") || themes.equals("ENV_MINING") || themes.contains("FUELPRICES")
                                || themes.contains("GRIEVANCES") || themes.contains("HEALTH_PANDEMIC") || themes.contains("INFO_HOAX")
                                || themes.contains("INFO_RUMOR") || themes.contains("INFRASTRUCTURE_BAD_ROADS") || themes.contains("LEGALIZE")
                                || themes.contains("LEGISLATION") || themes.contains("MANMADE_DISASTER") || themes.contains("MANMADE_DISASTER_IMPLIED")
                                || themes.contains("MOVEMENT_ENVIRONMENTAL")|| themes.contains("MOVEMENT_GENERAL")|| themes.contains("MOVEMENT_OTHER")
                                || themes.contains("NATURAL_DISASTER")|| themes.contains("NEGOTIATIONS")|| themes.contains("NEW_CONSTRUCTION")
                                || themes.contains("ORGANIZED_CRIME")|| themes.contains("PIPELINE_INCIDENT")|| themes.contains("POLITICAL_TURMOIL")
                                || themes.contains("POWER_OUTAGE")|| themes.contains("PRIVATIZATION")|| themes.contains("PROPERTY_RIGHTS")
                                || themes.contains("PROTEST")|| themes.contains("REBELLION")|| themes.contains("REBELS")
                                || themes.contains("ROAD_INCIDENT")|| themes.contains("SANCTIONS")|| themes.contains("SEIGE")
                                || themes.contains("SELF_IDENTIFIED_ENVIRON_DISASTER")|| themes.contains("SELF_IDENTIFIED_HUMAN_RIGHTS")|| themes.contains("SLFID_MINERAL_RESOURCES")
                                || themes.contains("SLFID_NATURAL_RESOURCES")|| themes.contains("SOC_SUSPICIOUSACTIVITIES")|| themes.contains("STATE_OF_EMERGENCY")
                                || themes.contains("STRIKE")|| themes.contains("UNREST_CLOSINGBORDER")|| themes.contains("UNSAFE_WORK_ENVIRONMENT")
                                || themes.contains("VETO")) && event.getV15Tone() <= 0 && start.compareTo(eventDate) < 0
                                && end.compareTo(eventDate) > 0) {
                            //if (themes.contains("ECON_") ) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                });


        PatternStream<GDELTGkgData> patternMessageTypeWarning = CEP.pattern(GkgOrganizationsData, patternRaw);

        DataStream<Tuple5<String, String, String, String, String>> warnings = patternMessageTypeWarning.select(new GenerateMessageTypeRaw());

        ElasticsearchStoreSink esStoreSink = new ElasticsearchStoreSink();
        if (esStoreSink.isOnline()) {
            warnings.addSink(esStoreSink.getEsSink());
        }
        //warnings.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);

    }

    private static void intermediateChain(DataStream<GDELTGkgData> gdeltGkgData) {
        DataStream<GDELTGkgData> GkgOrganizationsData = gdeltGkgData.filter(new FilterOrganisationsIntermediate());
        //GkgOrganizationsData.print();

        Pattern<GDELTGkgData, ?> patternRaw = Pattern.<GDELTGkgData>begin("first")
                .where(new IterativeCondition<GDELTGkgData>() {
                    @Override
                    public boolean filter(GDELTGkgData event, Context<GDELTGkgData> context) throws Exception {
                        String themes = event.getV1Themes();
                        Date eventDate = event.getV21Date();
                        Date start = new SimpleDateFormat("yyyyMMddHHmm").parse(startDate);
                        Date end = new SimpleDateFormat("yyyyMMddHHmm").parse(endDate);
                        if ((themes.contains("BAN") || themes.contains("BLOCKADE") || (themes.contains("CLOSURE"))
                                || themes.contains("CORRUPTION") || themes.contains("DELAY") || themes.contains("ECON_BANKRUPTCY")
                                || themes.contains("ECON_BOYCOTT") || themes.contains("ECON_DEBT") || themes.contains("ECON_EARNINGSREPORT")
                                || themes.contains("ECON_ENTREPRENEURSHIP") || themes.contains("ECON_FREETRADE") || themes.contains("ECON_MONOPOLY")
                                || themes.contains("ECON_NATIONALIZE") || themes.contains("ECON_PRICECONTROL") || themes.contains("ECON_STOCKMARKET")
                                || themes.contains("ECON_SUBSIDIES") || themes.contains("ECON_TAXATION") || themes.contains("ECON_TRADE_DISPUTE")
                                || themes.contains("ECON_UNIONS") || themes.equals("ENV_GREEN") || themes.contains("FUELPRICES")
                                || themes.contains("GRIEVANCES") || themes.contains("HEALTH_PANDEMIC") || themes.contains("INFO_HOAX")
                                || themes.contains("INFO_RUMOR") || themes.contains("INFRASTRUCTURE_BAD_ROADS") || themes.contains("LEGALIZE")
                                || themes.contains("LEGISLATION") || themes.contains("MANMADE_DISASTER") || themes.contains("MANMADE_DISASTER_IMPLIED")
                                || themes.contains("MOVEMENT_ENVIRONMENTAL") || themes.contains("MOVEMENT_GENERAL")|| themes.contains("MOVEMENT_OTHER")
                                || themes.contains("NATURAL_DISASTER")|| themes.contains("NEGOTIATIONS")|| themes.contains("NEW_CONSTRUCTION")
                                || themes.contains("PIPELINE_INCIDENT")|| themes.contains("POLITICAL_TURMOIL")|| themes.contains("POWER_OUTAGE")
                                || themes.contains("PRIVATIZATION")|| themes.contains("PROPERTY_RIGHTS")|| themes.contains("PROTEST")
                                || themes.contains("REBELLION")|| themes.contains("REBELS")|| themes.contains("ROAD_INCIDENT")
                                || themes.contains("SANCTIONS")|| themes.contains("SCANDAL")|| themes.contains("SEIGE")
                                || themes.contains("SELF_IDENTIFIED_ENVIRON_DISASTER")|| themes.contains("SELF_IDENTIFIED_HUMAN_RIGHTS")|| themes.contains("SOC_SUSPICIOUSACTIVITIES")
                                || themes.contains("STATE_OF_EMERGENCY")|| themes.contains("STRIKE")|| themes.contains("TRANSPARENCY")
                                || themes.contains("TRIAL")|| themes.contains("UNREST_CLOSINGBORDER")|| themes.contains("UNSAFE_WORK_ENVIRONMENT")
                                || themes.contains("VETO")|| themes.contains("WHISTLEBLOWER")) && event.getV15Tone() <= 0 && start.compareTo(eventDate) < 0
                                && end.compareTo(eventDate) > 0) {
                            //if (themes.contains("ECON_") ) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                });


        PatternStream<GDELTGkgData> patternMessageTypeWarning = CEP.pattern(GkgOrganizationsData, patternRaw);

        DataStream<Tuple5<String, String, String, String, String>> warnings = patternMessageTypeWarning.select(new GenerateMessageTypeIntermediate());

        ElasticsearchStoreSink esStoreSink = new ElasticsearchStoreSink();
        if (esStoreSink.isOnline()) {
            warnings.addSink(esStoreSink.getEsSink());
        }
        //warnings.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);

    }

    private static void endChain(DataStream<GDELTGkgData> gdeltGkgData) {
        DataStream<GDELTGkgData> GkgOrganizationsData = gdeltGkgData.filter(new FilterOrganisationsEnd());
        //GkgOrganizationsData.print();

        Pattern<GDELTGkgData, ?> patternRaw = Pattern.<GDELTGkgData>begin("first")
                .where(new IterativeCondition<GDELTGkgData>() {
                    @Override
                    public boolean filter(GDELTGkgData event, Context<GDELTGkgData> context) throws Exception {
                        String themes = event.getV1Themes();
                        Date eventDate = event.getV21Date();
                        Date start = new SimpleDateFormat("yyyyMMddHHmm").parse(startDate);
                        Date end = new SimpleDateFormat("yyyyMMddHHmm").parse(endDate);
                        if ((themes.contains("BAN")|| themes.contains("CORRUPTION") || themes.contains("CYBER_ATTACK")
                                || themes.contains("DELAY") || themes.contains("ECON_BANKRUPTCY") || themes.contains("ECON_BOYCOTT")
                                || themes.contains("ECON_DEBT") || themes.contains("ECON_EARNINGSREPORT") || themes.contains("ECON_ENTREPRENEURSHIP")
                                || themes.contains("ECON_FREETRADE") || themes.contains("ECON_MONOPOLY") || themes.contains("ECON_PRICECONTROL")
                                || themes.contains("ECON_STOCKMARKET") || themes.contains("ECON_SUBSIDIES") || themes.contains("ECON_TAXATION")
                                || themes.contains("ECON_TRADE_DISPUTE")
                                || themes.contains("ECON_UNIONS") || themes.equals("GRIEVANCES") || themes.contains("INFO_HOAX")
                                || themes.contains("INFO_RUMOR") || themes.contains("INTERNET_BLACKOUT") || themes.contains("LEGALIZE")
                                || themes.contains("LEGISLATION") || themes.contains("NEGOTIATIONS") || themes.contains("NEW_CONSTRUCTION")
                                || themes.contains("POLITICAL_TURMOIL") || themes.contains("POWER_OUTAGE") || themes.contains("PROPERTY_RIGHTS")
                                || themes.contains("RESIGNATION") || themes.contains("SANCTIONS")|| themes.contains("SCANDAL")
                                || themes.contains("STRIKE")|| themes.contains("TRANSPARENCY")|| themes.contains("TRIAL")
                                || themes.contains("UNSAFE_WORK_ENVIRONMENT")|| themes.contains("VETO")|| themes.contains("WHISTLEBLOWER")
                        ) && event.getV15Tone() <= 0 && start.compareTo(eventDate) < 0
                                && end.compareTo(eventDate) > 0) {
                            //if (themes.contains("ECON_") ) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                });


        PatternStream<GDELTGkgData> patternMessageTypeWarning = CEP.pattern(GkgOrganizationsData, patternRaw);

        DataStream<Tuple5<String, String, String, String, String>> warnings = patternMessageTypeWarning.select(new GenerateMessageTypeEnd());

        ElasticsearchStoreSink esStoreSink = new ElasticsearchStoreSink();
        if (esStoreSink.isOnline()) {
            warnings.addSink(esStoreSink.getEsSink());
        }
        //warnings.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);

    }

    private static class GenerateMessageTypeRaw implements PatternSelectFunction<GDELTGkgData, Tuple5<String, String, String, String, String>> {
        @Override
        public Tuple5<String, String, String, String, String> select(Map<String, List<GDELTGkgData>> pattern) {
            GDELTGkgData first = (GDELTGkgData) pattern.get("first").get(0);
            Date date = first.getV21Date();
            // filter the Themes
            String themes = first.getV1Themes();
            /*String orgThemes = "";
            try {
                for (String theme : themes.split(";")) {
                    if (theme.equals("DELAY") || theme.equals("BAN") || theme.equals("CORRUPTION") || (theme.equals("FUELPRICES"))
                            || theme.equals("GRIEVANCES") || theme.equals("INFO_HOAX") || theme.equals("INFO_RUMOR")
                            || theme.equals("LEGALIZE") || theme.equals("LEGISLATION") || theme.equals("MOVEMENT_OTHER")
                            || theme.equals("POWER_OUTAGE") || theme.equals("PROTEST") || theme.equals("SANCTIONS")
                            || theme.equals("SCANDAL") || theme.equals("SLFID_MINERAL_RESOURCES") || theme.equals("SLFID_NATURAL_RESOURCES")
                            || theme.equals("TRANSPARENCY") || theme.equals("TRIAL") || theme.equals("UNSAFE_WORK_ENVIRONMENT")
                            || theme.equals("WHISTLEBLOWER") || theme.equals("ECON_BANKRUPTCY") || theme.equals("ECON_BOYCOTT")
                            || theme.equals("ECON_EARNINGSREPORT") || theme.equals("ECON_ENTREPRENEURSHIP") || theme.equals("ECON_FREETRADE")
                            || theme.equals("ECON_MONOPOLY") || theme.equals("ECON_PRICECONTROL") || theme.equals("ECON_STOCKMARKET")
                            || theme.equals("ECON_SUBSIDIES") || theme.equals("ECON_TAXATION") || theme.equals("ECON_TRADE_DISPUTE")
                            || theme.equals("ECON_UNIONS") || theme.equals("ENV_METALS") || theme.equals("ENV_MINING") || theme.equals("NEGOTIATIONS")) {
                        orgThemes = orgThemes + theme + ", ";
                    }
                }
            } catch (Exception e) {
                log.log(Level.WARNING, "Failed split, field themes is null");
            }
            // remove the last ,
            if (orgThemes.length() > 1)
                orgThemes = orgThemes.substring(0, orgThemes.length() - 2);
            */
            //System.out.println("  WARN:" + first.toString());
            /*return new Tuple5<String, String, String, String, String>(date.toString(), first.getV1Organizations(),
                    orgThemes, first.getV21AllNames(), first.getV21Amounts());*/
            return new Tuple5<String, String, String, String, String>(date.toString(), first.getGkgRecordId(), first.getV1Organizations(),
                    themes, "RAW SECTION"/*first.getV21AllNames(), first.getV21Amounts()*/);
        }
    }


    private static class GenerateMessageTypeIntermediate implements PatternSelectFunction<GDELTGkgData, Tuple5<String, String, String, String, String>> {
        @Override
        public Tuple5<String, String, String, String, String> select(Map<String, List<GDELTGkgData>> pattern) {
            GDELTGkgData first = (GDELTGkgData) pattern.get("first").get(0);
            Date date = first.getV21Date();
            // filter the Themes
            String themes = first.getV1Themes();
            /*String orgThemes = "";
            try {
                for (String theme : themes.split(";")) {
                    if (theme.equals("DELAY") || theme.equals("BAN") || theme.equals("CORRUPTION") || (theme.equals("FUELPRICES"))
                            || theme.equals("GRIEVANCES") || theme.equals("INFO_HOAX") || theme.equals("INFO_RUMOR")
                            || theme.equals("LEGALIZE") || theme.equals("LEGISLATION") || theme.equals("MOVEMENT_OTHER")
                            || theme.equals("POWER_OUTAGE") || theme.equals("PROTEST") || theme.equals("SANCTIONS")
                            || theme.equals("SCANDAL") || theme.equals("SLFID_MINERAL_RESOURCES") || theme.equals("SLFID_NATURAL_RESOURCES")
                            || theme.equals("TRANSPARENCY") || theme.equals("TRIAL") || theme.equals("UNSAFE_WORK_ENVIRONMENT")
                            || theme.equals("WHISTLEBLOWER") || theme.equals("ECON_BANKRUPTCY") || theme.equals("ECON_BOYCOTT")
                            || theme.equals("ECON_EARNINGSREPORT") || theme.equals("ECON_ENTREPRENEURSHIP") || theme.equals("ECON_FREETRADE")
                            || theme.equals("ECON_MONOPOLY") || theme.equals("ECON_PRICECONTROL") || theme.equals("ECON_STOCKMARKET")
                            || theme.equals("ECON_SUBSIDIES") || theme.equals("ECON_TAXATION") || theme.equals("ECON_TRADE_DISPUTE")
                            || theme.equals("ECON_UNIONS") || theme.equals("ENV_METALS") || theme.equals("ENV_MINING") || theme.equals("NEGOTIATIONS")) {
                        orgThemes = orgThemes + theme + ", ";
                    }
                }
            } catch (Exception e) {
                log.log(Level.WARNING, "Failed split, field themes is null");
            }
            // remove the last ,
            if (orgThemes.length() > 1)
                orgThemes = orgThemes.substring(0, orgThemes.length() - 2);
            */
            //System.out.println("  WARN:" + first.toString());
            /*return new Tuple5<String, String, String, String, String>(date.toString(), first.getV1Organizations(),
                    orgThemes, first.getV21AllNames(), first.getV21Amounts());*/
            return new Tuple5<String, String, String, String, String>(date.toString(), first.getGkgRecordId(), first.getV1Organizations(),
                    themes, "INTERMEDIATE SECTION"/*first.getV21AllNames(), first.getV21Amounts()*/);
        }
    }

    private static class GenerateMessageTypeEnd implements PatternSelectFunction<GDELTGkgData, Tuple5<String, String, String, String, String>> {
        @Override
        public Tuple5<String, String, String, String, String> select(Map<String, List<GDELTGkgData>> pattern) {
            GDELTGkgData first = (GDELTGkgData) pattern.get("first").get(0);
            Date date = first.getV21Date();
            // filter the Themes
            String themes = first.getV1Themes();
            /*String orgThemes = "";
            try {
                for (String theme : themes.split(";")) {
                    if (theme.equals("DELAY") || theme.equals("BAN") || theme.equals("CORRUPTION") || (theme.equals("FUELPRICES"))
                            || theme.equals("GRIEVANCES") || theme.equals("INFO_HOAX") || theme.equals("INFO_RUMOR")
                            || theme.equals("LEGALIZE") || theme.equals("LEGISLATION") || theme.equals("MOVEMENT_OTHER")
                            || theme.equals("POWER_OUTAGE") || theme.equals("PROTEST") || theme.equals("SANCTIONS")
                            || theme.equals("SCANDAL") || theme.equals("SLFID_MINERAL_RESOURCES") || theme.equals("SLFID_NATURAL_RESOURCES")
                            || theme.equals("TRANSPARENCY") || theme.equals("TRIAL") || theme.equals("UNSAFE_WORK_ENVIRONMENT")
                            || theme.equals("WHISTLEBLOWER") || theme.equals("ECON_BANKRUPTCY") || theme.equals("ECON_BOYCOTT")
                            || theme.equals("ECON_EARNINGSREPORT") || theme.equals("ECON_ENTREPRENEURSHIP") || theme.equals("ECON_FREETRADE")
                            || theme.equals("ECON_MONOPOLY") || theme.equals("ECON_PRICECONTROL") || theme.equals("ECON_STOCKMARKET")
                            || theme.equals("ECON_SUBSIDIES") || theme.equals("ECON_TAXATION") || theme.equals("ECON_TRADE_DISPUTE")
                            || theme.equals("ECON_UNIONS") || theme.equals("ENV_METALS") || theme.equals("ENV_MINING") || theme.equals("NEGOTIATIONS")) {
                        orgThemes = orgThemes + theme + ", ";
                    }
                }
            } catch (Exception e) {
                log.log(Level.WARNING, "Failed split, field themes is null");
            }
            // remove the last ,
            if (orgThemes.length() > 1)
                orgThemes = orgThemes.substring(0, orgThemes.length() - 2);
            */
            //System.out.println("  WARN:" + first.toString());
            /*return new Tuple5<String, String, String, String, String>(date.toString(), first.getV1Organizations(),
                    orgThemes, first.getV21AllNames(), first.getV21Amounts());*/
            return new Tuple5<String, String, String, String, String>(date.toString(), first.getGkgRecordId(), first.getV1Organizations(),
                    themes, "END SECTION"/*first.getV21AllNames(), first.getV21Amounts()*/);
        }
    }

    public static class FilterOrganisationsIntermediate implements FilterFunction<GDELTGkgData> {
        @Override
        public boolean filter(GDELTGkgData event) {
            String orgList = event.getV1Organizations();
            //Double tone = event.getV15Tone();
            if ((orgList.toLowerCase().contains("samsung") ||
                    orgList.toLowerCase().contains("tsmc") ||
                    orgList.toLowerCase().contains("taiwan semiconductor manufacturing company") ||
                    orgList.toLowerCase().contains("qualcomm"))) {
                return true;
            } else {
                return false;
            }
        }
    }

    public static class FilterOrganisationsEnd implements FilterFunction<GDELTGkgData> {
        @Override
        public boolean filter(GDELTGkgData event) {
            String orgList = event.getV1Organizations();
            //Double tone = event.getV15Tone();
            if ((orgList.toLowerCase().contains("qualcomm"))) {
                return true;
            } else {
                return false;
            }
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

