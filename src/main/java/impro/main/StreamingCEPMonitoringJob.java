package impro.main;

import impro.connectors.sinks.ElasticsearchStoreSink;
import jdk.internal.jline.internal.Log;
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

    public static Logger log = Logger.getGlobal();

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // GKG Table
        DataStream<GDELTGkgData> gdeltGkgData = env.readTextFile(params.get("input"))
                .map(new ParseGdeltGkgDataToBin())
                .assignTimestampsAndWatermarks(new GkgDataAssigner());

        DataStream<GDELTGkgData> GkgOrganizationsData = gdeltGkgData.filter(new FilterOrganisations());
        //GkgOrganizationsData.print();

        Pattern<GDELTGkgData, ?> warningPattern = Pattern.<GDELTGkgData>begin("first")
                .where(new IterativeCondition<GDELTGkgData>() {
                    @Override
                    public boolean filter(GDELTGkgData event, Context<GDELTGkgData> context) throws Exception {
                        String themes = event.getV1Themes();
                        Date eventDate = event.getV21Date();
                        Date start = new SimpleDateFormat("yyyyMMddHHmm").parse("201911290000");
                        Date end = new SimpleDateFormat("yyyyMMddHHmm").parse("201912012359");
                        if (( themes.contains("DELAY")
                                || themes.contains("BAN") || themes.contains("CORRUPTION") || (themes.contains("FUELPRICES"))
                                || themes.contains("GRIEVANCES") || themes.contains("INFO_HOAX") || themes.contains("INFO_RUMOR")
                                || themes.contains("LEGALIZE") || themes.contains("LEGISLATION") || themes.contains("MOVEMENT_OTHER")
                                || themes.contains("POWER_OUTAGE") || themes.contains("PROTEST") || themes.contains("SANCTIONS")
                                || themes.contains("SCANDAL") || themes.contains("SLFID_MINERAL_RESOURCES") || themes.contains("SLFID_NATURAL_RESOURCES")
                                || themes.contains("TRANSPARENCY") || themes.contains("TRIAL") || themes.contains("UNSAFE_WORK_ENVIRONMENT")
                                || themes.contains("WHISTLEBLOWER")|| themes.equals("ECON_BANKRUPTCY") || themes.contains("ECON_BOYCOTT")
                                || themes.contains("ECON_EARNINGSREPORT") || themes.contains("ECON_ENTREPRENEURSHIP") || themes.contains("ECON_FREETRADE")
                                || themes.contains("ECON_MONOPOLY") || themes.contains("ECON_PRICECONTROL") || themes.contains("ECON_STOCKMARKET")
                                || themes.contains("ECON_SUBSIDIES") || themes.contains("ECON_TAXATION") || themes.contains("ECON_TRADE_DISPUTE")
                                || themes.contains("ECON_UNIONS") || themes.contains("ENV_METALS") || themes.contains("ENV_MINING")
                                || themes.contains("NEGOTIATIONS")) && event.getV15Tone() <= 0 && start.compareTo(eventDate)<0
                                && end.compareTo(eventDate) > 0) {
                            //if (themes.contains("ECON_") ) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                }).within(Time.days(2)).timesOrMore(2);


        PatternStream<GDELTGkgData> patternMessageTypeWarning = CEP.pattern(GkgOrganizationsData, warningPattern);

        DataStream<Tuple5<String, String, String, String, String>> warnings = patternMessageTypeWarning.select(new GenerateMessageTypeWarning());

        //warnings.print();
        ElasticsearchStoreSink esStoreSink = new ElasticsearchStoreSink();
        warnings.addSink(esStoreSink.getEsSink());
        
        warnings.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);

        env.execute("CPU Events processing and storing");
    }

    private static class GenerateMessageTypeWarning implements PatternSelectFunction<GDELTGkgData, Tuple5<String, String, String, String, String>> {
        @Override
        public Tuple5<String, String, String, String, String> select(Map<String, List<GDELTGkgData>> pattern) {
            GDELTGkgData first = (GDELTGkgData) pattern.get("first").get(0);
            Date date = first.getV21Date();
            // filter the Themes
            String themes = first.getV1Themes();
            String orgThemes = "";
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
            }catch(Exception e){
                log.log(Level.WARNING,"Failed split, field themes is null");
            }
            // remove the last ,
            if (orgThemes.length() > 1)
                orgThemes = orgThemes.substring(0, orgThemes.length() - 2);

            //System.out.println("  WARN:" + first.toString());
            /*return new Tuple5<String, String, String, String, String>(date.toString(), first.getV1Organizations(),
                    orgThemes, first.getV21AllNames(), first.getV21Amounts());*/
            return new Tuple5<String, String, String, String, String>(date.toString(), first.getGkgRecordId(),first.getV1Organizations(),
                    orgThemes,""/*first.getV21AllNames(), first.getV21Amounts()*/);
        }
    }

    public static class FilterOrganisations implements FilterFunction<GDELTGkgData> {
        @Override
        public boolean filter(GDELTGkgData event) {
            String orgList = event.getV1Organizations();
            //Double tone = event.getV15Tone();
            if ((orgList.toLowerCase().contains("qualcomm") ||
                    orgList.toLowerCase().contains("snapdragon") ||
                    orgList.toLowerCase().contains("samsung") ||
                    orgList.toLowerCase().contains("atheros") ||
                    orgList.toLowerCase().contains("wilocity") ||
                    orgList.toLowerCase().contains("airgo") ||
                    orgList.toLowerCase().contains("nujira") ||
                    //orgList.toLowerCase().contains("exynnos") ||
                    orgList.toLowerCase().contains("tsmc")) ) {
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
            return new Watermark(extractedTimestamp - 20000);
            //return new Watermark(extractedTimestamp);
        }
    }

}

