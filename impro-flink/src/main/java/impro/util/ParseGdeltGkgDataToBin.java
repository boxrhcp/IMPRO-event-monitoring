package impro.util;


import com.typesafe.config.ConfigException;
import org.apache.flink.api.common.functions.RichMapFunction;
import impro.data.GDELTGkgData;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ParseGdeltGkgDataToBin extends RichMapFunction<String, GDELTGkgData> {
    private static final long serialVersionUID = 1L;
    public static Logger log = Logger.getGlobal();

    public ParseGdeltGkgDataToBin() {
    }

    @Override
    public GDELTGkgData map(String record) {

        String rawData = record;
        // System.out.println("         RECORD: " + record);
        String gkgRecordId = "";
        Date V21Date = new Date();
        String V2SourceCommonName = "";
        String V2DocumentIdentifier = "";
        String V1Themes = "";
        String V2EnhancedThemes = "";
        String V1Locations = "";
        String V2EnhancedLocations = "";
        String V1Persons = "";
        String V2EnhancedPersons = "";
        String V1Organizations = "";
        String V2EnhancedOrganizations = "";
        Double V15Tone = 0.0;
        String V21AllNames = "";
        String V21Amounts = "";
        try {
            String[] data = rawData.split("\\t");
            //System.out.println("DATA Length: " + data.length);

            gkgRecordId = data[0];
            V21Date = new SimpleDateFormat("yyyyMMddHHmmss").parse(data[1]);
            V2SourceCommonName = data[3];
            V2DocumentIdentifier = data[4];
            V1Themes = data[7];
            V2EnhancedThemes = data[8];
            V1Locations = data[9];
            V2EnhancedLocations = data[10];
            V1Persons = data[11];
            V2EnhancedPersons = data[12];
            V1Organizations = data[13];
            V2EnhancedOrganizations = data[14];
            V15Tone = Double.parseDouble(data[15].split(",")[0]);
            /*V21AllNames = data[23];
            V21Amounts = data[24];*/
        } catch (Exception e) {
            log.log(Level.WARNING, "Failed split, field themes is null");
        }


        return new GDELTGkgData("gdelt_gkg", gkgRecordId, V21Date, V2SourceCommonName, V2DocumentIdentifier,
                V1Themes, V2EnhancedThemes, V1Locations, V2EnhancedLocations, V1Persons, V2EnhancedPersons,
                V1Organizations, V2EnhancedOrganizations, V15Tone/*, V21AllNames, V21Amounts*/);

    }
}
