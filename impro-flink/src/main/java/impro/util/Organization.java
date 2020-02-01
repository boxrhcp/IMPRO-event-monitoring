package impro.util;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;

public class Organization {
    public static String[] formatOrganizations(String v1Organizations) {

        List<String> organizations = Arrays.asList(v1Organizations.split(";"));
        organizations.forEach(v1Organization -> {
            v1Organization = StringUtils.lowerCase(v1Organization);
            v1Organization = StringUtils.capitalize(v1Organization);
        });

        return organizations.toArray(new String[0]);
    }
}
