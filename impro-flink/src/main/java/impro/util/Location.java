package impro.util;

import org.elasticsearch.common.geo.GeoPoint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Location {
    private String original;
    private String name;
    private String code;
    private Integer type;
    private GeoPoint geoPoint;

    public Location() {}

    public String getOriginal() {
        return original;
    }

    public void setOriginal(String original) {
        this.original = original;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }

    public GeoPoint getGeoPoint() {
        return geoPoint;
    }

    public void setGeoPoint(GeoPoint geoPoint) {
        this.geoPoint = geoPoint;
    }

    public static Location[] formatLocations(String v1Locations) {
        List<Location> locations = new ArrayList<>();

        Arrays.asList(v1Locations.split(";")).forEach(location -> {
            locations.add(formatLocation(location));
        });

        return locations.toArray(new Location[0]);
    }

    private static Location formatLocation(String v1Location) {
        Location location = new Location();
        location.setOriginal(v1Location);

        String[] locationFields = v1Location.split("#");

        if (locationFields.length >= 3) {
            location.setCode(locationFields[0]);
            location.setName(locationFields[1]);
            location.setCode(locationFields[2]);
        }

        if (locationFields.length >= 6) {
            double lat = Double.parseDouble(locationFields[4]);
            double lon = Double.parseDouble(locationFields[5]);
            GeoPoint geoPoint = new GeoPoint(lat, lon);
            location.setGeoPoint(geoPoint);
        }

        return location;
    }

    public static String[] getCountryCodes(List<Location> locations) {
        List<String> countryCodes = new ArrayList<>();
        locations.forEach(location -> countryCodes.add(location.getCode()));
        return countryCodes.toArray(new String[0]);
    }

    @Override
    public String toString() {
        return this.original;
    }
}
