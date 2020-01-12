package impro.util;

public class ChainSection {
    private String sectionLabel;
    private String[] organizationFilter;
    private String[] themeFilter;
    private String[] locationFilter;

    public ChainSection() {}

    public String getSectionLabel() {
        return sectionLabel;
    }

    public void setSectionLabel(String sectionLabel) {
        this.sectionLabel = sectionLabel;
    }

    public String[] getOrganizationFilter() {
        return organizationFilter;
    }

    public void setOrganizationFilter(String[] organizationFilter) {
        this.organizationFilter = organizationFilter;
    }

    public String[] getThemeFilter() {
        return themeFilter;
    }

    public void setThemeFilter(String[] themeFilter) {
        this.themeFilter = themeFilter;
    }

    public String[] getLocationFilter() {
        return locationFilter;
    }

    public void setLocationFilter(String[] locationFilter) {
        this.locationFilter = locationFilter;
    }
}
