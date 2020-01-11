package impro.util;

public class ChainSection {
    private String chainSectionLabel;
    private String[] chainOrganizationsFilter;
    private String[] chainThemesFilter;

    public ChainSection(String chainSectionLabel, String[] chainOrganizationsFilter, String[] chainThemesFilter) {
        this.chainSectionLabel = chainSectionLabel;
        this.chainOrganizationsFilter = chainOrganizationsFilter;
        this.chainThemesFilter = chainThemesFilter;
    }

    public String getChainSectionLabel() {
        return chainSectionLabel;
    }

    public String[] getChainOrganizationsFilter() {
        return chainOrganizationsFilter;
    }

    public String[] getChainThemesFilter() {
        return chainThemesFilter;
    }
}
