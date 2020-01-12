package impro.util;

public class ChainSection {
    private String chainSectionLabel;
    private String[] chainOrganizationsFilter;
    private String[] chainThemesFilter;

    public ChainSection() {}

    public String getChainSectionLabel() {
        return chainSectionLabel;
    }

    public void setChainSectionLabel(String chainSectionLabel) {
        this.chainSectionLabel = chainSectionLabel;
    }

    public String[] getChainOrganizationsFilter() {
        return chainOrganizationsFilter;
    }

    public void setChainOrganizationsFilter(String[] chainOrganizationsFilter) {
        this.chainOrganizationsFilter = chainOrganizationsFilter;
    }

    public String[] getChainThemesFilter() {
        return chainThemesFilter;
    }

    public void setChainThemesFilter(String[] chainThemesFilter) {
        this.chainThemesFilter = chainThemesFilter;
    }
}
