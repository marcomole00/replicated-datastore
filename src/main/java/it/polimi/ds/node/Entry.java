package it.polimi.ds.node;

import org.apache.commons.lang3.tuple.MutableTriple;

public class Entry extends MutableTriple<String, Integer, Metadata> {

    public Entry(String value, Integer version, Metadata metadata) {
        super(value, version, metadata);
    }
    public String getValue() {
        return super.getLeft();
    }

    public Integer getVersion() {
        return super.getMiddle();
    }

    public Metadata getMetadata() {
        return super.getRight();
    }

    public void setValue(String value) {
        super.setLeft(value);
    }

    public void setVersion(Integer version) {
        super.setMiddle(version);
    }
}
