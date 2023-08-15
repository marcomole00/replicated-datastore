package it.polimi.ds.node;

import org.apache.commons.lang3.tuple.MutableTriple;

public class Entry extends MutableTriple<String, Integer, State > {

    public Entry(String value, Integer version, State state) {
        super(value, version, state);
    }
    public String getValue() {
        return super.getLeft();
    }

    public Integer getVersion() {
        return super.getMiddle();
    }

    public State getState() {
        return super.getRight();
    }

    public void setValue(String value) {
        super.setLeft(value);
    }

    public void setVersion(Integer version) {
        super.setMiddle(version);
    }

    public void setState(State state) {
        super.setRight(state);
    }
}
