package it.polimi.ds.node;

import org.apache.commons.lang3.tuple.MutablePair;

public class Entry  extends MutablePair<String, Integer> {

    public Entry(String value, Integer version) {
        super(value, version);
    }
    @Override
    public String getLeft() {
        return super.getLeft();
    }

    @Override
    public Integer getRight() {
        return super.getRight();
    }

}
