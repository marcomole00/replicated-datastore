package it.polimi.ds.networking;

import org.apache.commons.lang3.tuple.MutablePair;

public class Address extends MutablePair<String, Integer> {
    public Address(String ip, Integer port) {
        super(ip, port);
    }

    public String getIp() {
        return super.getLeft();
    }

    public Integer getPort() {
        return super.getRight();
    }
}
