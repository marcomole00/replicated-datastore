package it.polimi.ds.networking;

import java.util.ArrayList;
import java.util.List;

public class Topology {
    List<Address> nodes;

    public Topology() {
        nodes = new ArrayList<>();

    }

    public String getIp(int id) {
        return nodes.get(id).getIp();
    }

    public int getPort(int id) {
        return nodes.get(id).getPort();
    }
}
