package it.polimi.ds.utils;

import it.polimi.ds.networking.Address;

import java.util.ArrayList;
import java.util.List;

public class Topology {
    List<Address> nodes;

    public Topology() {
        nodes = new ArrayList<>();
    }

    public List<Address> getNodes() {
        return nodes;
    }

    public String getIp(int id) {
        return nodes.get(id).getIp();
    }

    public int getPort(int id) {
        return nodes.get(id).getPort();
    }

    public void addNode(String ip, int port) {
        nodes.add(new Address(ip, port));
    }

    public void addNode(Address address) {
        nodes.add(address);
    }

    public int getId(Address address) {
        return nodes.indexOf(address);
    }

    public int getId(String ip) {
        for (int i = 0; i < nodes.size(); i++) {
            if (getIp(i).equals(ip)) {
                return i;
            }
        }
        return -1;
    }

    public String toString() {
        String s = "";
        for (int i = 0; i < nodes.size(); i++) {
            s += i + " " + getIp(i) + " " + getPort(i) + "\n";
        }
        return s;
    }

    public int size() {
        return nodes.size();
    }
}
