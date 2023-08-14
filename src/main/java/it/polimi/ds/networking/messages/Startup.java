package it.polimi.ds.networking.messages;

import java.util.Map;

public class Startup extends Message{
    public final static String STARTUP_KEY = "";

    private final Map<String, Integer> nodes;

    Startup (Map<String, Integer> nodes) {
        super(STARTUP_KEY);
        this.nodes = nodes;
    }

    public Map<String, Integer> getNodes() {
        return nodes;
    }
}
