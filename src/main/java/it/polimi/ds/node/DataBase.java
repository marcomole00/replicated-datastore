package it.polimi.ds.node;

import java.util.HashMap;

public class DataBase extends HashMap<String, Entry> {

    public synchronized void putIfNotPresent(String key) {
        if (!containsKey(key)) {
            put(key, new it.polimi.ds.node.Entry(null, 0, new Metadata()));
        }
    }
}
