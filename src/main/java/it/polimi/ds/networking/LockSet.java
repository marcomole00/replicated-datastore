package it.polimi.ds.networking;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class LockSet {
    final Map<Topic, AtomicBoolean> map = new HashMap<>();

    public AtomicBoolean get(String key) {
        return get(Topic.fromString(key));
    }

    public synchronized AtomicBoolean get(Topic topic) {
        if (!map.containsKey(topic))
            map.put(topic, new AtomicBoolean());
        return map.get(topic);
    }

    public void set(String key, boolean value) {
        set(Topic.fromString(key), value);
    }

    public synchronized void set(Topic topic, boolean value) {
        if (!map.containsKey(topic))
            map.put(topic, new AtomicBoolean());
        map.get(topic).set(value);
    }
}
