package it.polimi.ds.networking;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class LockSet {
    final Map<Topic, Object> map = new HashMap<>();

    public Object get(String key) {
        return get(Topic.fromString(key));
    }

    public synchronized Object get(Topic topic) {
        if (!map.containsKey(topic))
            map.put(topic, new Object());
        return map.get(topic);
    }
}
