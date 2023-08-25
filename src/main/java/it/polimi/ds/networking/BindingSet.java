package it.polimi.ds.networking;

import it.polimi.ds.networking.messages.Message;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

public class BindingSet {
    private final Map<Topic, Map<MessageFilter, BiPredicate<Connection, Message>>> map = new HashMap<>();

    private synchronized void putIfNotPresent(Topic topic) {
        if (!map.containsKey(topic))
            map.put(topic, new HashMap<>());
    }
    private synchronized Map<MessageFilter, BiPredicate<Connection, Message>> getSubset(Topic topic) {
        putIfNotPresent(topic);
        return map.get(topic);
    }

    public List<Map.Entry<MessageFilter, BiPredicate<Connection, Message>>> getMatchList(Topic topic) {
        List<Map.Entry<MessageFilter, BiPredicate<Connection, Message>>> res = new java.util.ArrayList<>(getSubset(topic).entrySet().stream().toList());
        res.addAll(getSubset(Topic.any()).entrySet().stream().toList());
        return res;
    }

    public void insert(MessageFilter filter, BiPredicate<Connection, Message> action) {
        putIfNotPresent(filter.getTopic());
        getSubset(filter.getTopic()).put(filter, action);
    }

    public void clear(Topic topic) {
        getSubset(topic).clear();
    }
}
