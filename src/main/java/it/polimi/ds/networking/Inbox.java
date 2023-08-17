package it.polimi.ds.networking;

import it.polimi.ds.networking.messages.Message;
import it.polimi.ds.utils.SafeLogger;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.logging.Level;

public class Inbox {
    private final List<Message> queue = new LinkedList<>();

    private final Map<MessageFilter, BiPredicate<Connection, Message>> bindings = new HashMap<>();

    private final SafeLogger logger;

    private final Connection connection;

    public  Inbox(SafeLogger logger, Connection connection) {
        this.logger = logger;
        this.connection = connection;
    }


    /**
     * remove the first matching message from messagesToProcess queue and returns it
     * @param filter the filter the message must match
     * @return the message removed from the queue or null if none is available
     */
    public Message pollFirstMatch(MessageFilter filter) {
        synchronized (queue) {
            for (Message m : queue) {
                if (matchFilter(m, filter)) {
                    queue.remove(m);
                    return m;
                }
            }
            return null;
        }
    }

    void updateQueue() {
        synchronized (queue) {
            int i = 0;
            while (i < queue.size()){
                Message m = queue.get(i);
                queue.remove(i);
                if (!matchBindings(m)) {
                    queue.add(i, m);
                    i++;
                }
            }
        }
    }

    public void bindToMessage(MessageFilter filter, BiPredicate<Connection, Message> action) {
        synchronized (bindings) {
            logger.log(Level.INFO,"Binding " + filter + " to " + action);
            bindings.put(filter, action);
        }
        updateQueue();
    }

    public void clearBindings(Topic topic) {
        synchronized (bindings) {
            bindings.keySet().stream().filter(e->topic.contains(e.getTopic())).toList().forEach(bindings.keySet()::remove);
            logger.log(Level.INFO, "cLearing bindings for " + topic);
        }
    }

    Boolean matchBindings(Message m) {
        synchronized (bindings) {
            for (Map.Entry<MessageFilter, BiPredicate<Connection, Message>> b : bindings.entrySet()) {
                if(matchFilter(m, b.getKey())) {
                    return b.getValue().test(connection, m);
                }
            }
            return false;
        }
    }

    /**
     * checks if the message belongs to one of the classes in the filter
     * @param message the message to be checked
     * @param filter a list of class that the filter will match
     * @return if the message matches
     */
    static boolean matchFilter(Message message, MessageFilter filter) {
        for (Class<?> c: filter.getClasses()) {
            if(c.isInstance(message))
                return filter.getTopic().match(message.getKey());
        }
        return false;
    }

    public void add(Message message) {
        synchronized (queue) {
            queue.add(message);
            updateQueue();
        }
    }
}
