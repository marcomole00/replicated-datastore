package it.polimi.ds.networking;

import it.polimi.ds.networking.messages.Message;
import it.polimi.ds.networking.messages.Presentation;
import it.polimi.ds.utils.SafeLogger;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

public class Inbox {
    private final List<Message> queue = new ArrayList<>();

    private final BindingSet bindings = new BindingSet();

    private final SafeLogger logger;

    private final Consumer<String> fullUpdate;

    private final Connection connection;

    private final LockSet locks;

    public  Inbox(SafeLogger logger, Connection connection, LockSet locks,  Consumer<String> fullUpdate) {
        this.logger = logger;
        this.connection = connection;
        this.locks = locks;
        this.fullUpdate = fullUpdate;
    }

    public boolean updateQueue(Topic topic) {
        int i = 0;
        boolean result = false;
        synchronized (queue) {
            while (i < queue.size()) {
                Message m = queue.get(i);
                queue.remove(m);
                if (matchBindings(m, bindings.getMatchList(topic))) {
                    result = true;
                }
                else {
                    queue.add(i, m);
                    i++;
                }
            }
        }
        return result;
    }

    public boolean waitUpdateQueue(Topic topic) {
        synchronized (locks.get(topic)) {
            while (locks.get(topic).get()) {
                try {
                    locks.get(topic).wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            locks.get(topic).set(true);
        }
        boolean result = updateQueue(topic);
        synchronized (locks.get(topic)) {
            locks.get(topic).set(false);
            locks.get(topic).notifyAll();
        }
        return result;
    }

    public void bind(MessageFilter filter, BiPredicate<Connection, Message> action) {
        synchronized (locks.get(filter.getTopic())) {
           // logger.log(Level.INFO,"Binding " + filter + " to " + action);
            bindings.insert(filter, keySafeAction(action));
        }
    }

    public void clearBindings(Topic topic) {
        synchronized (locks.get(topic)) {
            bindings.clear(topic);
            //logger.log(Level.INFO, "Clearing bindings for " + topic);
        }
    }

    Boolean matchBindings(Message m, List<Map.Entry<MessageFilter, BiPredicate<Connection, Message>>> relatedBindings) {
        for (Map.Entry<MessageFilter, BiPredicate<Connection, Message>> b : relatedBindings) { // always match messages also against topic "any"
            if(b.getKey().match(m)) {  // getKey extracts the MessageFilter
                return b.getValue().test(connection, m);
            }
        }
        return false;
    }

    BiPredicate<Connection, Message> keySafeAction(BiPredicate<Connection, Message> action) {
        return (c,m)-> {
            if (m instanceof Presentation) {
                return action.test(c, m);
            }
            else {
                synchronized (locks.get(m.getKey())) {
                    return action.test(c, m);
                }
            }
        };
    }

    public void add(Message message) {
        synchronized (queue) {
            queue.add(message);
        }
        if (message instanceof Presentation)
            waitUpdateQueue(Topic.fromString(message.getKey()));
        else {
            if (fullUpdate != null)
                fullUpdate.accept(message.getKey());
            else
                waitUpdateQueue(Topic.fromString(message.getKey()));
        }
    }

    public String printQueue() {
        synchronized (queue) {
            return queue.toString();
        }
    }
}
