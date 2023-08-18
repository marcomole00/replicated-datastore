package it.polimi.ds.networking;

import it.polimi.ds.networking.messages.Message;
import it.polimi.ds.networking.messages.Presentation;
import it.polimi.ds.utils.SafeLogger;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.logging.Level;

public class Inbox {
    private final List<Message> queue = new LinkedList<>();

    private final BindingSet bindings = new BindingSet();

    private final SafeLogger logger;

    private final Connection connection;

    private final LockSet locks;

    public  Inbox(SafeLogger logger, Connection connection, LockSet locks) {
        this.logger = logger;
        this.connection = connection;
        this.locks = locks;
    }

    void updateQueue(Topic topic) {
        synchronized (queue) {
            synchronized (locks.get(topic)) {
                Message processed;
                do {
                    processed = null;
                    int i = 0;
                    while (i < queue.size()) {
                        Message m = queue.get(i);
                        queue.remove(i);
                        if (matchBindings(m, topic)) {
                            processed = m;
                            break;
                        }
                        else {
                            queue.add(i, m);
                            i++;
                        }
                    }
                } while (processed != null);
            }
        }
    }

    public void bindCheckPrevious(MessageFilter filter, BiPredicate<Connection, Message> action) {
        synchronized (locks.get(filter.getTopic())) {
            bind(filter, action);
            updateQueue(filter.getTopic());
        }
    }

    public void bind(MessageFilter filter, BiPredicate<Connection, Message> action) {
        synchronized (locks.get(filter.getTopic())) {
            logger.log(Level.INFO,"Binding " + filter + " to " + action);
            bindings.insert(filter, keySafeAction(action));
        }
    }

    public void clearBindings(Topic topic) {
        synchronized (locks.get(topic)) {
            bindings.clear(topic);
            logger.log(Level.INFO, "cLearing bindings for " + topic);
        }
    }

    Boolean matchBindings(Message m, Topic topic) {
        synchronized (locks.get(topic)) {
            for (Map.Entry<MessageFilter, BiPredicate<Connection, Message>> b : bindings.getMatchList(topic)) { // always match messages also against topic "any"
                if(b.getKey().match(m)) {  // getKey extracts the MessageFilter
                    return b.getValue().test(connection, m);
                }
            }
            return false;
        }
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
            queue.add(message); //TODO: presentation key
            updateQueue(Topic.fromString(message.getKey()));
        }
    }
}
