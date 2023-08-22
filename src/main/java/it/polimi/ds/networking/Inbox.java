package it.polimi.ds.networking;

import it.polimi.ds.networking.messages.Message;
import it.polimi.ds.networking.messages.Presentation;
import it.polimi.ds.utils.SafeLogger;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.logging.Level;

public class Inbox {
    private final List<Pair<Integer, Message>> queue = new LinkedList<>();

    private final BindingSet bindings = new BindingSet();

    private final SafeLogger logger;

    private final Connection connection;

    private final LockSet locks;

    private boolean checking = false;

    public  Inbox(SafeLogger logger, Connection connection, LockSet locks) {
        this.logger = logger;
        this.connection = connection;
        this.locks = locks;
    }

    public void updateQueue(Topic topic) {
        int i = 0;
        Pair<Integer, Message> p;
        while (i < queueSize()) {
            synchronized (queue) {
                p = queue.get(i);
                queue.remove(i);
            }
            Message m = p.getRight();
            Random rand = new Random();
            int id = rand.nextInt(50000);
            logger.log(Level.INFO, "with id " + id + " testing message " + m + " with index " + i + " from queue: " + queue);
            if (matchBindings(m, topic)) {
                logger.log(Level.INFO, "with id " + id + " matched true and exiting the loop with queue: " + queue);
                break;
            }
            else {
                i = preciseInsert(p) + 1;
                logger.log(Level.INFO, "with id " + id + " matched false and inserted back in the queue: " + queue);
            }
        }
    }

    int queueSize() {
        synchronized (queue) {
            return queue.size();
        }
    }

    // returns the position it was inserted to
    int preciseInsert(Pair<Integer, Message> entry) {
        synchronized (queue) {
            for (int i = 0; i < queue.size(); i++) {
                if (queue.get(i).getLeft() > entry.getLeft()) {
                    queue.add(i, entry);
                    return i;
                }
            }
            queue.add(entry);
            return queue.size()-1;
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

    Boolean matchBindings(Message m, Topic topic) {
        for (Map.Entry<MessageFilter, BiPredicate<Connection, Message>> b : bindings.getMatchList(topic)) { // always match messages also against topic "any"
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
        synchronized (locks.get(Topic.fromString(message.getKey()))) {
            synchronized (queue) {
                int lastId;
                if (queue.isEmpty())
                    lastId = 0;
                else
                    lastId = queue.get(queue.size()-1).getLeft();
                queue.add(new ImmutablePair<>(lastId+1, message));
            }
            updateQueue(Topic.fromString(message.getKey()));
        }
    }

    public String printQueue() {
        synchronized (queue) {
            return queue.toString();
        }
    }
}
