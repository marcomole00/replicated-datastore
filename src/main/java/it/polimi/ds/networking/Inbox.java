package it.polimi.ds.networking;

import it.polimi.ds.networking.messages.Message;
import it.polimi.ds.networking.messages.Presentation;
import it.polimi.ds.utils.SafeLogger;

import java.util.*;
import java.util.function.BiPredicate;

public class Inbox {
    private final List<Message> queue = new ArrayList<>();

    private final BindingSet bindings = new BindingSet();

    private final SafeLogger logger;

    private final Connection connection;

    private final LockSet locks;

    private final List<Boolean> checking = new ArrayList<>(Collections.nCopies(1, false));

    public  Inbox(SafeLogger logger, Connection connection, LockSet locks) {
        this.logger = logger;
        this.connection = connection;
        this.locks = locks;
    }

    public void updateQueue(Topic topic) {
        int i = 0;
        List<Message> tmp;
        synchronized (queue) {
            tmp = new ArrayList<>(queue);
        }
        while (i < tmp.size()) {
            Message m = tmp.get(i);
            synchronized (queue) {
                if (queue.contains(m)) {
                    queue.remove(m);
                } else {
                    i++;
                    continue;
                }
            }
            if (matchBindings(m, topic)) {
                i = 0; // restart and check all messages that are in the queue
            }
            else {
                synchronized (queue) {
                    queue.add(m);
                }
                i++;
            }
        }
    }

    public void tryUpdateQueue(Topic topic) {
        synchronized (checking) {
            if (checking.get(0))
                return;
            else
                checking.set(0, true);
        }
        updateQueue(topic);
        synchronized (checking) {
            checking.set(0, false);
            checking.notifyAll();
        }
    }

    public void waitUpdateQueue(Topic topic) {
        synchronized (checking) {
            while (checking.get(0)) {
                try {
                    checking.wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            checking.set(0, true);
        }
        updateQueue(topic);
        synchronized (checking) {
            checking.set(0, false);
            checking.notifyAll();
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
        synchronized (queue) {
            queue.add(message);
        }
        waitUpdateQueue(Topic.fromString(message.getKey()));
    }

    public String printQueue() {
        synchronized (queue) {
            return queue.toString();
        }
    }
}
