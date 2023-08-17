package it.polimi.ds.networking;

import it.polimi.ds.networking.messages.Message;
import org.apache.commons.lang3.tuple.MutablePair;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MessageFilter extends MutablePair<Topic, List<Class<?>>> {

    public MessageFilter(Topic topic, Class<?> c) {
        super(topic, Collections.singletonList(c));
    }

    public MessageFilter(Topic topic, List<Class<?>> classes) {
        super(topic, classes);
    }

    public Topic getTopic() {return getLeft();}

    List<Class<?>> getClasses() {
        return getRight();
    }

    /**
     * checks if the message belongs to one of the classes in the filter
     * @param message the message to be checked
     * @return if the message matches
     */
    public boolean match(Message message) {
        for (Class<?> c: getClasses()) {
            if(c.isInstance(message))
                return getTopic().match(message.getKey());
        }
        return false;
    }
}
