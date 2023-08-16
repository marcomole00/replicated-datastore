package it.polimi.ds.networking;

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

    Topic getTopic() {return getLeft();}

    List<Class<?>> getClasses() {
        return getRight();
    }
}
