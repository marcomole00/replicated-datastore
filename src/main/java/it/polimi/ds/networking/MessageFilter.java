package it.polimi.ds.networking;

import org.apache.commons.lang3.tuple.MutablePair;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MessageFilter extends MutablePair<String, List<Class<?>>> {

    public MessageFilter(String key, Class<?> c) {
        super(key, Collections.singletonList(c));
    }

    public MessageFilter(String key, List<Class<?>> classes) {
        super(key, classes);
    }

    List<Class<?>> getClasses() {
        return getRight();
    }
}
