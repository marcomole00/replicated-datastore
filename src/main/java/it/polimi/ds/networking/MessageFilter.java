package it.polimi.ds.networking;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public abstract class MessageFilter extends Pair<String, List<Class<?>>> {
    List<Class<?>> getClasses() {
        return getRight();
    }
}
