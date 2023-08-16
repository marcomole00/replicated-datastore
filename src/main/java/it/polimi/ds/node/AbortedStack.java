package it.polimi.ds.node;

import it.polimi.ds.networking.Connection;
import it.polimi.ds.networking.messages.PutRequest;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Stack;

public class AbortedStack extends Stack<Pair<Connection, PutRequest>> {
    @Override
    public synchronized Pair<Connection, PutRequest> push(Pair<Connection, PutRequest> item) {
        return super.push(item);
    }

    @Override
    public synchronized Pair<Connection, PutRequest> pop() {
        return super.pop();
    }
}
