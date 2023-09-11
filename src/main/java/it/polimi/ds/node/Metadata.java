package it.polimi.ds.node;

import it.polimi.ds.networking.Connection;

import java.util.ArrayList;
import java.util.List;

public class Metadata {
    public State state = State.Idle;
    public Integer ackCounter = 0;

    public Integer writeMaxVersion = -1;

    public String toWrite;

    public Integer coordinator;

    public Integer contactId;

    public List<Integer> extraContacts = new ArrayList<>();

    public List<Integer> nacked = new ArrayList<>();

    public Connection writeClient;

    public boolean reading = false;
    public Integer readCounter = 0;

    public Integer readMaxVersion = -1;

    public String latestValue;

    public Connection readClient;
}
