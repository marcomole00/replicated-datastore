package it.polimi.ds.node;

import it.polimi.ds.networking.Connection;

public class Metadata {
    public State state = State.Idle;
    public Integer ackCounter = 0;

    public Integer writeMaxVersion = -1;

    public String toWrite;

    public Integer coordinator;

    public Integer contactId;

    public Connection writeClient;

    public boolean reading = false;
    public Integer readCounter = 0;

    public Integer readMaxVersion = -1;

    public String latestValue;

    public Connection readClient;
}
