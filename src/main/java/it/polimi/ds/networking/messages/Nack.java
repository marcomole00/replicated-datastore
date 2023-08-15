package it.polimi.ds.networking.messages;

public class Nack extends Message {

    private  final int nodeID;

    public Nack(String key, int nodeID) {
        super(key);
        this.nodeID = nodeID;

    }


    public int getNodeID() {
        return nodeID;
    }


}
