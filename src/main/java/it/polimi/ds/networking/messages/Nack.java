package it.polimi.ds.networking.messages;

public class Nack extends Message {

    private  final int nodeID;

    private final int contactId;

    public Nack(String key, int nodeID, int contactId) {
        super(key);
        this.nodeID = nodeID;
        this.contactId = contactId;
    }


    public int getNodeID() {
        return nodeID;
    }


    public int getContactId() {
        return contactId;
    }

    @Override
    public String toString() {
        return "Nack{" +
                "key='" + getKey() + '\'' +
                ", nodeID=" + nodeID +
                ", contactId=" + contactId +
                '}';
    }
}
