package it.polimi.ds.networking.messages;

public class Abort extends  Message{

    private int contactId;
    public Abort(String key, int contactId) {
        super(key);
        this.contactId = contactId;

    }

    @Override
    public String toString() {
        return "Abort{" +
                "key='" + getKey() + '\'' +
                ", contactId=" + contactId +
                '}';
    }

}
