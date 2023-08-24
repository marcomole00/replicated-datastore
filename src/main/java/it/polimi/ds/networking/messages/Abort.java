package it.polimi.ds.networking.messages;

public class Abort extends  Message{

    private final int contactId;
    public Abort(String key, int contactId) {
        super(key);
        this.contactId = contactId;

    }

    public int getContactId() {
        return contactId;
    }

    @Override
    public String toString() {
        return "Abort{" +
                "key='" + getKey() + '\'' +
                ", contactId=" + contactId +
                '}';
    }

}
