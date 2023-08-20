package it.polimi.ds.networking.messages;

public class ContactResponse extends  Message{

    private final int version;

    private final int contactId;

    public ContactResponse(String key, int version, int contactId) {
        super(key);
        this.version = version;
        this.contactId = contactId;
    }

    public int getVersion() {
        return version;
    }


    public int getContactId() {
        return contactId;
    }

    @Override
    public String toString() {
        return "ContactResponse{" +
                "key='" + getKey() + '\'' +
                ", version=" + version +
                ", contactId=" + contactId +
                '}';
    }
}
