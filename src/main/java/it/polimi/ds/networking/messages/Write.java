package it.polimi.ds.networking.messages;

public class Write extends  Message{

    private final String value;
    private final int version;

    private final int contactId;

    public Write(String key, String value, int version, int contactId) {
        super(key);
        this.value = value;
        this.version = version;
        this.contactId = contactId;
    }


    public String getValue() {
        return value;
    }

    public int getVersion() {
        return version;
    }

    public int getContactId() {
        return contactId;
    }

    @Override
    public String toString() {
        return "Write{" +
                "key='" + getKey() + '\'' +
                ", value='" + value + '\'' +
                ", version=" + version + '\'' +
                ", contactId=" + contactId +
                '}';
    }
}
