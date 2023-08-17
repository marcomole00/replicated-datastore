package it.polimi.ds.networking.messages;

public class ContactRequest extends  Message {

    private final int contactId;
    public ContactRequest(String key, int contactId) {
        super(key);
        this.contactId = contactId;
    }

    public int getContactId() {
        return contactId;
    }
}
