package it.polimi.ds.networking.messages;

public class ContactResponse extends  Message{

    private final int version;

    public ContactResponse(String key, int version) {
        super(key);
        this.version = version;
    }



    public int getVersion() {
        return version;
    }

}
