package it.polimi.ds.networking.messages;

public class PutResponse extends Message{
    private final int version;

    public PutResponse(String key, int version) {
        super(key);
        this.version = version;
    }

    public int getVersion() {
        return version;
    }
}
