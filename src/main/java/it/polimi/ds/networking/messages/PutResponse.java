package it.polimi.ds.networking.messages;

public class PutResponse extends Message{
    private final int version;
    private  final String value;

    public PutResponse(String key, int version, String value) {
        super(key);
        this.version = version;
        this.value = value;
    }

    public int getVersion() {
        return version;
    }

    public  String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "PutResponse{" +
                "version=" + version +
                ", key='" + super.getKey() + "'" +
                ", value='" + value + '\'' +
                '}';
    }
}
