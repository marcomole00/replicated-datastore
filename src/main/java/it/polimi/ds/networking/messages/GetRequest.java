package it.polimi.ds.networking.messages;

public class GetRequest extends Message{
    public GetRequest(String key) {
        super(key);
    }

    @Override
    public String toString() {
        return "GetRequest{" +
                "key='" + getKey() + '\'' +
                '}';
    }
}
