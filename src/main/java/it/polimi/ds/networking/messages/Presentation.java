package it.polimi.ds.networking.messages;

public class Presentation extends Message {

    private final int id;

   public  Presentation(int id) {
        super(null); // only message type is needed
        this.id = id;
    }

    public int getId() {
        return id;
    }

    @Override
    public String toString() {
        return "Presentation{" +
                "id=" + id +
                '}';
    }

}
