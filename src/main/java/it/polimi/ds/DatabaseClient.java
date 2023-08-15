package it.polimi.ds;

import it.polimi.ds.client.Client;

import java.io.IOException;

public class DatabaseClient {
    public static void main(String[] args) throws IOException {
        new Client().run();
    }
}
