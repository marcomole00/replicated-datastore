package it.polimi.ds;

import it.polimi.ds.client.Client;
import it.polimi.ds.exceptions.ConfigurationException;

import java.io.IOException;

public class DatabaseClient {
    public static void main(String[] args) throws IOException, ConfigurationException {
        new Client().run();
    }
}
