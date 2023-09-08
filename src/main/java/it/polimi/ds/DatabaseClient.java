package it.polimi.ds;

import it.polimi.ds.client.Client;
import it.polimi.ds.client.TimedClient;
import it.polimi.ds.exceptions.ConfigurationException;

import java.io.IOException;

public class DatabaseClient {
    public static void main(String[] args) throws IOException, ConfigurationException {

        if (args.length>0 && (args[0].equals("--timed") || args[0].equals("-t"))) {
            new TimedClient().run(Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        } else new Client().run();
    }
}
