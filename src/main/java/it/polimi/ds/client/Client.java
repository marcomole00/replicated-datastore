package it.polimi.ds.client;

import it.polimi.ds.networking.AddressConnection;
import it.polimi.ds.networking.Connection;
import it.polimi.ds.networking.MessageFilter;
import it.polimi.ds.networking.Topology;
import it.polimi.ds.networking.messages.*;
import it.polimi.ds.utils.SafeLogger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Level;

public class Client {

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    Boolean running = true;
    SafeLogger logger = SafeLogger.getLogger(this.getClass().getName());

    Topology topology = new Topology();

    public void run() throws IOException {
        while (running) {
            String cmd = reader.readLine();
            runCommand(cmd);
        }
    }

    void runCommand(String cmd) throws IOException {
        String[] pieces = cmd.split(" ");
        if (cmd.matches("stop")) {
            logger.log(Level.INFO, "Stopping the client");
            running = false;
        }
        else if (cmd.matches("get [^\\s]+ from [0-9]+")) {
            logger.log(Level.INFO, "Getting the value for key " + pieces[1]);
            get(pieces[1], Integer.parseInt(pieces[3]));
        }
        else if (cmd.matches("put [^\\s]+ [^\\s]+ to [0-9]+")) {
            logger.log(Level.INFO, "Putting into key " + pieces[1] + " the value " + pieces[2]);
            put(pieces[1], pieces[2], Integer.parseInt(pieces[4]));
        }
        else {
            logger.log(Level.WARNING, "Unknown command or wrong syntax, no action taken");
        }
    }

    void get(String key, int node) throws IOException {
        Connection connection = new AddressConnection(topology.getIp(node), topology.getPort(node), logger);
        connection.send(new GetRequest(key));
        connection.bindToMessage(new MessageFilter(key, GetResponse.class), this::logGetResponse);
    }

    void logGetResponse(Message msg) {
        GetResponse res = (GetResponse) msg;
        logger.log(Level.INFO, "Suvvessfully got value " + res.getValue() + " with version " + res.getVersion());
    }

    void put(String key, String value, int node) throws IOException {
        Connection connection = new AddressConnection(topology.getIp(node), topology.getPort(node), logger);
        connection.send(new PutRequest(key, value));
        connection.bindToMessage(new MessageFilter(key, PutResponse.class), this::logPutResponse);
    }

    void logPutResponse(Message msg) {
        PutResponse res = (PutResponse) msg;
        logger.log(Level.INFO, "Successfully put value with version " + res.getVersion());
    }
}
