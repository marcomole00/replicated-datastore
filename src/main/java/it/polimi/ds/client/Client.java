package it.polimi.ds.client;

import it.polimi.ds.networking.AddressConnection;
import it.polimi.ds.networking.Connection;
import it.polimi.ds.networking.MessageFilter;
import it.polimi.ds.networking.messages.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Client {

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    Boolean running = true;
    Logger logger = Logger.getLogger(this.getClass().getName());

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
        else if (cmd.matches("get [^\\s]+ from [0-9\\.]+\\:[0-9]+")) {
            logger.log(Level.INFO, "Getting the value for key " + pieces[1]);
            get(pieces[1], getIp(pieces[3]), getPort(pieces[3]));
        }
        else if (cmd.matches("put [^\\s]+ [^\\s]+ to [0-9\\.]+\\:[0-9]+")) {
            logger.log(Level.INFO, "Putting into key " + pieces[1] + " the value " + pieces[2]);
            put(pieces[1], pieces[2], getIp(pieces[4]), getPort(pieces[4]));
        }
        else {
            logger.log(Level.WARNING, "Unknown command or wrong syntax, no action taken");
        }
    }

    static String getIp(String s) {
        return s.split(":")[0];
    }

    static int getPort(String s) {
        return Integer.parseInt(s.split(":")[1]);
    }

    void get(String key, String address, int port) throws IOException {
        Connection connection = new AddressConnection(address, port, logger);
        connection.send(new GetRequest(key));
        connection.bindToMessage(new MessageFilter(key, GetResponse.class), this::logGetResponse);
    }

    void logGetResponse(Message msg) {
        GetResponse res = (GetResponse) msg;
        logger.log(Level.INFO, "Suvvessfully got value " + res.getValue() + " with version " + res.getVersion());
    }

    void put(String key, String value, String address, int port) throws IOException {
        Connection connection = new AddressConnection(address, port, logger);
        connection.send(new PutRequest(key, value));
        connection.bindToMessage(new MessageFilter(key, PutResponse.class), this::logPutResponse);
    }

    void logPutResponse(Message msg) {
        PutResponse res = (PutResponse) msg;
        logger.log(Level.INFO, "Successfully put value with version " + res.getVersion());
    }
}
