package it.polimi.ds.client;

import it.polimi.ds.networking.*;
import it.polimi.ds.networking.messages.*;
import it.polimi.ds.utils.Config;
import it.polimi.ds.utils.SafeLogger;
import it.polimi.ds.utils.Topology;

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
        topology = new Config().getTopology();
        System.out.println(topology);
        System.out.println("usage: get <key> from <node> | put <key> <value> to <node>");
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
        else if (cmd.matches("test1")) {
            test1();
        }
        else {
            logger.log(Level.WARNING, "Unknown command or wrong syntax, no action taken");
        }
    }

    Connection openConnection(int node) throws IOException {
        return Connection.fromAddress(new Address(topology.getIp(node), topology.getPort(node)), logger);
    }

    void get(String key, int node) throws IOException {
        Connection connection = openConnection(node);
        connection.send(new Presentation(-1));
        connection.send(new GetRequest(key));
        connection.bindCheckPrevious(new MessageFilter(Topic.fromString(key), GetResponse.class), this::logGetResponse);
    }

    boolean logGetResponse(Connection c, Message msg) {
        GetResponse res = (GetResponse) msg;
        logger.log(Level.INFO, "Successfully got value " + res.getValue() + " with version " + res.getVersion());
        c.stop();
        return true;
    }

    void put(String key, String value, int node) throws IOException {
        Connection connection = openConnection(node);
        connection.send(new Presentation(-1));
        connection.send(new PutRequest(key, value));
        connection.bindCheckPrevious(new MessageFilter(Topic.fromString(key), PutResponse.class), this::logPutResponse);
    }

    boolean logPutResponse(Connection c, Message msg) {
        PutResponse res = (PutResponse) msg;
        logger.log(Level.INFO, "Successfully put value with version " + res.getVersion());
        c.stop();
        return true;
    }

    void test1() {
        for (int i = 0; i < 500; i++) {
            try {
                put("key", "value" + i, (i+1) % topology.size());
                Thread.sleep(20);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
