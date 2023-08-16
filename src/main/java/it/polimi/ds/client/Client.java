package it.polimi.ds.client;

import it.polimi.ds.networking.*;
import it.polimi.ds.networking.messages.*;
import it.polimi.ds.utils.SafeLogger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;
import java.util.logging.Level;

public class Client {

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    Boolean running = true;
    SafeLogger logger = SafeLogger.getLogger(this.getClass().getName());

    Topology topology = new Topology();

    public void run() throws IOException {

        topology = parseTopology();

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
        else {
            logger.log(Level.WARNING, "Unknown command or wrong syntax, no action taken");
        }
    }

    Connection openConnection(int node) throws IOException {
        return new AddressConnection(topology.getIp(node), topology.getPort(node), logger);
    }

    void get(String key, int node) throws IOException {
        Connection connection = openConnection(node);
        connection.send(new Presentation(-1));
        connection.send(new GetRequest(key));
        connection.bindToMessage(new MessageFilter(Topic.fromString(key), GetResponse.class), this::logGetResponse);
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
        connection.bindToMessage(new MessageFilter(Topic.fromString(key), PutResponse.class), this::logPutResponse);
    }

    boolean logPutResponse(Connection c, Message msg) {
        PutResponse res = (PutResponse) msg;
        logger.log(Level.INFO, "Successfully put value with version " + res.getVersion());
        c.stop();
        return true;
    }


    Topology parseTopology() throws IOException {

        Topology topology = new Topology();
        File file;
        file = new File("config.txt");
        int read_quorum ;
        int write_quorum;

        Scanner sc = new Scanner(file);

        read_quorum = sc.nextInt();
        write_quorum = sc.nextInt();

        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            if (line.isBlank()) {
                continue;
            }
            String[] pieces = line.split(" ");
            if (pieces.length != 2) {
               throw  new IOException("Error in reading the topology file, line " + line + " is not formatted correctly");
            }
            int port = Integer.parseInt(pieces[1]);
            topology.addNode(pieces[0], port);

        }

        return topology;
    }
}
