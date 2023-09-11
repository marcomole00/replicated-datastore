package it.polimi.ds.client;

import it.polimi.ds.exceptions.ConfigurationException;
import it.polimi.ds.networking.Address;
import it.polimi.ds.networking.Connection;
import it.polimi.ds.networking.MessageFilter;
import it.polimi.ds.networking.Topic;
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
    SafeLogger logger = SafeLogger.getLogger(this.getClass().getName(),true);

    Topology topology = new Topology();



    public void run() throws IOException, ConfigurationException {
        topology = new Config().getTopology();
        System.out.println(topology);
        System.out.println("usage: get <key> from <node> | put <key> <value> to <node>");
        System.out.println("tests: rr put <number of puts> <milliseconds of delay between puts> <same key flag>");
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
        } else if (cmd.matches("get [^\\s]+ from [0-9]+")) {
            logger.log(Level.INFO, "Getting the value for key " + pieces[1]);
            get(pieces[1], Integer.parseInt(pieces[3]));
        } else if (cmd.matches("put [^\\s]+ [^\\s]+ to [0-9]+")) {
            logger.log(Level.INFO, "Putting into key " + pieces[1] + " the value " + pieces[2]);
            put(pieces[1], pieces[2], Integer.parseInt(pieces[4]));
        } else if (cmd.matches("test1")) {
            test1();
        } else if (cmd.matches("rr put [0-9]+ [0-9]+ [0-1]")) {
            String[] pieces2 = cmd.split(" ");
            round_robin_put_test(Integer.parseInt(pieces2[2]), Integer.parseInt(pieces2[3]), Integer.parseInt(pieces2[4]));
        }
        else{
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
        connection.bind(new MessageFilter(Topic.fromString(key), GetResponse.class), this::logGetResponse);
    }

    boolean logGetResponse(Connection c, Message msg) {
        GetResponse res = (GetResponse) msg;
        logger.log(Level.INFO, "Successfully got value " + res.getValue() + " with version " + res.getVersion());
        c.stop();
        return true;
    }

    void put(String key, String value, int node) throws IOException {
        Connection connection = openConnection(node);
        logger.log(Level.INFO, "opened connection with node: " + node + ", socket: " + connection.printSocket());
        connection.send(new Presentation(-1));
        connection.send(new PutRequest(key, value));
        connection.bind(new MessageFilter(Topic.fromString(key), PutResponse.class), this::logPutResponse);
    }

    boolean logPutResponse(Connection c, Message msg) {
        PutResponse res = (PutResponse) msg;
       // logger.log(Level.INFO, "Successfully put value with version " + res.getVersion());
        System.out.println("succefully "+ msg);
        c.stop();
        return true;
    }

    void test1() {
        for (int i = 0; i < 500; i++) {
            try {
               Thread put_thread = new Thread(() -> {
                   try {
                       put("key", "value", 0);
                   } catch (IOException e) {
                       e.printStackTrace();
                   }
               });
               put_thread.start();
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }


    void round_robin_put_test(int number_of_operations, int milliseconds_of_idle, int same_key){

        for (int i = 0; i < number_of_operations; i++) {
            try {

                if (same_key == 1) put("key", "value" + i, i % topology.size());
                else put("key" + i, "value" + i, i % topology.size());

                if(milliseconds_of_idle > 0) Thread.sleep(milliseconds_of_idle);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
