package it.polimi.ds.client;

import com.google.gson.Gson;
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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

public class TimedClient {

    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    Boolean running = true;
    SafeLogger logger = SafeLogger.getLogger(this.getClass().getName(),true);

    Topology topology = new Topology();


    List<Long> timestamps = new LinkedList<>();


    private int number_of_operations;
    private int number_of_response = 0;

    private Map<String, Long> durations = new HashMap<>();


    public void run(int type_of_test, int number_of_op, int milliseconds_of_idle) throws IOException, ConfigurationException {
        topology = new Config().getTopology();

        number_of_operations = number_of_op;
        timestamps.add(System.currentTimeMillis());

        switch (type_of_test) {
            case 0: round_robin_put_test(number_of_op, 0, 0);
            case 1: round_robin_put_test(number_of_op, milliseconds_of_idle, 1);
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
        durations.put(value, System.currentTimeMillis());
        connection.send(new Presentation(-1));
        connection.send(new PutRequest(key, value));
        connection.bind(new MessageFilter(Topic.fromString(key), PutResponse.class), this::logPutResponse);
    }

    boolean logPutResponse(Connection c, Message msg) {
        PutResponse res = (PutResponse) msg;
        logger.log(Level.INFO, "Successfully put value with version " + res.getVersion());
        durations.computeIfPresent(res.getValue(), (k, v) -> System.currentTimeMillis() - v);
        number_of_response++;
        if (number_of_response == number_of_operations) {
           System.out.println("durations: " + durations);
           Gson gson = new Gson();
              System.out.println("durations in json: " + gson.toJson(durations));
        }
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


