package it.polimi.ds.utils;

import it.polimi.ds.exceptions.ConfigurationException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class Config {
    int readQuorum;
    int writeQuorum;
    private Topology topology = new Topology();

    public Config() throws FileNotFoundException, ConfigurationException {
        //open file, read the topology and the quorums
        File file;
        file = new File("config.txt");
        Scanner sc = new Scanner(file);


        try {
            readQuorum = sc.nextInt();
            writeQuorum = sc.nextInt();
        } catch (Exception e) {
            throw new ConfigurationException("Error in reading the topology file, quorums specified incorrectly");

        }

        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            if (line.isBlank()) {
                continue;
            }
            if(line.startsWith("#")) {
                continue;
            }
            String[] pieces = line.split(" ");
            if (pieces.length != 2) {
                throw  new ConfigurationException("Error in reading the topology file, line " + line + " is not formatted correctly");
            }
            try {
                int port = Integer.parseInt(pieces[1]);
                topology.addNode(pieces[0], port);

            } catch (Exception e) {
                throw  new ConfigurationException("Error in reading the topology file, line " + line + " is not formatted correctly");
            }
        }

        if (readQuorum + writeQuorum <= getNumberOfNodes()) {
            throw  new ConfigurationException("QR + QW must be greater than N");
        }

        if (readQuorum > getNumberOfNodes()) {
            throw  new ConfigurationException("QR must be less than N");

        }

        if (writeQuorum > getNumberOfNodes()) {
            throw  new ConfigurationException("QW must be less than N");
        }

        if (writeQuorum < (getNumberOfNodes() / 2) + 1) {
            throw  new ConfigurationException("QW must be greater than N/2");
        }
    }

    public int getReadQuorum() {
        return readQuorum;
    }

    public int getWriteQuorum() {
        return writeQuorum;
    }

    public int getNumberOfNodes() {
        return topology.size();
    }

    public Topology getTopology() {
        return topology;
    }
}
