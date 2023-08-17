package it.polimi.ds.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class Config {
    int readQuorum;
    int writeQuorum;
    private Topology topology = new Topology();

    public Config() throws FileNotFoundException {
        //open file, read the topology and the quorums
        File file;
        try {
            file = new File("config.txt");
        } catch (Exception e) {
            System.out.println("Error in opening the topology file");
            return ;
        }
        Scanner sc = new Scanner(file);


        try {
            readQuorum = sc.nextInt();
            writeQuorum = sc.nextInt();
        } catch (Exception e) {
            System.out.println("Error in reading the topology file, quorums specified incorrectly");
            return ;
        }

        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            if (line.isBlank()) {
                continue;
            }
            String[] pieces = line.split(" ");
            if (pieces.length != 2) {
                System.out.println("Error in reading the topology file, line " + line + " is not formatted correctly");
                return ;
            }
            try {
                int port = Integer.parseInt(pieces[1]);
                topology.addNode(pieces[0], port);

            } catch (Exception e) {
                System.out.println("Error in reading the topology file, line " + line + " is not formatted correctly");
                return ;
            }
        }

        if (readQuorum + writeQuorum <= getNumberOfNodes()) {
            System.out.println("QR + QW must be greater than N");
            return ;
        }

        if (readQuorum > getNumberOfNodes()) {
            System.out.println("QR must be less than N");
            return ;
        }

        if (writeQuorum > getNumberOfNodes()) {
            System.out.println("QW must be less than N");
            return ;
        }

        if (readQuorum < (getNumberOfNodes() / 2) + 1) {
            System.out.println("QR must be greater than N/2");
            return ;
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
