package it.polimi.ds.node;

import it.polimi.ds.networking.Address;
import it.polimi.ds.networking.AddressConnection;
import it.polimi.ds.networking.Connection;
import it.polimi.ds.networking.Topology;
import it.polimi.ds.utils.SafeLogger;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.io.File;
import java.util.Scanner;

public class Node {

   private  HashMap<Integer, Connection> peers = new HashMap<>();
   private ServerSocket serverSocket;
    int read_quorum;
    int write_quorum;
    int my_id = -1;

    int number_of_nodes;

    Address my_address;
    SafeLogger logger =  SafeLogger.getLogger(this.getClass().getName());
    private Topology topology = new Topology();

    HashMap<String, Entry> db = new HashMap<>();

    public void run() throws Exception {

        //open file and read the topology
        //for each node in the topology create a connection
        //start the server socket
        File file;
        try {
            file = new File("config.txt");
        } catch (Exception e) {
            System.out.println("Error in opening the topology file");
            return ;
        }
        Scanner sc = new Scanner(file);


        try {
            read_quorum = sc.nextInt();
            write_quorum = sc.nextInt();
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

        number_of_nodes = topology.getNodes().size();

        if (read_quorum + write_quorum <= number_of_nodes) {
            System.out.println("QR + QW must be greater than N");
            return ;
        }

        if (read_quorum > number_of_nodes) {
            System.out.println("QR must be less than N");
            return ;
        }

        if (write_quorum > number_of_nodes) {
            System.out.println("QW must be less than N");
            return ;
        }

        if (read_quorum < (number_of_nodes / 2) + 1) {
            System.out.println("QR must be greater than N/2");
            return ;
        }

        ipAutoDiscovery();

        serverSocket = new ServerSocket(my_address.getPort());




    }


    public void ipAutoDiscovery() throws Exception {
        try {
            boolean tobreak = false;
            InetAddress inet = InetAddress.getLocalHost();
            InetAddress[] ips = InetAddress.getAllByName(inet.getCanonicalHostName());
            if (ips  != null ) {
                for (int i= 0; i < ips.length; i++) {
                    System.out.println("IP address: " + ips[i].getHostAddress());
                    for (int j = 0; j < topology.getNodes().size(); j++) {
                        if (ips[i].getHostAddress().equals(topology.getNodes().get(j).getIp())) {
                            my_id = j;
                            System.out.println("My id is " + my_id);

                            my_address = topology.getNodes().get(j);

                            System.out.println("My address is " + my_address);
                            tobreak = true;
                            break;
                        }
                    }

                    if (tobreak) {
                        break;
                    }
                }

                if (my_id == -1) {
                    throw  new Exception("Error in reading the topology file, my ip is not in the topology");
                }
            }
        } catch (Exception  e) {
            System.out.println("Error in getting the local host");
            throw e;
        }
    }


}

