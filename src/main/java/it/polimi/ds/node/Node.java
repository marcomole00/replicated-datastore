package it.polimi.ds.node;

import it.polimi.ds.networking.*;
import it.polimi.ds.networking.messages.*;
import it.polimi.ds.utils.SafeLogger;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.io.File;
import java.util.Scanner;
import java.util.Stack;

import static java.lang.Thread.sleep;

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

    AbortedStack aborted = new AbortedStack();

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

       serverSocket =  ipAutoDiscovery();


        SocketAccepter socketAccepter = new SocketAccepter(serverSocket, peers, topology, my_id);
        Thread accepterThread = new Thread(socketAccepter);
        accepterThread.start();

        PeerConnector peerConnector = new PeerConnector(peers, topology, my_id);
        Thread connectorThread = new Thread(peerConnector);
        connectorThread.start();


        for (Connection p : peers.values()) {
            p.bindToMessage(new MessageFilter("", Read.class), this::onRead);
            p.bindToMessage(new MessageFilter("", Read.class), this::onReadResponse);
        }
    }


    public ServerSocket ipAutoDiscovery() throws Exception {

        ServerSocket serverSocket = null;
        try {
            InetAddress inet = InetAddress.getLocalHost();
            InetAddress[] ips = InetAddress.getAllByName(inet.getCanonicalHostName());
            if (ips  != null ) {
                for (int i= 0; i < ips.length; i++) {
                    System.out.println("IP address: " + ips[i].getHostAddress());
                    for (int j = 0; j < topology.getNodes().size(); j++) {
                        if (ips[i].getHostAddress().equals(topology.getIp(j))) {

                            my_id = j;

                            my_address = topology.getNodes().get(j);


                            try {
                                serverSocket = new ServerSocket(my_address.getPort());
                                // the node found its address and a free socket.
                                System.out.println("Server socket opened on port " + my_address.getPort());
                                System.out.println("My id is " + my_id);
                                return serverSocket;
                            } catch (Exception e) {
                                // the node found its address but the socket is already in use.
                                // the
                            }

                        }
                    }

                }

                if (my_id == -1) {
                    throw  new Exception("Error in reading the topology file, my ip is not in the topology");
                }


            } else {
                throw  new Exception("No ips found for the local host");
            }
        } catch (Exception  e) {
            System.out.println("Error in getting the local host");
            throw e;
        }

        if (serverSocket == null) {
            throw new Exception("Error in opening the server socket");
        }
        return serverSocket;
    }

    void putIfNotPresent(String key) {
        if (!db.containsKey(key)) {
            db.put(key, new Entry(null, 0, new State()));
        }
    }

    void changeLabel(String key, Label newLabel) {
        for (Connection c : peers.values()) {
            c.clearBindings(key);
        }
        db.get(key).getState().label = newLabel;
        db.get(key).getState().ackCounter = 0;
        db.get(key).getState().writeMaxVersion = -1;
        if (newLabel == Label.Idle && !aborted.isEmpty()) {
            Pair<Connection, PutRequest> p = aborted.pop();
            onPutRequest(p.getLeft(), p.getRight());
        }
        else {
            for (Connection c : peers.values()) {
                if (newLabel == Label.Idle) {
                    c.bindToMessage(new MessageFilter(key, ContactRequest.class), this::onContactRequest);
                    db.get(key).getState().toWrite = null;
                    db.get(key).getState().coordinator = null;
                } else if (newLabel == Label.Ready) {
                    c.bindToMessage(new MessageFilter(key, Abort.class), this::onAbort);
                    c.bindToMessage(new MessageFilter(key, ContactRequest.class), this::onContactRequest);
                    c.bindToMessage(new MessageFilter(key, Write.class), this::onWrite);
                } else if (newLabel == Label.Waiting) {
                    c.bindToMessage(new MessageFilter(key, ContactResponse.class), this::onContactRequest);
                    c.bindToMessage(new MessageFilter(key, Nack.class), this::onNack);
                    c.bindToMessage(new MessageFilter(key, ContactResponse.class), this::onContactResponse);
                }
            }
            for (Connection c : peers.values()) { //TODO: use clients list instead
                if (newLabel == Label.Idle) {
                    c.bindToMessage(new MessageFilter(key, PutRequest.class), this::onPutRequest);
                }
            }
        }
    }

    boolean onAbort(Connection c, Message msg) {
        //TODO: handle late abort
        changeLabel(msg.getKey(), Label.Idle);
        return true;
    }

    boolean onContactRequest(Connection c, Message msg) {
        putIfNotPresent(msg.getKey());
        int node = new ArrayList<>(peers.values()).indexOf(c);
        State s = db.get(msg.getKey()).getState();
        if(db.get(msg.getKey()).getState().label == Label.Waiting) {
            if (node > my_id) {
                changeLabel(msg.getKey(), Label.Aborted);
                for(int i = my_id; i < my_id + write_quorum; i++) {
                    peers.get(i % peers.size()).send(new Abort(msg.getKey()));
                }
                aborted.push(new ImmutablePair<>(s.writeClient, new PutRequest(msg.getKey(), s.toWrite)));
                changeLabel(msg.getKey(), Label.Ready);
                s.coordinator = node;
                c.send(new ContactResponse(msg.getKey(), db.get(msg.getKey()).getVersion()));
            }
            else {
                return false;
            }
        }
        else if (db.get(msg.getKey()).getState().label == Label.Ready){
            if (node > s.coordinator) {
                c.send(new Nack(msg.getKey(), s.coordinator));
            }
            else {
                return false;
            }
        }
        else if (db.get(msg.getKey()).getState().label == Label.Idle){
            changeLabel(msg.getKey(), Label.Ready);
            s.coordinator = node;
            c.send(new ContactResponse(msg.getKey(), db.get(msg.getKey()).getVersion()));
        }
        return true;
    }

    boolean onContactResponse(Connection ignored, Message msg) {
        //TODO: synchronize all callbacks on the key?
        ContactResponse contactResponse = (ContactResponse) msg;
        State s = db.get(msg.getKey()).getState();
        s.ackCounter++;
        if (contactResponse.getVersion() > s.writeMaxVersion)
            s.writeMaxVersion = contactResponse.getVersion();
        if (s.ackCounter == write_quorum) {
            changeLabel(contactResponse.getKey(), Label.Committed);
            for(int i = my_id; i < my_id + write_quorum; i++) {
                peers.get(i % peers.size()).send(new Write(msg.getKey(), s.toWrite, s.writeMaxVersion+1));
            }
            s.writeClient.send(new PutResponse(msg.getKey(), s.writeMaxVersion+1));
            s.writeClient.stop();
            changeLabel(contactResponse.getKey(), Label.Idle);
        }
        return true;
    }

    boolean onGetRequest(Connection c, Message msg) {
        GetRequest getRequest = (GetRequest) msg;
        putIfNotPresent(msg.getKey());
        State s = db.get(msg.getKey()).getState();
        if (!s.reading) {
            s.reading = true;
            s.readClient = c;
            for(int i = my_id; i < my_id + read_quorum; i++) {
                peers.get(i % peers.size()).send(new Read(msg.getKey()));
            }
            return  true;
        }
        else {
            return false;
        }
    }

    boolean onNack(Connection ignored, Message msg) {
        Nack nack = (Nack) msg;
        peers.get(nack.getNodeID()).send(new ContactRequest(msg.getKey()));
        return true;
    }

    boolean onPutRequest(Connection c, Message msg) {
        PutRequest putRequest = (PutRequest) msg;
        putIfNotPresent(msg.getKey());
        changeLabel(msg.getKey(), Label.Waiting);
        db.get(msg.getKey()).getState().toWrite = putRequest.getValue();
        db.get(msg.getKey()).getState().writeClient = c;
        for(int i = my_id; i < my_id + write_quorum; i++) {
            peers.get(i % peers.size()).send(new ContactRequest(msg.getKey()));
        }
        return true;
    }

    boolean onRead(Connection c, Message msg) {
        c.send(new ReadResponse(msg.getKey(), db.get(msg.getKey()).getValue(), db.get(msg.getKey()).getVersion()));
        return true;
    }

    boolean onReadResponse(Connection ignored, Message msg) {
        ReadResponse readResponse = (ReadResponse) msg;
        State s = db.get(msg.getKey()).getState();
        s.readCounter++;
        if (readResponse.getVersion() > s.readMaxVersion) {
            s.readMaxVersion = readResponse.getVersion();
            s.latestValue = readResponse.getValue();
        }
        if (s.readCounter == read_quorum) {
            s.readClient.send(new GetResponse(msg.getKey(), s.latestValue, s.readMaxVersion));
            s.readClient.stop();
            s.readMaxVersion = -1;
            s.latestValue = null;
            s.readClient = null;
            s.readCounter = 0;
            s.reading = false;
        }
        return true;
    }

    boolean onWrite(Connection ignored, Message msg) {
        Write write = (Write) msg; 
        db.get(write.getKey()).setValue(write.getValue());
        db.get(write.getKey()).setVersion(write.getVersion());
        changeLabel(write.getKey(), Label.Idle);
        return true;
    }
}

