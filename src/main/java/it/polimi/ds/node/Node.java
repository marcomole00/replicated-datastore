package it.polimi.ds.node;

import it.polimi.ds.networking.*;
import it.polimi.ds.networking.messages.*;
import it.polimi.ds.utils.Config;
import it.polimi.ds.utils.SafeLogger;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.net.Socket;
import java.util.*;
import java.util.logging.Level;


public class Node {

    private final  HashMap<Integer, Connection> peers = new HashMap<>();

    SafeCounter contactCounter = new SafeCounter();

    SafeLogger logger = SafeLogger.getLogger(this.getClass().getName());

    Config config;

    AutoDiscoverSocket serverSocket;

    DataBase db = new DataBase();

    AbortedStack aborted = new AbortedStack();

    public void run() throws Exception {
        //for each node in the topology create a connection
        //start the server socket
        config = new Config();
        serverSocket = new AutoDiscoverSocket(config);
        PeerConnector peerConnector = new PeerConnector(peers, config.getTopology(), serverSocket.getMyId(), this);
        Thread connectorThread = new Thread(peerConnector);
        connectorThread.start();

        while(true) {
            try {
                Socket socket = serverSocket.accept();
                int id = config.getTopology().getId(socket.getInetAddress().getHostAddress());
                if (id == -1) {
                    logger.log(Level.WARNING ,"Received connection from unknown address " + socket.getInetAddress().getHostAddress());
                    continue;
                }
                Connection connection = new SocketConnection(socket, logger);
                connection.bindToMessage(new MessageFilter(Topic.any(), Presentation.class), this::onPresentation);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    boolean onPresentation(Connection connection, Message message) {

        Presentation p = (Presentation) message;

        if ( p.getId() < 0) {
            connection.clearBindings(Topic.any());
            connection.bindToMessage(new MessageFilter(Topic.any(), GetRequest.class), this::onGetRequest);
            connection.bindToMessage(new MessageFilter(Topic.any(), PutRequest.class), this::onPutRequest);
            System.out.println("a client connected");
        }
        else if (0 <= p.getId() && p.getId() < config.getNumberOfNodes()) {
            synchronized (peers) {
                peers.put(p.getId(), connection);
                System.out.println("Received connection from " + p.getId());
                connection.clearBindings(Topic.any());
                connection.bindToMessage(new MessageFilter(Topic.any(), Read.class), this::onRead);
                connection.bindToMessage(new MessageFilter(Topic.any(), ReadResponse.class), this::onReadResponse);
                connection.bindToMessage(new MessageFilter(Topic.any(), ContactRequest.class), this::onContactRequest);
            }
        }
        return false;
    }

    void changeState(String key, State newState) {
        for (Connection c : peers.values()) {
            c.clearBindings(Topic.fromString(key));
        }

        logger.log(Level.INFO, "Changing state(" + key + ") from " +  db.get(key).getMetadata().state +  " to " + newState);
        db.get(key).getMetadata().state = newState;
        db.get(key).getMetadata().ackCounter = 0;
        db.get(key).getMetadata().writeMaxVersion = -1;
        if (newState == State.Idle && !aborted.isEmpty()) {
            Pair<Connection, PutRequest> p = aborted.pop();
            onPutRequest(p.getLeft(), p.getRight());
        }
        else {
            for (Connection c : peers.values()) {
                if (newState == State.Idle) {
                    c.bindToMessage(new MessageFilter(Topic.fromString(key), ContactRequest.class), this::onContactRequest);
                    db.get(key).getMetadata().toWrite = null;
                    db.get(key).getMetadata().coordinator = null;
                    db.get(key).getMetadata().contactId = null;
                } else if (newState == State.Ready) {
                    c.bindToMessage(new MessageFilter(Topic.fromString(key), Abort.class), this::onAbort);
                    c.bindToMessage(new MessageFilter(Topic.fromString(key), ContactRequest.class), this::onContactRequest);
                    c.bindToMessage(new MessageFilter(Topic.fromString(key), Write.class), this::onWrite);
                } else if (newState == State.Waiting) {
                    c.bindToMessage(new MessageFilter(Topic.fromString(key), ContactRequest.class), this::onContactRequest);
                    c.bindToMessage(new MessageFilter(Topic.fromString(key), Nack.class), this::onNack);
                    c.bindToMessage(new MessageFilter(Topic.fromString(key), ContactResponse.class), this::onContactResponse);
                }
            }
        }
    }

    boolean onAbort(Connection c, Message msg) {
        //TODO: handle late abort
        changeState(msg.getKey(), State.Idle);
        return true;
    }

    boolean onContactRequest(Connection c, Message msg) {
        db.putIfNotPresent(msg.getKey());
        ContactRequest contactRequest = (ContactRequest) msg;
        int node = new ArrayList<>(peers.values()).indexOf(c);
        Metadata metadata = db.get(msg.getKey()).getMetadata();
        if(db.get(msg.getKey()).getMetadata().state == State.Waiting) {
            if (node > serverSocket.getMyId()) {
                changeState(msg.getKey(), State.Aborted);
                for(int i = serverSocket.getMyId()+1; i < serverSocket.getMyId() + config.getWriteQuorum(); i++) {
                    peers.get(i % config.getNumberOfNodes()).send(new Abort(msg.getKey()));
                }
                aborted.push(new ImmutablePair<>(metadata.writeClient, new PutRequest(msg.getKey(), metadata.toWrite)));
                changeState(msg.getKey(), State.Ready);
                metadata.coordinator = node;
                metadata.contactId = contactRequest.getContactId();
                c.send(new ContactResponse(msg.getKey(), db.get(msg.getKey()).getVersion(), metadata.contactId));
            }
            else {
                return false;
            }
        }
        else if (db.get(msg.getKey()).getMetadata().state == State.Ready){
            if (node > metadata.coordinator) {
                c.send(new Nack(msg.getKey(), metadata.coordinator, metadata.contactId));
            }
            else {
                return false;
            }
        }
        else if (db.get(msg.getKey()).getMetadata().state == State.Idle){
            changeState(msg.getKey(), State.Ready);
            metadata.coordinator = node;
            metadata.contactId = contactRequest.getContactId();
            c.send(new ContactResponse(msg.getKey(), db.get(msg.getKey()).getVersion(), metadata.contactId));
        }
        return true;
    }

    boolean onContactResponse(Connection ignored, Message msg) {
        //TODO: synchronize all callbacks on the key?
        ContactResponse contactResponse = (ContactResponse) msg;
        Metadata metadata = db.get(msg.getKey()).getMetadata();
        if (contactResponse.getContactId() != metadata.contactId)
            return true; // drop message
        metadata.ackCounter++;
        if (contactResponse.getVersion() > metadata.writeMaxVersion)
            metadata.writeMaxVersion = contactResponse.getVersion();
        if (metadata.ackCounter == config.getWriteQuorum()-1) {
            Write write = new Write(msg.getKey(), metadata.toWrite, metadata.writeMaxVersion+1);
            PutResponse putResponse = new PutResponse(msg.getKey(), metadata.writeMaxVersion+1);
            changeState(contactResponse.getKey(), State.Committed);
            db.get(msg.getKey()).setValue(write.getValue());
            db.get(msg.getKey()).setVersion(write.getVersion());
            for(int i = serverSocket.getMyId()+1; i < serverSocket.getMyId() + config.getWriteQuorum(); i++) {
                peers.get(i % config.getNumberOfNodes()).send(write);
            }
            metadata.writeClient.send(putResponse);
            metadata.writeClient.stop();
            changeState(contactResponse.getKey(), State.Idle);
        }
        return true;
    }

    boolean onGetRequest(Connection c, Message msg) {
        //GetRequest getRequest = (GetRequest) msg;
        System.out.println("Received get request for key " + msg.getKey());
        db.putIfNotPresent(msg.getKey());
        Metadata metadata = db.get(msg.getKey()).getMetadata();
        if (!metadata.reading) {
            metadata.reading = true;
            metadata.readMaxVersion = db.get(msg.getKey()).getVersion();
            metadata.latestValue = db.get(msg.getKey()).getValue();
            metadata.readClient = c;
            for(int i = serverSocket.getMyId()+1; i < serverSocket.getMyId() + config.getReadQuorum(); i++) {
                peers.get(i % config.getNumberOfNodes()).send(new Read(msg.getKey()));
            }
            return  true;
        }
        else {
            return false;
        }
    }

    boolean onNack(Connection ignored, Message msg) {
        Nack nack = (Nack) msg;
        if (nack.getContactId() != db.get(msg.getKey()).getMetadata().contactId)
            return true; // drop message
        peers.get(nack.getNodeID()).send(new ContactRequest(msg.getKey(), nack.getContactId()));
        return true;
    }

    boolean onPutRequest(Connection c, Message msg) {
        System.out.println("Received put request for key " + msg.getKey());
        PutRequest putRequest = (PutRequest) msg;
        db.putIfNotPresent(msg.getKey());
        Metadata metadata = db.get(msg.getKey()).getMetadata();
        if (metadata.state != State.Idle) {
            aborted.push(new ImmutablePair<>(c, putRequest));
            return true;
        }
        changeState(msg.getKey(), State.Waiting);
        metadata.toWrite = putRequest.getValue();
        metadata.writeMaxVersion = db.get(msg.getKey()).getVersion();
        metadata.writeClient = c;
        metadata.contactId = contactCounter.getAndIncrement();
        for(int i = serverSocket.getMyId()+1; i < serverSocket.getMyId() + config.getWriteQuorum(); i++) {
            peers.get(i % config.getNumberOfNodes()).send(new ContactRequest(msg.getKey(), metadata.contactId));
        }
        return true;
    }

    boolean onRead(Connection c, Message msg) {
        db.putIfNotPresent(msg.getKey());
        System.out.println("Received read request for key " + msg.getKey());
        Entry  entry = db.get(msg.getKey());
        String value = entry.getValue();
        int version = entry.getVersion();
        c.send(new ReadResponse(msg.getKey(), value, version));
        return true;
    }

    boolean onReadResponse(Connection ignored, Message msg) {
        ReadResponse readResponse = (ReadResponse) msg;
        Metadata metadata = db.get(msg.getKey()).getMetadata();
        metadata.readCounter++;
        if (readResponse.getVersion() > metadata.readMaxVersion) {
            metadata.readMaxVersion = readResponse.getVersion();
            metadata.latestValue = readResponse.getValue();
        }
        if (metadata.readCounter == config.getReadQuorum()-1) {
            metadata.readClient.send(new GetResponse(msg.getKey(), metadata.latestValue, metadata.readMaxVersion));
            metadata.readClient.stop();
            metadata.readMaxVersion = -1;
            metadata.latestValue = null;
            metadata.readClient = null;
            metadata.readCounter = 0;
            metadata.reading = false;
        }
        return true;
    }

    boolean onWrite(Connection ignored, Message msg) {
        Write write = (Write) msg;
        db.get(write.getKey()).setValue(write.getValue());
        db.get(write.getKey()).setVersion(write.getVersion());
        changeState(write.getKey(), State.Idle);
        return true;
    }
}

