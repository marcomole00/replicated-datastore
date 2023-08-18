package it.polimi.ds.networking;

import it.polimi.ds.networking.messages.ContactRequest;
import it.polimi.ds.networking.messages.Message;
import it.polimi.ds.utils.SafeLogger;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.logging.Level;

public class Connection {
    private final Socket socket;
    protected final ObjectInputStream reader;
    protected final ObjectOutputStream writer;
    private final SafeLogger logger;

    private final Thread listenThread;

    private final Inbox inbox;

    protected Connection(Socket socket, SafeLogger logger, LockSet locks) throws IOException {
        this.socket = socket;
        writer = new ObjectOutputStream(socket.getOutputStream());
        reader = new ObjectInputStream(socket.getInputStream());
        this.logger = logger;
        this.inbox = new Inbox(logger, this, locks);
        listenThread = new Thread(this::listenMessages);
        listenThread.start();
    }

    public static Connection fromSocket(Socket socket, SafeLogger logger, LockSet locks) throws IOException {
        return new Connection(socket, logger, locks);
    }

    public static Connection fromAddress(Address address, SafeLogger logger, LockSet locks) throws IOException {
        return new Connection(new Socket(address.getIp(), address.getPort()), logger, locks);
    }

    public static Connection fromAddress(Address address, SafeLogger logger) throws IOException {
        return fromAddress(address, logger, new LockSet());
    }

    public void clearBindings(Topic topic) {
        inbox.clearBindings(topic);
    }

    public void bind(MessageFilter filter, BiPredicate<Connection, Message> action, boolean checkPrevious) {
        if(checkPrevious) {
            inbox.bindCheckPrevious(filter, action);
        }
        else {
            inbox.bind(filter, action);
        }
    }

    public void bind(MessageFilter filter, BiPredicate<Connection, Message> action) {
        bind(filter, action, true);
    }

    /**
     * serialize the message and send it through the network, if the message is a move set sending order before
     * @param message the message to be serialized and sent
     */
    public void send(Message message) {
        try {
            synchronized (writer) {
                writer.writeObject(message);
                logger.log(Level.INFO, "Sent message: " + message);
            }
        } catch (IOException ignored) { /*ignored*/ }
    }

    void listenMessages() {
        String toLog = "Listening for new messages from: " + socket.getInetAddress();
        logger.log(Level.INFO, toLog);
        while (isRunning()) {
            try {
                Message msg = (Message) reader.readObject();
                logger.log(Level.INFO, "Received message: " + msg);
                logger.log(Level.INFO, "No binding found");
                inbox.add(msg);
            } catch (IOException e) {
                toLog = "IOException when reading message: " + e.getMessage();
                logger.log(Level.SEVERE, toLog);
            } catch (ClassNotFoundException ignored) {}
        }
    }

    /**
     * checks if the connection is running by checking socket state
     * @return if the connection is still listening for messages
     * socket method is already synchronized properly
     */
    boolean isRunning() {
        return !socket.isClosed();
    }

    /**
     * stops the listen message loop by closing the socket that will raise an exception on readObject()
     * socket method is already synchronized properly
     */
    public void stop() {
        try {
            socket.close();
        } catch (IOException ignored) {}
    }

    /**
     * runs stop() and wait for the listen message thread to terminate
     */
    public void close() {
        try {
            stop();
            if (!Thread.currentThread().equals(listenThread))
                listenThread.join();
        } catch (InterruptedException e) {
            logger.log(Level.WARNING, "Interrupted", e);
            Thread.currentThread().interrupt();
        }
    }
}
