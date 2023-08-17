package it.polimi.ds.networking;

import it.polimi.ds.networking.messages.Message;
import it.polimi.ds.utils.SafeLogger;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import java.util.function.BiPredicate;
import java.util.logging.Level;

public abstract class Connection {
    private final Socket socket;
    protected final ObjectInputStream reader;
    protected final ObjectOutputStream writer;
    private final SafeLogger logger;

    private final Thread listenThread;

    private final Inbox inbox;

    protected Connection(Socket socket, SafeLogger logger) throws IOException {
        this.socket = socket;
        writer = new ObjectOutputStream(socket.getOutputStream());
        reader = new ObjectInputStream(socket.getInputStream());
        this.logger = logger;
        this.inbox = new Inbox(logger, this);
        listenThread = new Thread(this::listenMessages);
        listenThread.start();
    }

    public void clearBindings(Topic topic) {
        inbox.clearBindings(topic);
    }

    public void bindToMessage(MessageFilter filter, BiPredicate<Connection, Message> action) {
        inbox.bindToMessage(filter, action);
    }

    /**
     * serialize the message and send it through the network, if the message is a move set sending order before
     * @param message the message to be serialized and sent
     */
    public void send(Message message) {
        try {
            synchronized (writer) {
                writer.writeObject(message);
            }
        } catch (IOException ignored) { /*ignored*/ }
    }

    /*
    /**
     * waits for a message that matches the filter
     * @param filter a list of class that compose the filter
     * @return the first received message or in queue to be processed that matches the filter
     */
    /*public Message waitMessage(MessageFilter filter) {
        Message m = inbox.pollFirstMatch(filter); // eventual message that was received before the call of waitMessage
        while (m == null) {  // if no compatible found wait for one

                try {
                    synchronized (this) {
                        wait();
                        m = inbox.pollFirstMatch(filter);
                    }
                } catch (InterruptedException e) {
                    logger.log(Level.WARNING, "Interrupted", e);
                    Thread.currentThread().interrupt();
                }
            }
        return m;
    }*/

    void listenMessages() {
        String toLog = "Listening for new messages from: " + socket.getInetAddress();
        logger.log(Level.INFO, toLog);
        while (isRunning()) {
            try {
                Message msg = (Message) reader.readObject();
                logger.log(Level.INFO, "Received message: " + msg);
                logger.log(Level.INFO, "No binding found");
                inbox.add(msg); //TODO: fifo channels
                /*synchronized (this) {
                    notifyAll();
                }*/
            } catch (IOException e) {
                toLog = "IOException when reading message: " + e.getMessage();
                logger.log(Level.SEVERE, toLog);
            } catch (ClassNotFoundException ignored) {}
        }
    }

    /**
     * checks if the connection is running by checking socket state
     * @return if the connection is still listening for messages
     */
    boolean isRunning() {
        return !socket.isClosed();
    }

    /**
     * stops the listen message loop by closing the socket that will raise an exception on readObject()
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
