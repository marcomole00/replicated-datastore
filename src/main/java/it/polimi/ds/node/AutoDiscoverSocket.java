package it.polimi.ds.node;

import it.polimi.ds.networking.Address;
import it.polimi.ds.utils.Config;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;

public class AutoDiscoverSocket {
    ServerSocket serverSocket;
    int myId = -1;
    public AutoDiscoverSocket(Config config) throws Exception {
        try {
            InetAddress[] ips = Collections.list(NetworkInterface.getNetworkInterfaces()).stream().map(i -> Collections.list(i.getInetAddresses()).get(1)).toArray(InetAddress[]::new);
            if (ips  != null ) {
                for (int i= 0; i < ips.length; i++) {
                    System.out.println("IP address: " + ips[i].getHostAddress());
                    for (int j = 0; j < config.getNumberOfNodes(); j++) {
                        if (ips[i].getHostAddress().equals(config.getTopology().getIp(j))) {
                            myId = j;
                            Address my_address = config.getTopology().getNodes().get(j);
                            try {
                                serverSocket = new ServerSocket(my_address.getPort());
                                // the node found its address and a free socket.
                                System.out.println("Server socket opened on port " + my_address.getPort());
                                System.out.println("My id is " + myId);
                                return;
                            } catch (Exception e) {
                                // the node found its address but the socket is already in use.
                                // the
                            }
                        }
                    }
                }
                if (myId == -1) {
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
    }

    public Socket accept() throws IOException {
        return serverSocket.accept();
    }

    public int getMyId() {
        return myId;
    }
}
