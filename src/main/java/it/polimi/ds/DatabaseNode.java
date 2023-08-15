package it.polimi.ds;

import it.polimi.ds.node.Node;

public class DatabaseNode {


    public static void main(String[] args) throws Exception {
        System.out.println("Starting client on port");
       try {
           Node node = new Node();
           node.run();
       } catch (Exception e) {
           System.out.println("Error in opening the server socket");
           e.printStackTrace();
       }
    }
}
