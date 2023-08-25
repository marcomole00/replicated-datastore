package it.polimi.ds;

import it.polimi.ds.node.Node;

public class DatabaseNode {


    public static void main(String[] args) throws Exception {

        boolean debug = args.length > 0 &&( args[0].equals("--debug") || args[0].equals("-d"));

        System.out.println("Starting node on port");
        try {
           Node node = new Node();
           node.run(debug);
        } catch (Exception e) {
           System.out.println("Error in opening the server socket");
           e.printStackTrace();
        }
    }
}
