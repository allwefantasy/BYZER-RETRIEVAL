//package tech.mlsql.retrieval;
//
//import py4j.GatewayServer;
//
//public class RetrievalPy4JGateway {
//    private final LocalRetrievalMaster master;
//
//    public RetrievalPy4JGateway(LocalRetrievalMaster master) {
//        this.master = master;
//    }
//
//    public boolean createTable(String tableSettingStr) throws Exception {
//        return master.createTable(tableSettingStr);
//    }
//
//    public String search(String queryStr) throws Exception {
//        return master.search(queryStr);
//    }
//
//    public static void main(String[] args) {
//        LocalRetrievalMaster master = new LocalRetrievalMaster(...); // 初始化你的LocalRetrievalMaster
//        GatewayServer gatewayServer = new GatewayServer(new RetrievalPy4JGateway(master));
//        gatewayServer.start();
//        System.out.println("Py4J Gateway Server started");
//    }
//}