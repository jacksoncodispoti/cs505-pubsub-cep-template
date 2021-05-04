package cs505pubsubcep;

import cs505pubsubcep.CEP.CEPEngine;
import cs505pubsubcep.Topics.TopicConnector;
import cs505pubsubcep.httpfilters.AuthenticationFilter;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;


public class Launcher {

    public static final String API_SERVICE_KEY = "12200587"; //Change this to your student id
    public static final int WEB_PORT = 9000;
    public static long accessCount = -1;

    public static TopicConnector topicConnector;

    public static CEPEngine cepEngine = null;
    public static DBEngine dbEngine = null;

    public static int messageCounter = 0;
    public static String message30 = "";
    public static boolean[] isInAlert = new boolean[2789]; // ex if danville(zip=40422) is in alert, isInAlert[422] = true


    public static void restart() {
	//cepEngine.restart();
	dbEngine.restart();
    }
    public static void main(String[] args) throws IOException {


        System.out.println("Starting CEP...");
        //Embedded database initialization

        cepEngine = new CEPEngine();
	dbEngine = new DBEngine();


        //cepEngine.createCEP(inputStreamName, outputStreamName, inputStreamAttributesString, outputStreamAttributesString, queryString);
        cepEngine.createCEP();
	dbEngine.initDB();

        System.out.println("CEP Started...");


        //starting Collector
        topicConnector = new TopicConnector();
        topicConnector.connect();

        //Embedded HTTP initialization
        startServer();


        try {
            while (true) {
                Thread.sleep(5000);
            }
        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void startServer() throws IOException {

        final ResourceConfig rc = new ResourceConfig()
        .packages("cs505pubsubcep.httpcontrollers")
        .register(AuthenticationFilter.class);

        System.out.println("Starting Web Server...");
        URI BASE_URI = UriBuilder.fromUri("http://0.0.0.0/").port(WEB_PORT).build();
        HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(BASE_URI, rc);

        try {
            httpServer.start();
            System.out.println("Web Server Started...");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
