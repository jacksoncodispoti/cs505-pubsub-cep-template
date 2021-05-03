package cs505pubsubcep.CEP;

import com.google.gson.Gson;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.stream.output.sink.InMemorySink;
import io.siddhi.core.util.transport.InMemoryBroker;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class CEPEngine {
    public int PositiveTests = 0;
    public int NegativeTests = 0;

    public static final String INPUT_STREAM_NAME = "PatientInStream";
    public static final String OUTPUT_STREAM_NAME = "PatientOutStream";
    private SiddhiManager siddhiManager;
    private SiddhiAppRuntime siddhiAppRuntime;
    private Map<String,String> topicMap;

    private Gson gson;

    public CEPEngine() {

        Class JsonClassSource = null;
        Class JsonClassSink = null;

        try {
            JsonClassSource = Class.forName("io.siddhi.extension.map.json.sourcemapper.JsonSourceMapper");
            JsonClassSink = Class.forName("io.siddhi.extension.map.json.sinkmapper.JsonSinkMapper");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        try {
            InMemorySink sink = new InMemorySink();
            sink.connect();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        topicMap = new ConcurrentHashMap<>();

        // Creating Siddhi Manager
        siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("sourceMapper:json",JsonClassSource);
        siddhiManager.setExtension("sinkMapper:json",JsonClassSink);
        gson = new Gson();
    }


    private String createStreams() {
	//Input stream
	String inputStreamName = "PatientInStream";
	String inputStreamAttributes = "first_name string, last_name string, mrn string, zip_code string, patient_status_code string";
	String sourceString = getSourceString(inputStreamName, inputStreamAttributes);

	//Output stream
	String outputStreamAttributes = "patient_status_code string, count long";
	String outputStream = getSinkString(OUTPUT_STREAM_NAME, outputStreamAttributes);

	//15 Second ZipCodePositiveStream
	String zipStream15Name = "ZipPositive15";
	String zipStream15Attributes = "zip_code string, count long";
	String zipStream15 = getSinkString(zipStream15Name, zipStream15Attributes);

	//30 Second ZipCodePositiveStream
	String zipStream30Name = "ZipPositive30";
	String zipStream30Attributes = "zip_code string, count long";
	String zipStream30 = getSinkString(zipStream30Name, zipStream30Attributes);

	return sourceString + outputStream + zipStream15 + zipStream30;
    }

    private String createTables() {
	return "";
    }
    private String createQueries() {
	String defaultQuery = " " +
		"from PatientInStream#window.timeBatch(5 sec) " +
                "select patient_status_code, count() as count " +
                "group by patient_status_code " +
                "insert into PatientOutStream; ";

        String zip15Query = " " +
                "FROM PatientInStream#window.timeBatch(15 sec)[patient_status_code == '2' OR patient_status_code == '5' OR patient_status_code == '6'] " +
                "SELECT zip_code, count() as count " +
		"INSERT INTO ZipPositive15;";

	String zip30Query = " " +
                "FROM PatientInStream#window.timeBatch(30 sec)[patient_status_code == '2' OR patient_status_code == '5' OR patient_status_code == '6'] " +
                "SELECT zip_code, count() as count " +
		"INSERT INTO ZipPositive30;";

	return defaultQuery + zip15Query + zip30Query;
    }
    private String createDatabase() {
	return createStreams() + createTables();
    }

    public void createCEP() {
        try {
	String createDatabaseString = createDatabase();
            siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(createDatabaseString);

            InMemoryBroker.Subscriber subscriberTest = new OutputSubscriber(topicMap.get(OUTPUT_STREAM_NAME), OUTPUT_STREAM_NAME);

            //subscribe to "inMemory" broker per topic
            InMemoryBroker.subscribe(subscriberTest);

            //Starting event processing
            siddhiAppRuntime.start();

            } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public void preProcess(Map<String, String> payload) {
	if(payload.containsKey("patient_status_code")) {
		String strStatusCode = payload.get("patient_status_code");
		int statusCode = Integer.parseInt(strStatusCode);

		if(statusCode == 1 || statusCode == 4) {
			PositiveTests += 1;
		}
		else if(statusCode == 2 || statusCode == 5 || statusCode == 6) {
			NegativeTests += 1;
		}
	}
    }

    public void input(String streamName, Map<String, String> payload) {
        try {
            if (topicMap.containsKey(streamName)) {
                //InMemoryBroker.publish(topicMap.get(streamName), getByteGenericDataRecordFromString(schemaMap.get(streamName),jsonPayload));
		String jsonPayload = gson.toJson(payload);
		preProcess(payload);
                InMemoryBroker.publish(topicMap.get(streamName), jsonPayload);

            } else {
                System.out.println("input error : no schema");
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }


    private String getSourceString(String streamName, String inputStreamAttributesString) {
        String sourceString = null;
        try {
		String topic = UUID.randomUUID().toString();
		topicMap.put(streamName, topic);
	
            sourceString  = "@source(type='inMemory', topic='" + topic + "', @map(type='json')) " +
                    "define stream " + streamName + " (" + inputStreamAttributesString + "); ";

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return sourceString;
    }

    private String getSinkString(String streamName, String outputSchemaString) {
        String sinkString = null;
        try {
		String topic = UUID.randomUUID().toString();
		topicMap.put(streamName, topic);
            sinkString = "@sink(type='inMemory', topic='" + topic + "', @map(type='json')) " +
                    "define stream " + streamName + " (" + outputSchemaString + "); ";

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return sinkString;
    }

}
