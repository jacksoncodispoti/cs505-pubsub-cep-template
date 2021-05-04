package cs505pubsubcep.CEP;

import com.google.gson.Gson;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.stream.output.sink.InMemorySink;
import io.siddhi.core.util.transport.InMemoryBroker;
import io.siddhi.core.event.Event;
import io.siddhi.core.table.Table;

import java.io.*;
import java.util.*;
import java.util.Scanner;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import cs505pubsubcep.Launcher;

import com.opencsv.CSVReader;

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

		//Hospital source
		String hospitalStreamName = "HospitalInStream";
		String hospitalStreamAttributes = "id string, name string, address string, city string, state string, zip string, type string, beds string, county string, countyfips string, country string, latitude string, longitude string, naics_code string, website string, owner string, trauma string, helipad string";
		String hospitalString = getSourceString(hospitalStreamName, hospitalStreamAttributes);

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

		return sourceString + hospitalString + outputStream + zipStream15 + zipStream30;
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
			"GROUP BY zip_code " +
			"INSERT INTO ZipPositive15; ";

		String zip30Query = " " +
			"FROM PatientInStream#window.timeBatch(30 sec)[patient_status_code == '2' OR patient_status_code == '5' OR patient_status_code == '6'] " +
			"SELECT zip_code, count() as count " +
			"GROUP BY zip_code " +
			"INSERT INTO ZipPositive30; ";

		return defaultQuery + zip15Query + zip30Query;
	}
	private String createDatabase() {
		return createStreams() + createQueries();
	}

	private void loadHospitals(String file_path) throws IOException {
		Scanner scanner = new Scanner(new File(file_path));
		scanner.useDelimiter(",");
		scanner.nextLine();

		Reader reader = new FileReader(file_path);
		CSVReader csvReader = new CSVReader(reader);

		//skip header
		String[] headers = csvReader.readNext();

		String[] row;
		while((row = csvReader.readNext()) != null) {
			Map<String, String> hashMap = new HashMap<String, String>();

			for(int i = 0; i < headers.length; i++)
				hashMap.put(headers[i], row[i]);

			input("HospitalInStream", hashMap);
		}
	}

	private void loadData() throws IOException {
		loadHospitals("/home/ndfl222/cs505project/src/main/java/cs505pubsubcep/data/hospitals.csv");
	}

	public void restart() {
		PositiveTests = 0;
		NegativeTests = 0;
		siddhiAppRuntime.shutdown();
		siddhiAppRuntime = null;
		createCEP();
	}

	public void createCEP() {
		try {
			String createDatabaseString = createDatabase();
			siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(createDatabaseString);

			InMemoryBroker.Subscriber subscriber15 = new OutputSubscriber(topicMap.get("ZipPositive15"), "ZipPositive15");
			InMemoryBroker.Subscriber subscriber30 = new OutputSubscriber(topicMap.get("ZipPositive30"), "ZipPositive30");

			//subscribe to "inMemory" broker per topic
			InMemoryBroker.subscribe(subscriber15);
			InMemoryBroker.subscribe(subscriber30);

			//Starting event processing
			siddhiAppRuntime.start();

			loadData();

		} catch (Exception ex) {
			ex.printStackTrace();
		}

	}

	int getPatientAssignment(int statusCode, int zipCode) {
		if(statusCode < 3 || statusCode == 4) {
			return 0;
		}
		//Look for closest facility
		if(statusCode == 3 || statusCode == 5) {
			return Launcher.dbEngine.closestFacility(zipCode);
		}
		//Look for LEVEL IV or better facility
		else if(statusCode == 6) {
			return Launcher.dbEngine.closestLevelIV(zipCode);
		}

		return -1;
	}

	public void preProcess(Map<String, String> payload) {
		if(payload.containsKey("patient_status_code")) {
			String strStatusCode = payload.get("patient_status_code");
			int statusCode = Integer.parseInt(strStatusCode);

			//Increment counter
			if(statusCode == 1 || statusCode == 4) {
				PositiveTests += 1;
			}
			else if(statusCode == 2 || statusCode == 5 || statusCode == 6) {
				NegativeTests += 1;
			}

			//Do hospital assign logic
			String strZipCode = payload.get("zip_code");
			int zipCode = Integer.parseInt(strZipCode);
			int patient_assignment = getPatientAssignment(statusCode, zipCode);
			String mrn = payload.get("mrn");
			String first_name = payload.get("first_name");
			String last_name = payload.get("last_name");
			try {
				Launcher.dbEngine.assignHospital(mrn, first_name, last_name, patient_assignment);
			}
			catch(Exception ex) {
				System.out.format("Unable to assign patient %s: %s", ex.getClass().getCanonicalName(), ex.getMessage());
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
