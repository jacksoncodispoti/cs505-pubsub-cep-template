package cs505pubsubcep;

import com.google.gson.reflect.TypeToken;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import javax.sql.DataSource;
import java.io.File;
import java.io.*;
import java.util.Scanner;
import com.opencsv.CSVReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBEngine {
	private Connection cmdConnection;
	private Statement cmdStatement;
    private DataSource ds;
    private Gson gson;
    private HashMap<String, Boolean> patientMap = new HashMap<String, Boolean>();

    public DBEngine() {
        gson = new Gson();
        try {
            //Name of database
            String databaseName = "myDatabase";

            //Driver needs to be identified in order to load the namespace in the JVM
            String dbDriver = "org.apache.derby.jdbc.EmbeddedDriver";
            Class.forName(dbDriver).newInstance();

            //Connection string pointing to a local file location
            String dbConnectionString = "jdbc:derby:memory:" + databaseName + ";create=true";
            ds = setupDataSource(dbConnectionString);

            /*
            if(!databaseExist(databaseName)) {
                System.out.println("No database, creating " + databaseName);
                initDB();
            } else {
                System.out.println("Database found, removing " + databaseName);
                delete(Paths.get(databaseName).toFile());
                System.out.println("Creating " + databaseName);
                initDB();
            }
             */

            initDB();


        }

        catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public static DataSource setupDataSource(String connectURI) {
        //
        // First, we'll create a ConnectionFactory that the
        // pool will use to create Connections.
        // We'll use the DriverManagerConnectionFactory,
        // using the connect string passed in the command line
        // arguments.
        //
        ConnectionFactory connectionFactory = null;
        connectionFactory = new DriverManagerConnectionFactory(connectURI, null);


        //
        // Next we'll create the PoolableConnectionFactory, which wraps
        // the "real" Connections created by the ConnectionFactory with
        // the classes that implement the pooling functionality.
        //
        PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory, null);

        //
        // Now we'll need a ObjectPool that serves as the
        // actual pool of connections.
        //
        // We'll use a GenericObjectPool instance, although
        // any ObjectPool implementation will suffice.
        //
        ObjectPool<PoolableConnection> connectionPool =
                new GenericObjectPool<>(poolableConnectionFactory);

        // Set the factory's pool property to the owning pool
        poolableConnectionFactory.setPool(connectionPool);

        //
        // Finally, we create the PoolingDriver itself,
        // passing in the object pool we created.
        //
        PoolingDataSource<PoolableConnection> dataSource =
                new PoolingDataSource<>(connectionPool);

        return dataSource;
    }

	private List<String> loadHospitals(String file_path) throws IOException {
		Scanner scanner = new Scanner(new File(file_path));
		scanner.useDelimiter(",");
		scanner.nextLine();

		Reader reader = new FileReader(file_path);
		CSVReader csvReader = new CSVReader(reader);

		//skip header
		String[] headers = csvReader.readNext();

		String[] row;
		List<String> valueStrings = new ArrayList<String>();
		while((row = csvReader.readNext()) != null) {
			Map<String, String> hashMap = new HashMap<String, String>();

			String currentValues = "(";
			for(int i = 0; i < headers.length; i++) {
				if(headers[i].toLowerCase().equals("beds")/* || headers[i].toLowerCase().equals("id")*/) {
					currentValues += row[i];
				}
				else {
					currentValues += "'" + row[i].replace("'", "`") + "'";
				}
				currentValues += ",";
			}
			currentValues = currentValues.substring(0, currentValues.length() - 1);
			currentValues += ")";
			valueStrings.add(currentValues);
		}

		return valueStrings;
	}
    private void loadData(Statement stmt) throws Exception {
	    String query = "INSERT INTO hospitals (id, name, address, city, state, zip, type, beds, county, countyfips, country, latitude, longitude, naics_code, website, owner, trauma, helipad) VALUES ";
	    List<String> values = loadHospitals("/home/ndfl222/cs505project/src/main/java/cs505pubsubcep/data/hospitals.csv");

	    query += values.get(0);
	    
	    for(int i = 1; i < values.size(); i++)
		    query += ",\n " + values.get(i);

	stmt.executeUpdate(query);
    }
    public ResultSet RresultExecute(String query) {
	try(Connection connection = ds.getConnection()) {
		try(Statement statement = connection.createStatement()) {
			return statement.executeQuery(query);
		}
	}
	catch(Exception ex) {
		System.out.format("Error Apache Derby! %s:%s", ex.getClass().getCanonicalName(), ex.getMessage());
		return null;
	}
    }

    private void insertPatient(String mrn, String first_name, String last_name) {
	patientMap.put(mrn, true);
	String query = String.format("INSERT INTO patients (mrn, first_name, last_name, location_code) VALUES ('%s', '%s', '%s', -1)", mrn, first_name, last_name);

	executeUpdate(query);
    }
    private boolean isPatientInserted(String mrn) {
	if(patientMap.containsKey(mrn)) {
		return true;
	}
	else {
		return false;
	}
    }
    public void assignHospital(String mrn, String first_name, String last_name, int location_code) throws Exception {
	if(!isPatientInserted(mrn)) {
		insertPatient(mrn, first_name, last_name);
	}

	int location_code_old = getPatientLocation(mrn);

	if(location_code_old > 0) {
		String removePatient = "UPDATE hospitals SET used_beds = used_beds - 1 WHERE id LIKE '%" + location_code + "%'";
		executeUpdate(removePatient);
	}

	String query = "UPDATE patients SET location_code = " + location_code + " WHERE mrn LIKE '%" + mrn + "%'";
	executeUpdate(query);

	if(location_code > 0) {
		String updateHospital = "UPDATE hospitals SET used_beds = used_beds + 1 WHERE id LIKE '%" + location_code + "%'";
		executeUpdate(updateHospital);
	}
    }
    private int getPatientLocation(String mrn) throws Exception {
	try(Connection connection = ds.getConnection()) {
		try(Statement statement = connection.createStatement()) {
			String query = "SELECT location_code FROM patients WHERE mrn = '" + mrn + "'";

			ResultSet results = statement.executeQuery(query);

			while(results.next()) {
				int location_id = results.getInt(1);

				return location_id;
			}
		}
	}
	catch(Exception ex) {
		System.out.format("Error Apache Derby! %s:%s", ex.getClass().getCanonicalName(), ex.getMessage());
	}
	return -1;
    }

    public String getPatient(String mrn) throws Exception {
	int location_code = getPatientLocation(mrn);
	JsonObject response = new JsonObject();
	response.addProperty("location_code", location_code);
	response.addProperty("mrn", mrn);

	return gson.toJson(response);
    }

    public int closestFacility(int zipCode) {
	//TODO: REPLACE WITH LOGIC
	return 1174020;
    }
    public int closestLevelIV(int zipCode) {
	//TODO: REPLACE WITH LOGIC
	return 1174020;
    }
    public String getHospital(int id) {
	try(Connection connection = ds.getConnection()) {
		try(Statement statement = connection.createStatement()) {
			String query = "SELECT beds, used_beds, zip, id FROM hospitals WHERE id LIKE '%" + id + "%'";
			//String query = "SELECT beds, used_beds, zip FROM hospitals";
			ResultSet results = statement.executeQuery(query);

			while(results.next()) {
				int beds = results.getInt(1);
				int used_beds = results.getInt(2);
				int available_beds = beds - used_beds;
				String sid = results.getString(4);

				String zipCode = results.getString(3);
				JsonObject response = new JsonObject();
				response.addProperty("id", sid);
				response.addProperty("total_beds", beds);
				response.addProperty("available_beds", available_beds);
				response.addProperty("zipcode", zipCode);

				return gson.toJson(response);
			}
		}
	}
	catch(Exception ex) {
		System.out.format("Error Apache Derby! %s:%s", ex.getClass().getCanonicalName(), ex.getMessage());
	}
	return "Hospital not found";
    }

    public void restart() {
	if(tableExist("hospitals"))
		dropTable("hospitals");
	if(tableExist("patients"))
		dropTable("patients");
	if(tableExist("alerts"))
		dropTable("alerts");

	initDB();
    }

    public void initDB() {
	String createHospitals = "CREATE TABLE hospitals(" +
			"id VARCHAR(10), " +
			//"id INTEGER, " +
			"name VARCHAR(64)," +
			"address VARCHAR(64)," +
			"city VARCHAR(64)," +
			"state VARCHAR(2)," +
			"zip VARCHAR(5)," +
			"type VARCHAR(64)," +
			"beds INTEGER," +
			"county VARCHAR(64)," +
			"countyfips VARCHAR(64)," +
			"country VARCHAR(64)," +
			"latitude VARCHAR(16)," +
			"longitude VARCHAR(16)," +
			"naics_code VARCHAR(16)," +
			"website VARCHAR(128)," +
			"owner VARCHAR(64)," +
			"trauma VARCHAR(32)," +
			"helipad VARCHAR(2)," +
			"used_beds INTEGER DEFAULT 0" +
		")";
	
	String createPatients = "CREATE TABLE patients(" +
			"mrn VARCHAR(64)," +
			"first_name VARCHAR(64)," + 
			"last_name VARCHAR(64)," + 
			"location_code INTEGER DEFAULT -1" +
		")";

	String createAlertList = "CREATE TABLE alerts(" +
			"zip VARCHAR(5)," +
			"alerted int default 0," +
			"timestamp BIGINT" +
		")";

        try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
			cmdConnection = conn;
			cmdStatement = stmt;
			if(!tableExist("hospitals"))
			    cmdStatement.executeUpdate(createHospitals);
			if(!tableExist("patients"))
			    cmdStatement.executeUpdate(createPatients);
			if(!tableExist("alerts"))
			    cmdStatement.executeUpdate(createAlertList);
		    loadData(cmdStatement);
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }

    public int executeUpdate(String stmtString) {
        int result = -1;
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeUpdate(stmtString);
                stmt.close();
            } catch (Exception e) {

                e.printStackTrace();
            } finally {
                conn.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return  result;
    }

    public int dropTable(String tableName) {
        int result = -1;
        try {
            Connection conn = ds.getConnection();
            try {
                String stmtString = null;

                stmtString = "DROP TABLE " + tableName;

                Statement stmt = conn.createStatement();

                result = stmt.executeUpdate(stmtString);

                stmt.close();
            } catch (Exception e) {

                e.printStackTrace();
            } finally {
                conn.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

    /*
    public boolean databaseExist(String databaseName)  {
        return Paths.get(databaseName).toFile().exists();
    }
    */
    public boolean databaseExist(String databaseName)  {
        boolean exist = false;
        try {

            if(!ds.getConnection().isClosed()) {
                exist = true;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }

    public boolean tableExist(String tableName)  {
        boolean exist = false;

        ResultSet result;
        DatabaseMetaData metadata = null;

        try {
            metadata = ds.getConnection().getMetaData();
            result = metadata.getTables(null, null, tableName.toUpperCase(), null);

            if(result.next()) {
                exist = true;
            }
        } catch(java.sql.SQLException e) {
            e.printStackTrace();
        }

        catch(Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }

    public List<Map<String,String>> getAccessLogs() {
        List<Map<String,String>> accessMapList = null;
        try {

            accessMapList = new ArrayList<>();

            Type type = new TypeToken<Map<String, String>>(){}.getType();

            String queryString = null;

            //fill in the query
            queryString = "SELECT * FROM accesslog";

            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    try(ResultSet rs = stmt.executeQuery(queryString)) {

                        while (rs.next()) {
                            Map<String, String> accessMap = new HashMap<>();
                            accessMap.put("remote_ip", rs.getString("remote_ip"));
                            accessMap.put("access_ts", rs.getString("access_ts"));
                            accessMapList.add(accessMap);
                        }

                    }
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return accessMapList;
    }

}
