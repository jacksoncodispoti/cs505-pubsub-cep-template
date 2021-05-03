package cs505pubsubcep;

import com.google.gson.reflect.TypeToken;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

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

    private DataSource ds;

    public DBEngine() {

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
				if(headers[i].toLowerCase().equals("beds")) {
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
    public Statement getStatement() {
	try(Connection conn = ds.getConnection()) {
		try(Statement stmt = conn.createStatement()) {
			return stmt;
		}
	}
	catch(Exception ex) {
		return null;
	}
    }
    public void initDB() {
	String createHospitals = "CREATE TABLE hospitals(" +
			"id VARCHAR(64), " +
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

        try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(createHospitals);
                    stmt.executeUpdate(createPatients);
		    loadData(stmt);
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
