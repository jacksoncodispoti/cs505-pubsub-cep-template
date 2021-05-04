package cs505pubsubcep.httpcontrollers;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import cs505pubsubcep.CEP.accessRecord;
import cs505pubsubcep.Launcher;

import io.siddhi.core.event.Event;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.PathParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

@Path("/api")
public class API {

    @Inject
    private javax.inject.Provider<org.glassfish.grizzly.http.server.Request> request;

    private Gson gson;

    public API() {
        gson = new Gson();
    }

    @GET
    @Path("/getteam")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getteam(@HeaderParam("X-Auth-API-Key") String authKey) {
	Map<String,String> responseMap = new HashMap<>();

	JsonObject response = new JsonObject();

	response.addProperty("team_name", "Nick and Jackson");
	response.addProperty("app_status_code", "1");

	JsonArray teamMembers = new JsonArray();
	teamMembers.add(12200587);
	teamMembers.add(12346767);

	response.add("team_member_sids", teamMembers);
	String responseString = gson.toJson(response);

	return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reset(@HeaderParam("X-Auth-API-Key") String authKey) {
	return null;
    }
    @GET
    @Path("/zipalertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response ziperalertlist(@HeaderParam("X-Auth-API-Key") String authKey) {

	JsonObject response = new JsonObject();

	JsonArray zips = new JsonArray();
        for(int i=0;i<Launcher.isInAlert.length;i++)
            if(Launcher.isInAlert[i])
                zips.add(40000 + i);
                
	response.add("ziplist", zips);
	
	String responseString = gson.toJson(response);

	return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/alertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response alertlist(@HeaderParam("X-Auth-API-Key") String authKey) {

	JsonObject response = new JsonObject();

        int count = 0;
        for(int i=0;i<Launcher.isInAlert.length;i++)
            if(Launcher.isInAlert[i])
                count++;
        
        if(count>=5)      
	    response.addProperty("state_status", 1);
	else   
	    response.addProperty("state_status", 0);
	    
	String responseString = gson.toJson(response);

	return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/testcount")
    @Produces(MediaType.APPLICATION_JSON)
    public Response testcount(@HeaderParam("X-Auth-API-Key") String authKey) {
	Map<String,String> responseMap = new HashMap<>();

	JsonObject response = new JsonObject();

	response.addProperty("positive_test", Launcher.cepEngine.PositiveTests);
	response.addProperty("negative_test", Launcher.cepEngine.NegativeTests);

	String responseString = gson.toJson(response);

	return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getpatient/{mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPatient(@HeaderParam("X-Auth-API-Key") String authKey, @PathParam("mrn") String mrn) {
	    try {
		    String patientInfo = Launcher.dbEngine.getPatient(mrn);

		    return Response.ok(patientInfo).header("Access-Control-Allow-Origin", "*").build();
	    }
	    catch(Exception ex) {
		return Response.ok(ex.getMessage()).header("Access-Control-Allow-Origin", "*").build();
	    }
    }

    @GET
    @Path("/gethospital/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response gethospital(@HeaderParam("X-Auth-API-Key") String authKey, @PathParam("id") int id) {
	try {
		String hospitalInfo = Launcher.dbEngine.getHospital(id);

		return Response.ok(hospitalInfo).header("Access-Control-Allow-Origin", "*").build();
	}
	catch(Exception ex) {
		return Response.ok(ex.getMessage()).header("Access-Control-Allow-Origin", "*").build();
	}
    }


    //check local
    //curl --header "X-Auth-API-key:1234" "http://localhost:8082/api/checkmycep"

    //check remote
    //curl --header "X-Auth-API-key:1234" "http://[linkblueid].cs.uky.edu:8082/api/checkmycep"
    //curl --header "X-Auth-API-key:1234" "http://localhost:8081/api/checkmycep"

    //check remote
    //curl --header "X-Auth-API-key:1234" "http://[linkblueid].cs.uky.edu:8081/api/checkmycep"

    //@GET
    //@Path("/checkmycep")
    //@Produces(MediaType.APPLICATION_JSON)
    //public Response checkMyEndpoint(@HeaderParam("X-Auth-API-Key") String authKey) {
    //    String responseString = "{}";
    //    try {

    //        //get remote ip address from request
    //        String remoteIP = request.get().getRemoteAddr();
    //        //get the timestamp of the request
    //        long access_ts = System.currentTimeMillis();
    //        System.out.println("IP: " + remoteIP + " Timestamp: " + access_ts);

    //        Map<String,String> responseMap = new HashMap<>();
    //        if(Launcher.cepEngine != null) {

    //                responseMap.put("success", Boolean.TRUE.toString());
    //                responseMap.put("status_desc","CEP Engine exists");

    //        } else {
    //            responseMap.put("success", Boolean.FALSE.toString());
    //            responseMap.put("status_desc","CEP Engine is null!");
    //        }

    //        responseString = gson.toJson(responseMap);


    //    } catch (Exception ex) {

    //        StringWriter sw = new StringWriter();
    //        ex.printStackTrace(new PrintWriter(sw));
    //        String exceptionAsString = sw.toString();
    //        ex.printStackTrace();

    //        return Response.status(500).entity(exceptionAsString).build();
    //    }
    //    return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    //}

    //@GET
    //@Path("/getaccesscount")
    //@Produces(MediaType.APPLICATION_JSON)
    //public Response getAccessCount(@HeaderParam("X-Auth-API-Key") String authKey) {
    //    String responseString = "{}";
    //    try {

    //        //get remote ip address from request
    //        String remoteIP = request.get().getRemoteAddr();
    //        //get the timestamp of the request
    //        long access_ts = System.currentTimeMillis();
    //        System.out.println("IP: " + remoteIP + " Timestamp: " + access_ts);

    //        //generate event based on access
    //        String inputEvent = gson.toJson(new accessRecord(remoteIP,access_ts));
    //        System.out.println("inputEvent: " + inputEvent);

    //        //send input event to CEP
    //        Launcher.cepEngine.input(Launcher.inputStreamName, inputEvent);

    //        //generate a response
    //        Map<String,String> responseMap = new HashMap<>();
    //        responseMap.put("accesscoint",String.valueOf(Launcher.accessCount));
    //        responseString = gson.toJson(responseMap);

    //    } catch (Exception ex) {

    //        StringWriter sw = new StringWriter();
    //        ex.printStackTrace(new PrintWriter(sw));
    //        String exceptionAsString = sw.toString();
    //        ex.printStackTrace();

    //        return Response.status(500).entity(exceptionAsString).build();
    //    }
    //    return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    //}
}
