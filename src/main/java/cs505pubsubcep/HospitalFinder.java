package cs505pubsubcep;

import java.util.*;
import java.io.*;
import com.opencsv.CSVReader;
import java.util.stream.Collectors;

public class HospitalFinder {
	private HashMap<Integer, List<Integer>> closest = new HashMap<Integer, List<Integer>>();
	private HashMap<Integer, List<Integer>> closestLevelIV = new HashMap<Integer, List<Integer>>();

	public static class Hospital {
		public int id;
		public int zipCode;
		public String trauma;

		public Hospital() { }
		public Hospital(int id, int zipCode, String trauma) {
			this.id = id;
			this.zipCode = zipCode;
			this.trauma = trauma;
		}
	}

	private HospitalFinder() { }

	public List<Integer> getClosestIds(int zipCode) {
		if(closest.containsKey(zipCode)) {
			return closest.get(zipCode);
		}

		return new ArrayList<Integer>();
	}

	public List<Integer> getClosestIVIds(int zipCode) { 
		if(closestLevelIV.containsKey(zipCode)) {
			return closestLevelIV.get(zipCode);
		}

		return new ArrayList<Integer>();
	}

	public static HospitalFinder load(String file_path) throws Exception {
		Scanner scanner = new Scanner(new File(file_path));
		scanner.useDelimiter(",");
		scanner.nextLine();

		Reader reader = new FileReader(file_path);
		CSVReader csvReader = new CSVReader(reader);

		//skip header
		String[] headers = csvReader.readNext();

		String[] row;
		List<Hospital> hospitals = new ArrayList<Hospital>();

		while((row = csvReader.readNext()) != null) {
			Map<String, String> hashMap = new HashMap<String, String>();

			String currentValues = "(";
			Hospital currentHospital = new Hospital();
			for(int i = 0; i < headers.length; i++) {
				if(i == 0) {
					currentHospital.id = Integer.parseInt(row[i]);
				}
				else if(headers[i].toLowerCase().equals("zip")) {
					currentHospital.zipCode = Integer.parseInt(row[i]);
				}
				else if(headers[i].toLowerCase().equals("trauma")) {
					currentHospital.trauma = row[i];
				}
			}
			hospitals.add(currentHospital);
		}

		HospitalFinder finder = new HospitalFinder();
		Distances dists = Distances.load("/home/ndfl222/cs505project/src/main/java/cs505pubsubcep/data/kyzipdistance.csv");


		for(int zipCode : dists.zipCodes()) {
			List<Integer> closest = new ArrayList<Integer>();
			List<Integer> closestLevelIV = new ArrayList<Integer>();

			List<Hospital> sorted = hospitals.stream().sorted((h1, h2) -> Float.compare(dists.get_distance(h1.zipCode, zipCode), dists.get_distance(h2.zipCode, zipCode))).collect(Collectors.toList());


			for(Hospital h : sorted) {
				closest.add(h.id);

				if(h.trauma.contains("IV"))
					closestLevelIV.add(h.id);
			}

			finder.closest.put(zipCode, closest);
			finder.closestLevelIV.put(zipCode, closestLevelIV);
		}

		return finder;
	}
}
