package cs505pubsubcep;

import com.opencsv.CSVReader;
import java.io.*;
import java.io.*;
import java.util.Scanner;
import java.util.HashMap;
import java.util.Set;
import java.lang.IllegalArgumentException;

public class Distances {
	private static int current_index = 0;
	private HashMap<Integer, Integer> zip_index_map;
	private float[][] distance_table;

	public Distances(int size) {
		zip_index_map = new HashMap<Integer, Integer>();
		distance_table = new float[size][size];
	}

	public Set<Integer> zipCodes() {
		return zip_index_map.keySet();	
	}

	public void set_distance(int zip_a, int zip_b, float distance) {
		if(!zip_index_map.containsKey(zip_a)) {
			zip_index_map.put(zip_a,  current_index++);	
		}
		if(!zip_index_map.containsKey(zip_b)) {
			zip_index_map.put(zip_b,  current_index++);	
		}

		distance_table[zip_index_map.get(zip_a)][zip_index_map.get(zip_b)] = distance;
	}

	public float get_distance(int zip_a, int zip_b) throws IllegalArgumentException {
		if(!zip_index_map.containsKey(zip_a) ||
			!zip_index_map.containsKey(zip_b)) {
			throw new IllegalArgumentException("Invaid zip code");
		}
		int a_index = zip_index_map.get(zip_a);
		int b_index = zip_index_map.get(zip_b);

		return distance_table[a_index][b_index];
	}

	public static Distances load(String file_path) throws Exception {
		Reader reader = new FileReader(file_path);
		CSVReader csvReader = new CSVReader(reader);

		HashMap<Integer, HashMap<Integer, Float>> map = new HashMap<Integer, HashMap<Integer, Float>>();

		String[] headers = csvReader.readNext();
		String[] row;
		
		int distinct_zip_count = 0;
		while((row = csvReader.readNext()) != null) {
			int zip_a = Integer.parseInt(row[0]);
			int zip_b = Integer.parseInt(row[1]);
			float distance = Float.parseFloat(row[2]);

			if(map.containsKey(zip_a)) {
				map.get(zip_a).put(zip_b, distance);	
			}
			else {
				HashMap<Integer, Float> nestedMap = new HashMap<Integer, Float>();
				nestedMap.put(zip_b, distance);
				map.put(zip_a, nestedMap);
				distinct_zip_count++;
			}
		}

		Distances distances = new Distances(distinct_zip_count);

		for(int i : map.keySet()) {
			for(int j : map.get(i).keySet()) {
				distances.set_distance(i, j, map.get(i).get(j));
			}
		}

		System.out.println("Loaded distances");
		return distances;
	}
}
