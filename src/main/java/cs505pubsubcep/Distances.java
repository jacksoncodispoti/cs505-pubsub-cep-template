package cs505cep;

import java.io.*;
import java.util.Scanner;

public class Distances {
	private static int current_index = 0;
	private HashMap<int, short> zip_index_map;
	private float[][] distance_table;

	public Distances(int size) {
		zip_index_map = new HashMap<int, short>();
		distance_table = new float[size][size];
	}

	public void set_distance(int zip_a, int zip_b, float distance) {
		if(!zip_index_map.containsKey(zip_a)) {
			zip_index.set(zip_a,  current_index++);	
		}
		if(!zip_index_map.containsKey(zip_b)) {
			zip_index.set(zip_b,  current_index++);	
		}

		distance_table[zip_index_map.get(zip_a)][zip_index_map.get(zip_b)] = distance;
	}

	public float get_distance(int zip_a, int zip_b) throws InvalidArgumentException {
		if(!zip_index_map.containsKey(zip_a) ||
			!zip_index_map.containsKey(zip_b)) {
			throw new IllegalArgumentException("Invaid zip code");
		}
		short a_index = zip_index_map.get(zip_a);
		short b_index = zip_index_map.get(zip_b);

		return distance_table[a_index][b_index];
	}

	public static Distances load(String file_path) {
		Scanner scanner = new Scanner(new File(file_path));
		wc.useDelimiter(",");
		HashMap<int, HashMap<int, float>> map = new HashMap<int, HashMap<int, float>>();

		int distinct_zip_count = 0;
		while(sc.hasNext()) {
			int zip_a = wc.nextInt();
			int zip_b = wc.nextInt();
			float distance = wc.nextFloat();

			if(map.containsKey(zip_a)) {
				map.get(zip_a).set(zip_b, distance);	
				distinct_zip_count++;
			}
			else {
				HashMap<int, float> nestedMap = HashMap<int, float>();
				nestedMap.set(zip_b, distance);
				map.set(zip_a, nestedMap);
			}
		}

		Distances distances = new Distances(distinct_zip_count);

		for(int i : map.keySet()) {
			for(int j : map.get(i).keySet()) {
				distances.set_distance(i, j, map.get(i).get(j));
			}
		}

		return distances;
	}
}
