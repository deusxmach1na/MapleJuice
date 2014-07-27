package com.kleck.MapleJuice;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

public class JuiceWordCount {
	public static void main(String[] args) {
		if(args.length != 1) {
			System.err.println("bad parameter list sent to the Juice task");
		}
		
		HashMap<String,Integer> hm = new HashMap<String,Integer>();
		//take in a file and group by key, then add up all the 1's
		File file = new File(args[0]);
		Scanner fileScan;
		try {
			fileScan = new Scanner(file);
			while(fileScan.hasNextLine()) {
				//get the key
				String line = fileScan.nextLine();
				String key = line.split(",")[0].replace("(", "").trim();
				int value = Integer.parseInt(line.split(",")[1].replace(")", "").trim());
				if(hm.containsKey(key)) {
					//add the value to the overall count
					hm.put(key, hm.get(key) + value);
				}
				else {
					hm.put(key, value);
				}
			}
		} catch (FileNotFoundException e) {
			System.err.println("ERROR: Could not find file");
		}
		
		//loop through and emit the reduced values
		for(String key:hm.keySet()) {
			System.out.println("( " + key + " , " + hm.get(key) + " )");
		}
	}
}
