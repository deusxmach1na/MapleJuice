package com.kleck.MapleJuice;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class MapleWordCount {
	public static void main(String[] args) {
		if(args.length != 1) {
			System.err.println("bad parameter list sent to the Maple task");
		}
		
		//take in a file and emit (word, 1) for each word encountered
		File file = new File(args[0]);
		Scanner fileScan;
		try {
			fileScan = new Scanner(file);
			while(fileScan.hasNext()) {
				System.out.println("( " + fileScan.next() + " , 1 )");
			}
		} catch (FileNotFoundException e) {
			System.err.println("Could not find file");
		}
	}
}
