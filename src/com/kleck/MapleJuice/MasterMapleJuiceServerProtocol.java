package com.kleck.MapleJuice;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MasterMapleJuiceServerProtocol {
	private FSServer fs;
	byte[] header = null;
	byte[] filedata = null;
	private String command;
	private String commandOptions;
	private String jarFile;
	private String prefix;
	
	
	public byte[] processInput(byte[] data, FSServer fs) {
		this.fs = fs;
		byte[] result = null;
		
		//master processes input
		//need to get the first x bytes of data for the command
		this.command = new String(Arrays.copyOfRange(data, 0, 16)).trim();
		//System.out.println(command);
		
		//System.out.println(command);
		//System.out.println(isFirst);
		//System.out.println(filename);
		if(command.trim().equals("put")) {
			result = new FileServerProtocol().processInput(data, fs);
		}
		else if(command.trim().equals("get")) {
			result = new FileServerProtocol().processInput(data, fs);
		}
		else if(command.trim().equals("del")) {
			result = new FileServerProtocol().processInput(data, fs);
		}
		else if(command.trim().equals("reb")) {
			result = new FileServerProtocol().processInput(data, fs);
		}
		else if(command.trim().equals("maple none")) {
			//System.out.println("maple worker started");
			result = new MapleJuiceServerProtocol().processInput(data, fs);
		}
		else if(command.trim().equals("juice none")) {
			//System.out.println("juice worker started");
			result = new MapleJuiceServerProtocol().processInput(data, fs);
		}
		else if(command.trim().equals("maple master")) {
			//get command options and send to master maple processor
			if(this.fs.getGs().getMembershipList().getMaster().equals(this.fs.getGs().getProcessId())) {
				this.commandOptions = new String(Arrays.copyOfRange(data, 0, data.length)).trim();
				//System.out.println("Im here");
				result = this.processMapleMaster();
			}
			//handle master failure here
			else {
				result = "Not the master".getBytes();
			}
		}
		else if(command.trim().equals("juice master")) {
			//get command options and send to master juice processor
			this.commandOptions = new String(Arrays.copyOfRange(data, 0, data.length)).trim();	
			result = this.processJuiceMaster();
		}
		
		return result;
	}

	private byte[] processMapleMaster() {
		byte[] result = "".getBytes();
		List<String> options = this.parseCommandOptions(this.commandOptions);
		List<String> filesToMap = new ArrayList<String>();
		List<DFSClientThread> mapleThreads = new ArrayList<DFSClientThread>();
		List<String> completedMaples = new ArrayList<String>();
		
		//System.out.println("Options size" + options.size());
		//first option is the jar file
		//for(int i=0;i<options.size();i++) {
		//	System.out.println(options.get(i));
		//System.out.println("param size = " + options.size());
		//}
		this.jarFile = options.get(2);
		
		//2nd option is the file prefix
		this.prefix = options.get(3);
		
		for(int i=0;i<options.size()-4;i++) {
			filesToMap.add(options.get(i+4));  //4 parameter offset
		}
		
		//do the below until all maple files complete
		while(completedMaples.size() < filesToMap.size()) {
		
			//rest of the options are what we need to run maple on
			for(int i=0;i<filesToMap.size();i++) {
				//only try to map this file if it is not completed
				if(!completedMaples.contains(filesToMap.get(i))) {
				//choose a maple worker and tell it to start
	
					//send the command to a random server
					byte[] command = this.formMJCommand("maple none", jarFile, filesToMap.get(i), prefix);
					String ipAddress = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getSendToProcess(filesToMap.get(i))).getIpAddress();
					int portNumber = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getSendToProcess(filesToMap.get(i))).getFilePortNumber();
					//System.out.println("master sending maple command to worker" + portNumber);
					mapleThreads.add(new DFSClientThread(ipAddress, portNumber, "maple none", command));
					mapleThreads.get(i).start();
				}
			}
			
			//wait for processes to finish and see if they finished correctly
		    //wait for all threads to return
		    for(int i=0;i<mapleThreads.size();i++){
		    	try {
		    		mapleThreads.get(i).join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		    }
		    
		    //find the completed files on the local system
		    File folder = new File(".");
			File[] listOfFiles = folder.listFiles();
			for(int i=0;i<listOfFiles.length;i++) {
				if(listOfFiles[i].isFile() && listOfFiles[i].toString().contains("MAPCOMPLETE_")) {
					//add the file to the list of completed files
					String addMe = listOfFiles[i].toString().split("_DELIM_")[1];
					if(!completedMaples.contains(addMe)) {
						completedMaples.add(addMe);
					}
				}
			}
		}
		
		//should have all the files group and concatenate by key
		this.groupFilesByKey();
	    
	    //delete the MAPCOMPLETE_ files 
	    //we have the data we need
	    File folder = new File(".");
		File[] listOfFiles = folder.listFiles();
		for(int i=0;i<listOfFiles.length;i++) {
			if(listOfFiles[i].isFile() && listOfFiles[i].toString().contains("MAPCOMPLETE_")) {
				listOfFiles[i].delete();
			}
		}
		
	    //see if there were any maple worker failures
	    result = "Maple Master Complete".getBytes();
		return result;
		
	}

	//after the mapper files run this will group all the files by key
	private void groupFilesByKey() {
		List<String> filesToInclude = new ArrayList<String>();
		List<String> keys = new ArrayList<String>();
		 //find the completed files on the local system
	    File folder = new File(".");
		File[] listOfFiles = folder.listFiles();
		for(int i=0;i<listOfFiles.length;i++) {
			if(listOfFiles[i].isFile() && listOfFiles[i].toString().contains("MAPCOMPLETE_")) {
				//add the file to the list of completed files only once
				if(!filesToInclude.contains(listOfFiles[i].toString())) {
					filesToInclude.add(listOfFiles[i].toString());
					if(!keys.contains(listOfFiles[i].toString().split("_DELIM_")[3])) {
						keys.add(listOfFiles[i].toString().split("_DELIM_")[3]);
					}
				}
			}
		}
		
		//System.out.println("Merging " + filesToInclude.size() + " many files.");
		
		//loop through each key and merge files
		List<String> completedFiles = new ArrayList<String>();
		for(String s:keys) {
			List<String> filesToMerge = new ArrayList<String>();
			for(int i=0;i<filesToInclude.size();i++) {
				//if it has the key in the file name then merge it
				if(filesToInclude.get(i).endsWith("_DELIM_" + this.prefix + "_DELIM_" + s) && !completedFiles.contains(filesToInclude.get(i))) {
					filesToMerge.add(filesToInclude.get(i));
				}
			}
			//now send the files over for merging
			this.mergeIntermediateFiles(filesToMerge);
			for(int j=0;j<filesToMerge.size();j++) {
				//System.out.println("Merging files" + filesToMerge.get(j));
				completedFiles.add(filesToMerge.get(j));
			}
			
			//distribute the finished files over hdfs
			this.fs.getGs().replicateFiles(this.prefix + "_");
		}
	}

	//merges intermediate files after mapper is done
	private void mergeIntermediateFiles(List<String> filesToMerge) {
		List<String> lines = new ArrayList<String>();
		for(int i=0;i<filesToMerge.size();i++) {	
			try {
				//read from the files
				FileReader fileReader = new FileReader(filesToMerge.get(i));
				BufferedReader br = new BufferedReader(fileReader);
				String line = "";
				while ((line = br.readLine()) != null) {
				    lines.add(line);
				}
				br.close();
				fileReader.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		//sort the lines and write them back to a new file name
		try {
			Collections.sort(lines);
			String newFilename = filesToMerge.get(0).toString().split("_DELIM_")[2] + "_" + filesToMerge.get(0).toString().split("_DELIM_")[3];
			FileWriter fileWriter;
			fileWriter = new FileWriter(newFilename);
			PrintWriter out = new PrintWriter(fileWriter);
			for (String outputLine : lines) {
				out.println(outputLine);
			}
			out.flush();
			out.close();
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		lines = null;
	}

	
	
	//JUICER
	private byte[] processJuiceMaster() {
		byte[] result = "".getBytes();
		
		return result;
		
	}
	
	//parse the command options
	private List<String> parseCommandOptions(String commandOptions) {
		//System.out.println(commandOptions);
		return Arrays.asList(commandOptions.split(" "));
	}
	
	//turns the command into a byte array
	public byte[] formFileCommand(String commandType, String filename, boolean b, byte[] data) {
		byte[] result = new byte[data.length + 128];
		byte[] com = new byte[16];
		com = Arrays.copyOf(commandType.getBytes(), 16);
		byte[] file = new byte[96];
		file = Arrays.copyOf(filename.getBytes(), 96);
		byte[] isFirst = new byte[16];
		if(b)
			isFirst = Arrays.copyOf("true".getBytes(), 16);
		else
			isFirst = Arrays.copyOf("false".getBytes(), 16);
		
		//System.out.println(file.length);
		System.arraycopy(com, 0, result, 0, 16);
		System.arraycopy(isFirst, 0, result, 16, 16);
		System.arraycopy(file, 0, result, 32, 96);
		System.arraycopy(data, 0, result, 128, data.length);
		//result = this.concatenateByte(com, file);
		//result = this.concatenateByte(result, isFirst);
		//result = this.concatenateByte(result, data);
		return result;
	}
	
	//turns the command into a byte array
	public byte[] formMJCommand(String commandType, String jarFile, String filename, String interFile) {
		byte[] result = new byte[512];
		byte[] com = new byte[16];
		com = Arrays.copyOf(commandType.getBytes(), 16);
		byte[] jarFileByte = new byte[96];
		jarFileByte = Arrays.copyOf(jarFile.getBytes(), 96);
		byte[] filenameByte = new byte[200];
		filenameByte = Arrays.copyOf(filename.getBytes(), 200);
		byte[] interFileByte = new byte[200];
		interFileByte = Arrays.copyOf(interFile.getBytes(), 200);
		
		//System.out.println(file.length);
		System.arraycopy(com, 0, result, 0, 16);
		System.arraycopy(jarFileByte, 0, result, 16, 96);
		System.arraycopy(filenameByte, 0, result, 112, 200);
		System.arraycopy(interFileByte, 0, result, 312, 200);
		return result;
	}
	
}
