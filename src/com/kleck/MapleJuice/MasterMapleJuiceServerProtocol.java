package com.kleck.MapleJuice;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MasterMapleJuiceServerProtocol {
	private FSServer fs;
	byte[] header = null;
	byte[] filedata = null;
	private String command;
	private String commandOptions;
	private String jarFile;
	private String prefix;
	private int numJuices;
	private String destFile;
	
	
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
		else if(command.trim().equals("get") || command.trim().equals("getserv")) {
			result = new FileServerProtocol().processInput(data, fs);
		}
		else if(command.trim().equals("del")) {
			result = new FileServerProtocol().processInput(data, fs);
		}
		else if(command.trim().equals("reb") || command.trim().equals("find")) {
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
				LoggerThread lt = new LoggerThread(this.fs.getGs().getProcessId(), "#MASTER_MAP_JOB_STARTED#");
				lt.start();	 
				this.commandOptions = new String(Arrays.copyOfRange(data, 0, data.length)).trim();
				//System.out.println("Im here");
				result = this.processMapleMaster();
				LoggerThread lt2 = new LoggerThread(this.fs.getGs().getProcessId(), "#MASTER_MAP_JOB_FINISHED#");
				lt2.start();	 
			}
			//handle master failure here
			else {
				result = "Not the master".getBytes();
			}
		}
		else if(command.trim().equals("juice master")) {
			//get command options and send to master juice processor
			if(this.fs.getGs().getMembershipList().getMaster().equals(this.fs.getGs().getProcessId())) {
				LoggerThread lt = new LoggerThread(this.fs.getGs().getProcessId(), "#MASTER_JUICE_JOB_STARTED#");
				lt.start();	 
				this.commandOptions = new String(Arrays.copyOfRange(data, 0, data.length)).trim();
				//System.out.println("Im here");
				result = this.processJuiceMaster();
				LoggerThread lt2 = new LoggerThread(this.fs.getGs().getProcessId(), "#MASTER_JUICE_JOB_FINISHED#");
				lt2.start();	 
			}
			//handle master failure here
			else {
				result = "Not the master".getBytes();
			}
		}
		
		return result;
	}

	//MAPLER
	//needs to do the following:
	//1. spin up a new maple worker for each filename user has passed
	//2. keep track of completed files 
	//3. merge files at the end
	//4. put the new files on dfs and clean up
	private byte[] processMapleMaster() {
		byte[] result = "".getBytes();
		List<String> options = this.parseCommandOptions(this.commandOptions);
		List<String> filesToMap = new ArrayList<String>();
		HashMap<String, String> fileCompletionStatus = new HashMap<String,String>();
		List<String> listOfFinishedFiles = new ArrayList<String>();
		
		
		//List<String> completedMaples = new ArrayList<String>();
		//List<String> redoQueue = new ArrayList<String>();
		
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
			fileCompletionStatus.put(options.get(i+4), "Incomplete");
		}
		
		//do the below until all maple files complete
		//LoggerThread lt = new LoggerThread(this.fs.getGs().getProcessId(), "#MASTER_MAP_TASK_STARTED#");
		//lt.start();	 
		while(fileCompletionStatus.values().contains("Incomplete")) {
			HashMap<String, DFSClientThread> mapleThreads = new HashMap<String, DFSClientThread>();
			//rest of the options are what we need to run maple on
			for(int i=0;i<filesToMap.size();i++) {
				//only try to map this file if it is not completed
				if(fileCompletionStatus.get(filesToMap.get(i)).equals("Incomplete")) {
				//choose a maple worker and tell it to start
	
					//send the command to a random server
					byte[] command = this.formMJCommand("maple none", jarFile, filesToMap.get(i), prefix);
					String potentialProcess = this.fs.getGs().getSendToProcess(filesToMap.get(i));
					if(potentialProcess.equals(this.fs.getGs().getProcessId())) {
						potentialProcess = this.fs.getGs().getSuccessor();
					}
						
					String ipAddress = this.fs.getGs().getMembershipList().getMember(potentialProcess).getIpAddress();
					int portNumber = this.fs.getGs().getMembershipList().getMember(potentialProcess).getFilePortNumber();
					//System.out.println("master sending maple command to worker" + portNumber);
					LoggerThread lt = new LoggerThread(this.fs.getGs().getProcessId(), "#WORKER_MAP_TASK_SENT#" + potentialProcess);
					lt.start();	 
					mapleThreads.put(filesToMap.get(i), new DFSClientThread(ipAddress, portNumber, "maple none", command));
					mapleThreads.get(filesToMap.get(i)).start();
				}
			}
			
			//wait for processes to finish and see if they finished correctly
		    //wait for all threads to return
		    for(String s:mapleThreads.keySet()){
		    	try {
		    		mapleThreads.get(s).join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		    }
		    
		    //get completed file names
		    //System.out.println("here" + mapleThreads.keySet());
		    for(String key: mapleThreads.keySet()){
			    String filesOnDFS = "";
		    	//System.out.println(mapleThreads.get(key).getResponse());
		    	//if the response does not start with ERROR: then 
		    	if(mapleThreads.get(key).getResponse().contains("ERROR")) {
		    		//do nothing so it can redo the maple
		    	}
		    	else {
		    		fileCompletionStatus.put(key, "Completed");
		    		filesOnDFS += " " + mapleThreads.get(key).getResponse();		    
				    //reuse parse command options and add finished file names to array
		    		for(String s:parseCommandOptions(filesOnDFS)) {
		    			if(!s.equals("")) {
		    				listOfFinishedFiles.add(s);
		    			}
		    		}
		    	}
		    }
		}
		
		//should have all the files split them by key now
		List<String> keyFiles = this.splitFiles(listOfFinishedFiles);
		//List<String> keyFiles = this.groupFilesByKey(listOfFinishedFiles);
		//put them on dfs quick
		this.putFilesOnDFSByList(keyFiles);

		//lt = new LoggerThread(this.fs.getGs().getProcessId(), "#MASTER_MAP_TASK_FINISHED#");
		//lt.start();	 
		
	    //delete the MAPCOMPLETE_ files from the dfs
	    //we have the data we need
		//put the key files on the DFS and clean up local stuff
		this.cleanUpLocal();
		
	    //return...finally!!!!!!
	    result = "Maple Master Complete".getBytes();
		return result;
		
	}



	//JUICER
	//needs to do the following
	//1. give 1 juicer task per num_juicers until all files are consumed
	//2. keep track of finished files
	//3. merge the files
	//4. put the files on the dfs and clean up
	private byte[] processJuiceMaster() {
		byte[] result = "".getBytes();
		//should only be 4 commandOptions
		//commandOptions.size == 6
		List<String> options = this.parseCommandOptions(this.commandOptions);
		List<String> filesToJuice = new ArrayList<String>();
		//HashMap<String, String> fileCompletionStatus = new HashMap<String,String>();
		HashMap<String, String> juiceCompletionStatus = new HashMap<String,String>();
		List<String> listOfFinishedFiles = new ArrayList<String>();
		
		this.jarFile = options.get(2);
		//System.out.println("jar file = " + this.jarFile);
		this.numJuices = Integer.parseInt(options.get(3));
		//System.out.println("num juices = " + this.numJuices);
		this.prefix = options.get(4);
		//System.out.println("prefix = " + this.prefix);
		this.destFile = options.get(5);
		//System.out.println("dest file = " + this.destFile);
		
		//get a list of files to juice
		filesToJuice = this.searchDFSByPattern(this.prefix);
		//System.out.println(filesToJuice);
		
		String proposedProcess = this.fs.getGs().getSuccessor();
		int iter = 0;
		while(juiceCompletionStatus.size() < this.numJuices || iter >100) {
			if(!juiceCompletionStatus.containsKey(proposedProcess) && !proposedProcess.equals(this.fs.getGs().getPriority())) {
				juiceCompletionStatus.putIfAbsent(proposedProcess, "Incomplete");
			}
			proposedProcess = this.fs.getGs().getMembershipList().getMember(proposedProcess).getSuccessor();
			iter++;
		}
		/*
		for(int i=0;i<filesToJuice.size();i++) {
			fileCompletionStatus.put(filesToJuice.get(i), "Incomplete");
		}
	
		//do this until all files are juiced
		while(fileCompletionStatus.values().contains("Incomplete")) {
			//for 1 through numJuices send fileToJuice to a juicer
			HashMap<String, DFSClientThread> juiceThreads = new HashMap<String, DFSClientThread>();
			List<String> allowedProcesses = new ArrayList<String>();
			//rest of the options are what we need to run maple on
			for(int i=0;i<filesToJuice.size();i++) {
				//only try to juice this file if it is not completed
				if(fileCompletionStatus.get(filesToJuice.get(i)).equals("Incomplete")) {
				//choose a juice worker and tell it to start
					//send the command to a random server
					byte[] command = this.formMJCommand("juice none", this.jarFile, filesToJuice.get(i), this.destFile);
					boolean fileComplete = false;
					
					//only allow certain processes
					String proposedProcess = this.fs.getGs().getSendToProcess(filesToJuice.get(i));
					
					while(!fileComplete) {
						//don't let the master get added
						if(proposedProcess.equals(this.fs.getGs().getProcessId())) {
							proposedProcess = this.fs.getGs().getSuccessor();
						}
						//add the new process
						if(allowedProcesses.size() < this.numJuices) {
							allowedProcesses.add(proposedProcess);
						}
						if(allowedProcesses.contains(proposedProcess)) {
							String ipAddress = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getSendToProcess(proposedProcess)).getIpAddress();
							int portNumber = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getSendToProcess(proposedProcess)).getFilePortNumber();
							//System.out.println("master sending maple command to worker" + portNumber);
							LoggerThread lt = new LoggerThread(this.fs.getGs().getProcessId(), "#MASTER_JUICE_TASK_STARTED#" + filesToJuice.get(i));
							lt.start();	 
							juiceThreads.put(filesToJuice.get(i), new DFSClientThread(ipAddress, portNumber, "juice none", command));
							juiceThreads.get(filesToJuice.get(i)).start();
							fileComplete = true;
						}
						proposedProcess = this.fs.getGs().getMembershipList().getMember(proposedProcess).getSuccessor();
						
						//delete dead processes from allowedProcesses
						for(int z=0;z<allowedProcesses.size();z++) {
							if(!this.fs.getGs().getMembershipList().hasKey(allowedProcesses.get(z))) {
								allowedProcesses.remove(z);
							}
							else if(this.fs.getGs().getMembershipList().getMember(allowedProcesses.get(z)).isDeletable()) {
								allowedProcesses.remove(z);
							}
						}
					}
					
					//if any processes have left remove them from the allowed process
				}
			}
			*/
			
			//go through list of files to juice
			//get num juices and divide the files up
			//LoggerThread lt = new LoggerThread(this.fs.getGs().getProcessId(), "#MASTER_GETTING_FILES_TO_JUICE#");
			//lt.start();	 
			ConcurrentHashMap<String, List<String>> juiceTasks = new ConcurrentHashMap<String, List<String>>();
			List<String> processesAllowed = new ArrayList<String>(juiceCompletionStatus.keySet());
			Random rando = new Random();
			for(int i=0;i<filesToJuice.size();i++) {
				proposedProcess = processesAllowed.get(rando.nextInt(processesAllowed.size()));
				if(juiceTasks.keySet().contains(proposedProcess)) {
					List<String> tempList = new ArrayList<String>(juiceTasks.get(proposedProcess));
					tempList = juiceTasks.get(proposedProcess);
					tempList.add(filesToJuice.get(i));
					juiceTasks.put(proposedProcess, tempList);
				}
				else {
					List<String> tempList = new ArrayList<String>();
					tempList.add(filesToJuice.get(i));
					juiceTasks.put(proposedProcess, tempList);
				}
				//System.out.println(juiceTasks.get(proposedProcess));
			}
			

			//keep going until all juices are complete
			//lt = new LoggerThread(this.fs.getGs().getProcessId(), "#MASTER_JUICE_TASK_STARTED#");
			//lt.start();	 
			int maxiter = 100;
			while(juiceCompletionStatus.containsValue("Incomplete")) {
				maxiter --;
				if(maxiter < 0) {
					result = "master failed".getBytes();
					return result;
				}
				List<String> juiceTheseFiles = new ArrayList<String>();
				HashMap<String, DFSClientThread> juiceThreads = new HashMap<String, DFSClientThread>();
				//merge a juice tasks key files
				for(String process:juiceCompletionStatus.keySet()) {
					if(juiceCompletionStatus.get(process).equals("Incomplete")) {
						//System.out.println(juiceTasks.get(process).toString());
						this.mergeIntermediateFiles(juiceTasks.get(process), "JUICEME_" + process);
						juiceTheseFiles.add("JUICEME_" + process);
					}
				}
				//put the new file on dfs
				this.putFilesOnDFSByList(juiceTheseFiles);
				
				//send juice request
				for(int j=0;j<processesAllowed.size();j++) {
					String ipAddress = this.fs.getGs().getMembershipList().getMember(processesAllowed.get(j)).getIpAddress();
					int portNumber = this.fs.getGs().getMembershipList().getMember(processesAllowed.get(j)).getFilePortNumber();
					//System.out.println("master sending maple command to worker" + portNumber);
					LoggerThread lt = new LoggerThread(this.fs.getGs().getProcessId(), "#WORKER_JUICE_TASK_SENT#" + processesAllowed.get(j));
					lt.start();	 
					byte[] command = this.formMJCommand("juice none", this.jarFile, juiceTheseFiles.get(j), this.destFile);
					juiceThreads.put(processesAllowed.get(j), new DFSClientThread(ipAddress, portNumber, "juice none", command));
					juiceThreads.get(processesAllowed.get(j)).start();
				}

			    //wait for all threads to return
			    for(String s:juiceThreads.keySet()){
			    	try {
			    		juiceThreads.get(s).join();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
			    }
			    
			    //get completed file names
			    //System.out.println("here" + mapleThreads.keySet());
			    for(String key: juiceThreads.keySet()){
				    String filesOnDFS = "";
			    	//System.out.println(mapleThreads.get(key).getResponse());
			    	//if the response does not start with ERROR: then 
			    	if(juiceThreads.get(key).getResponse().contains("ERROR")) {
			    		//do nothing so it can redo the maple
			    	}
			    	else {
			    		juiceCompletionStatus.put(key, "Completed");
			    		filesOnDFS += " " + juiceThreads.get(key).getResponse();		    
					    //reuse parse command options and add finished file names to array
			    		for(String s:parseCommandOptions(filesOnDFS)) {
			    			if(!s.equals("")) {
			    				listOfFinishedFiles.add(s);
			    				//System.out.println(s);
			    			}
			    		}
			    	}
			    }
			    
			   
			    //loop through process allowed and make sure they are not dead
			    /*
			    for(String p:juiceCompletionStatus.keySet()) {
			    	if(juiceCompletionStatus.get(p).equals("Completed")) {
			    		juiceCompletionStatus.remove(p);
			    	}
			    }
			    
			    
			    //if the process fails give it to someone else
			    for(String p:processesAllowed) {
			    	if(this.fs.getGs().getMembershipList().getMember(p).isDeletable()) {
			    		processesAllowed.remove(p);
			    		//assign those juicetasks to someone else
						List<String> tempList = new ArrayList<String>(juiceTasks.get(p));
			    		juiceTasks.remove(p);
			    		for(String s:processesAllowed) {
			    			if(juiceCompletionStatus.get(s).equals("Complete")) {
			    				juiceTasks.put(s, tempList);
			    				juiceCompletionStatus.put(s, "Incomplete");
			    			}
			    			else {
			    				//assign to master
			    				juiceTasks.put(this.fs.getGs().getProcessId(), tempList);
			    				juiceCompletionStatus.put(this.fs.getGs().getProcessId(), "Incomplete");
			    			}
			    		}
			    	}
			    }
			    */
					
			}
			

			/*
			//wait for processes to finish and see if they finished correctly
		    //wait for all threads to return
		    for(String s:juiceThreads.keySet()){
		    	try {
		    		juiceThreads.get(s).join();
					LoggerThread lt = new LoggerThread(this.fs.getGs().getProcessId(), "#MASTER_JUICE_TASK_FINISHED#" + s);
					lt.start();	 
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		    }
		    
		    //get completed file names
		    //System.out.println("here" + mapleThreads.keySet());
		    for(String key: juiceThreads.keySet()){
			    String filesOnDFS = "";
		    	//System.out.println(mapleThreads.get(key).getResponse());
		    	//if the response does not start with ERROR: then 
		    	if(juiceThreads.get(key).getResponse().contains("ERROR")) {
		    		//do nothing so it can redo the maple
		    	}
		    	else {
		    		fileCompletionStatus.put(key, "Completed");
		    		filesOnDFS += " " + juiceThreads.get(key).getResponse();		    
				    //reuse parse command options and add finished file names to array
		    		for(String s:parseCommandOptions(filesOnDFS)) {
		    			if(!s.equals("")) {
		    				listOfFinishedFiles.add(s);
		    				//System.out.println(s);
		    			}
		    		}
		    	}
		    }
			
		}
		*/
		
		//stitch the files back together in Key order
		this.mergeIntermediateFiles(listOfFinishedFiles, this.destFile);
		
		List<String> destFiles = new ArrayList<String>();
		destFiles.add(this.destFile);
		//put them on the dfs 
		this.putFilesOnDFSByList(destFiles);

		//lt = new LoggerThread(this.fs.getGs().getProcessId(), "#MASTER_JUICE_TASK_FINISHED#");
		//lt.start();	 
		
	    //delete the JUICOMPLETE_ files from the dfs
	    //we have the data we need		
		//clean up local stuff
		this.cleanUpLocal();
		
		result = "Juice Master Complete".getBytes();
		return result;
		
	}
	
	
	//used by juicer to get all the files with a certain pattern
	private List<String> searchDFSByPattern(String searchPattern) {
		//get a list of files matching the searchPattern from each FSServer
		//new FSServer command, 
		byte[] command = this.formFileCommand("find", searchPattern, true, "".getBytes());
		String ipAddress = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getProcessId()).getIpAddress();
		int portNumber = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getProcessId()).getFilePortNumber();
		DFSClientThread dct = new DFSClientThread(ipAddress, portNumber, "find none", command);
		dct.start();
		//System.out.println(ipAddress);
		//System.out.println(portNumber);
		//System.out.println(command.toString());
		try {
			dct.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		String keys = dct.getResponse().replace("[", "").replace("]", "").replace(",", "");
		//System.out.println(keys);
		return parseCommandOptions(keys);
	}

	//after the mapper files run this will group all the files by key
	private List<String> groupFilesByKey(List<String> filesToGroup) {
		List<String> result = new ArrayList<String>();
		List<String> keys = new ArrayList<String>();
		 //get keys from completed files
		for(int i=0;i<filesToGroup.size();i++) {
			//System.out.println(filesToGroup.get(i));
			try {
				if(!keys.contains(filesToGroup.get(i).split("_DELIM_")[3])) {
					keys.add(filesToGroup.get(i).split("_DELIM_")[3]);
				}
			} catch(ArrayIndexOutOfBoundsException e) {
				
			}
		}
		
		//System.out.println("Merging " + filesToInclude.size() + " many files.");
		
		//loop through each key and merge files
		for(String s:keys) {
			List<String> filesToMerge = new ArrayList<String>();
			for(int i=0;i<filesToGroup.size();i++) {
				//if it has the key in the file name then merge it
				if(filesToGroup.get(i).endsWith("_DELIM_" + this.prefix + "_DELIM_" + s)) {
					filesToMerge.add(filesToGroup.get(i));
				}
			}
			//now send the files over for merging
			this.mergeIntermediateFiles(filesToMerge, this.prefix + "_" + s);
			result.add(this.prefix + "_" + s);
		}
		return result;
	}

	//take the files the mappers gave you and print out a new list of files
	//broken down by key
	private List<String> splitFiles(List<String> listOfFinishedFiles) {
		List<String> keyFiles = new ArrayList<String>();
		ConcurrentHashMap<String, PrintWriter> keys = new ConcurrentHashMap<String, PrintWriter>();
		this.getFilesFromDFSByList(listOfFinishedFiles);
		
		try {
			//go through each file
			for(int i=0;i<listOfFinishedFiles.size();i++) {
				FileReader fileReader = new FileReader(listOfFinishedFiles.get(i));
				BufferedReader br = new BufferedReader(fileReader);
				String line = "";
				while ((line = br.readLine()) != null) {
				    String key = line.split(",")[0].replace("(", "").trim();
					if(!keys.contains(key)) {
						keys.putIfAbsent(key, new PrintWriter(this.prefix + "_" + key));
						keyFiles.add(this.prefix + "_" + key);
					}
					keys.get(key).println(line);
					//System.out.println(line);
				}
				br.close();		
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(String s:keys.keySet()) {
			keys.get(s).close();
		}
		
		
		return keyFiles;
	}

	//merges intermediate files after mapper is done and after juicer
	private void mergeIntermediateFiles(List<String> filesToMerge, String newFilename) {
		//System.out.println("here " + newFilename);
		//get the files from the dfs first
		this.getFilesFromDFSByList(filesToMerge);
		
		List<String> lines = new ArrayList<String>();
		for(int i=0;i<filesToMerge.size();i++) {	
			try {
				//read from the files
				FileReader fileReader = new FileReader(filesToMerge.get(i));
				BufferedReader br = new BufferedReader(fileReader);
				String line = "";
				while ((line = br.readLine()) != null) {
				    lines.add(line);
				    //System.out.println(line);
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
			FileWriter fileWriter = new FileWriter(newFilename);
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
	
	//parse the command options
	private List<String> parseCommandOptions(String commandOptions) {
		//System.out.println(commandOptions);
		//String str = commandOptions.replace("[", "").replace("]","").replace(",","");
		return Arrays.asList(commandOptions.split("\\s+"));
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
	
	/*
	private void deleteFromDFS(String s) {
		byte[] command = this.formFileCommand("del", s, true, "".getBytes());
		String ipAddress = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getProcessId()).getIpAddress();
		int portNumber = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getProcessId()).getFilePortNumber();
		DFSClientThread dct = new DFSClientThread(ipAddress, portNumber, "del none", command);
		dct.start();
    	try {
    		dct.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	*/
	
	//gets the files from the DFS
	private void getFilesFromDFSByList(List<String> filesToMerge) {
		ExecutorService ex = Executors.newFixedThreadPool(20);
		//List<DFSClientThread> threads = new ArrayList<DFSClientThread>();
		for(int i=0;i<filesToMerge.size();i++) {
			String s = filesToMerge.get(i);
			//System.out.println(s);
			byte[] command = this.formFileCommand("getserv", s, true, "".getBytes());
			String ipAddress = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getProcessId()).getIpAddress();
			int portNumber = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getProcessId()).getFilePortNumber();
			ex.execute(new DFSClientThread(ipAddress, portNumber, "getserv " + s, command));
			//threads.add(new DFSClientThread(ipAddress, portNumber, "getserv " + s, command));
			//threads.get(i).start();
		}
		
		/*
		//wait for threads to come back
	    for(int i=0;i<threads.size();i++){
	    	try {
	    		threads.get(i).join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	    }
	    */
		ex.shutdown();
		while(!ex.isTerminated()) {
			
		}
	}
	
	//gets the files from the DFS
	private void putFilesOnDFSByList(List<String> filesToPut) {
		ExecutorService ex = Executors.newFixedThreadPool(20);
		//List<DFSClientThread> threads = new ArrayList<DFSClientThread>();
		for(int i=0;i<filesToPut.size();i++) {
			try {
				String s = filesToPut.get(i);
				Path path = Paths.get(s);
				byte[] data = Files.readAllBytes(path);
				//System.out.println(s);
				byte[] command = this.formFileCommand("put", s, true, data);
				String ipAddress = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getProcessId()).getIpAddress();
				int portNumber = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getProcessId()).getFilePortNumber();
				//threads.add(new DFSClientThread(ipAddress, portNumber, "put none", command));
				//threads.get(i).start();
				ex.execute(new DFSClientThread(ipAddress, portNumber, "put none", command));
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}
		/*
		//wait for threads to come back
	    for(int i=0;i<threads.size();i++){
	    	try {
	    		threads.get(i).join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	    }
	    */
		ex.shutdown();
		while(!ex.isTerminated()) {
			
		}
	}
	
	private void cleanUpLocal() {
		//go through local files and delete everything that 
		//is like MAPCOMPLETE_ or JUICOMPLETE_
		File folder = new File(".");
		File[] listOfFiles = folder.listFiles();
		
		for(int i=0;i<listOfFiles.length;i++) {
			if(listOfFiles[i].isFile() && 
					(listOfFiles[i].toString().contains("MAPCOMPLETE_") || 
					listOfFiles[i].toString().contains("JUICOMPLETE_") ||
					listOfFiles[i].toString().startsWith("JUICEME_"))) {
				listOfFiles[i].delete();
			}
		}
		
	}
	
}
