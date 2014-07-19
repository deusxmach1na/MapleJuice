package com.kleck.MapleJuice;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileServerProtocol {
	private FSServer fs;
	byte[] header = null;
	byte[] filedata = null;
	private String command;
	private boolean isFirst;
	private String filename;

	public byte[] processInput(byte[] data, FSServer fs) {
		this.fs = fs;
		byte[] result = null;
		this.header = Arrays.copyOfRange(data, 0, 64);
		this.filedata = Arrays.copyOfRange(data, 64, data.length);
		
		//need to get the first x bytes of data for the command
		this.command = new String(Arrays.copyOfRange(header, 0, 16)).trim();
		this.isFirst = new Boolean(new String(Arrays.copyOfRange(header, 16, 32)).trim());
		this.filename = new String(Arrays.copyOfRange(header, 32, 64)).trim();
		
		//System.out.println(command);
		//System.out.println(isFirst);
		//System.out.println(filename);
		if(command.trim().equals("put")) {
			result = this.put(filename, filedata, isFirst);
		}
		else if(command.trim().equals("get")) {
			result = this.get(filename, isFirst);	
		}
		else if(command.trim().equals("del")) {
			result = this.delete(filename, isFirst);
		}
		else if(command.trim().equals("reb")) {
			result = this.rebalance();
		}
		
		return result;
	}
	

	//this needs to take a file and shard it then push it to other servers
	//so it is duplicated the number of times indicated in replicationfactor setting
	private byte[] put(String filename, byte[] file, boolean isFirstRun) {
		//System.out.println("put entrance");
		this.fs.getGs().getMembershipList().setMaster();
		//if you are the master you need to shard the file and send a call to the
		//other servers to save the file
		this.fs.getGs().getMembershipList().setSuccessors();
		
		if(this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getProcessId()).isMaster() && isFirstRun) {
			int size = Integer.parseInt(this.fs.getGs().getProps().getProperty("shardsize"));
			List<byte[]> files = FileServerProtocol.shardFile(file, size);
			System.out.println("Sharded into " + files.size() + " files.");
			int replicationFactor = Integer.parseInt(this.fs.getGs().getProps().getProperty("replicationfactor"));
			//append a digit to the filename then save 1 copy on the correct node
			//then store another copy on the each nodes successor
			for(int i=0;i<files.size();i++) {
				String newFile = filename + "PART_" + i;
				String sentToProcess = this.fs.getGs().getSendToProcess(newFile);
				
				//stores a copy on sentToProcess then
				//store more on each successor
				for(int j=0;j<replicationFactor;j++) {
					String hostname = this.fs.getGs().getMembershipList().getMember(sentToProcess).getIpAddress();
					int portNumber = this.fs.getGs().getMembershipList().getMember(sentToProcess).getFilePortNumber();
					//start a new socket and send the command
					Socket dlSocket;
					try {
						//System.out.println(hostname);
						//System.out.println(portNumber);
						dlSocket = new Socket(hostname, portNumber);
						byte[] command = FileServerProtocol.formCommand("put", newFile, false, files.get(i));
						OutputStream out = dlSocket.getOutputStream();
						DataOutputStream dos = new DataOutputStream(out);
						//this.fs.getGs().updateBytesUsed(command.length);
						dos.writeInt(command.length);
						dos.write(command);
						dlSocket.close();
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} 
					sentToProcess = this.fs.getGs().getMembershipList().getMember(sentToProcess).getSuccessor();
				}
			}
		}
		if (!isFirstRun) {
			//System.out.println("saving file");
			//the master is telling you to save a file shard
			FileOutputStream fos;
			try {
				fos = new FileOutputStream(filename);
				fos.write(file);
				LoggerThread lt = new LoggerThread(this.fs.getGs().getProcessId(), "#PUT_FILE#" + filename);
				lt.start();	 
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		//System.out.println("put finished");
		return "put finished".getBytes();
	}
	
	
	public byte[] get(String filename, boolean isFirstRun) {
		//System.out.println("entered get");
		byte[] result = null;
		//if you are the master you need to find all the file shards
		//and re-assemble them
		this.fs.getGs().getMembershipList().setMaster();
		this.fs.getGs().getMembershipList().setSuccessors();
		//System.out.println(this.gs.getMembershipList().getMember(this.gs.getProcessId()).isMaster());
		if(this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getProcessId()).isMaster() && isFirstRun) {
			byte[] fileToReturn = null;
			boolean isLastShard = false;
			int filesFound = 0;
			//don't know how many file fragments there are
			//loop through with i and increment by 1
			int i = 0;
			while(!isLastShard) {
				//calculate the hash of the filename and ask the appropriate
				//server if they have the filename, if not try the successor
				String fileToFind = filename + "PART_" + i;
				String potentialProcess = this.fs.getGs().getSendToProcess(fileToFind);
				
				//loop through all the potential processes so you can find the right file
				int replicationFactor = Integer.parseInt(this.fs.getGs().getProps().getProperty("replicationfactor"));
				for(int j=0;j<replicationFactor;j++) {
					//System.out.println(potentialProcess);
					String hostname = this.fs.getGs().getMembershipList().getMember(potentialProcess).getIpAddress();
					int portNumber = this.fs.getGs().getMembershipList().getMember(potentialProcess).getFilePortNumber();
					boolean exists = false;
					//start a new socket and send the file exists command
					Socket dlSocket;
					try {
						//System.out.println(hostname);
						//System.out.println(portNumber);
						dlSocket = new Socket(hostname, portNumber);
						byte[] command = FileServerProtocol.formCommand("get", fileToFind, false, "".getBytes());
						OutputStream out = dlSocket.getOutputStream();
						DataOutputStream dos = new DataOutputStream(out);
						//this.fs.getGs().updateBytesUsed(command.length);
						dos.writeInt(command.length);
						dos.write(command);
						
						//get input
						InputStream in = dlSocket.getInputStream();
						DataInputStream dis = new DataInputStream(in);
						int len = dis.readInt();
						this.fs.getGs().updateBytesUsed(len);
					    byte[] data = new byte[len];
					    //file found
					    if (len > 0) {
					        dis.readFully(data);
					        exists = true;
					    }
					    //file not found
					    else {
					    	exists = false;
					    }
						dlSocket.close();
						if(exists && filesFound == i) {
							//System.out.println("concatting files");
							filesFound++;
							fileToReturn = concatenateByte(fileToReturn, data);
							break;  //break the for loop we got our shard
						}
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (EOFException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} 
					potentialProcess = this.fs.getGs().getMembershipList().getMember(potentialProcess).getSuccessor();
				}
				//we should have found the file by now so if not 
				//it should be done
				if(filesFound == i) {
					isLastShard = true;  //breaks the while loop
				}
				i++;
			}
			
			//we now have all the pieces, sew them together
			result = fileToReturn;
			//System.out.println("get complete");
		}
		
		//if you are not the master just return the file you are asked to get
		if (!isFirstRun) {
			//System.out.println("getting file");
			File f = new File(this.filename);
			boolean isFound = f.exists() && !f.isDirectory();
			if(isFound) {
				try {
					Path path = Paths.get(filename);
					LoggerThread lt = new LoggerThread(this.fs.getGs().getProcessId(), "#GET_FILE#" + filename);
					lt.start();	 
					result = Files.readAllBytes(path);
					//System.out.println("output files");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			else {
				//return null
				//System.out.println("null here");
				result = "".getBytes();
			}
		}
		return result;
	}

	
	public byte[] delete(String filename, boolean isFirstRun) {
		//if you are the master you need to find all the file shards
		//and delete them
		boolean isLastShard = false;
		byte[] result = null;
		//System.out.println("entered delete");
		this.fs.getGs().getMembershipList().setMaster();
		this.fs.getGs().getMembershipList().setSuccessors();
		//System.out.println(this.gs.getMembershipList().getMember(this.gs.getProcessId()).isMaster());
		if(this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getProcessId()).isMaster() && isFirstRun) {
			//find all the files and delete them
			int i = 0;
			while(!isLastShard) {
				//calculate the hash of the filename and ask the appropriate
				//server if they have the filename, if not try the successor
				String fileToFind = filename + "PART_" + i;
				String potentialProcess = this.fs.getGs().getSendToProcess(fileToFind);
				boolean fileShardFound = false;
				
				//loop through all the potential processes so you can find the right file
				//int replicationFactor = Integer.parseInt(gs.getProps().getProperty("replicationfactor"));
				for(int j=0;j<this.fs.getGs().getMembershipList().getActiveKeys().size();j++) {
					//System.out.println(potentialProcess);
					String hostname = this.fs.getGs().getMembershipList().getMember(potentialProcess).getIpAddress();
					int portNumber = this.fs.getGs().getMembershipList().getMember(potentialProcess).getFilePortNumber();
					//start a new socket and send the file exists command
					Socket dlSocket;
					try {
						//System.out.println(hostname);
						//System.out.println(portNumber);
						dlSocket = new Socket(hostname, portNumber);
						byte[] command = FileServerProtocol.formCommand("del", fileToFind, false, "".getBytes());
						OutputStream out = dlSocket.getOutputStream();
						DataOutputStream dos = new DataOutputStream(out);
						//this.fs.getGs().updateBytesUsed(command.length);
						dos.writeInt(command.length);
						dos.write(command);
						
						//get input
						InputStream in = dlSocket.getInputStream();
						DataInputStream dis = new DataInputStream(in);
						int len = dis.readInt();
					    //file found
					    if (len > 0) {
					    	fileShardFound = true;
					    }
						dlSocket.close();
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (EOFException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} 
					potentialProcess = this.fs.getGs().getMembershipList().getMember(potentialProcess).getSuccessor();
				}
				//we should have found the file by now so if not 
				//it should be done
				if(!fileShardFound) {
					isLastShard = true;  //breaks the while loop
				}
				i++;
				result = "Delete Complete".getBytes();
			}
		}
		//if you are not the master just return the file you are asked to get
		if (!isFirstRun) {
			//System.out.println("deleting file");
			File f = new File(this.filename);
			boolean isFound = f.exists() && !f.isDirectory();
			if(isFound) {
				f.delete();
				LoggerThread lt = new LoggerThread(this.fs.getGs().getProcessId(), "#DELETE_FILE#" + filename);
				lt.start();	 
				result = "File Deleted".getBytes();
				//System.out.println("output files");
			}
			else {
				//return null
				//System.out.println("null here");
				result = "".getBytes();
			}
		}
		//System.out.println("hello from delete");
		return result;
	}

	public byte[] fileExists(String filename) {
		String result = "false";
		File f = new File(filename);
		if(f.exists() && !f.isDirectory()) {
			result = "true";
		}
		//System.out.println("hello from file exists");
		return result.getBytes();
	}
	
	//splits file into file.size()/size chunks
	public static List<byte[]> shardFile(byte[] file, int size) {
	    List<byte[]> result = new ArrayList<byte[]>();
	    int start = 0;
	    while (start < file.length) {
	        int end = Math.min(file.length, start + size);
	        result.add(Arrays.copyOfRange(file, start, end));
	        start += size;
	    }
	    return result;
	}

	
	public byte[] concatenateByte (byte[] a, byte[] b) {
		byte[] result;
		if(a == null) {
			result = new byte[b.length];
			// copy b to result
			System.arraycopy(b, 0, result, 0, b.length);
		}
		else {
			result = new byte[a.length + b.length];
			// copy a to result
			System.arraycopy(a, 0, result, 0, a.length);
			// copy b to result
			System.arraycopy(b, 0, result, a.length, b.length);
		}
		return result;
	}
	
	
	//turns the command into a byte array
	public static byte[] formCommand(String commandType, String filename, boolean b, byte[] data) {
		byte[] result = new byte[data.length + 64];
		byte[] com = new byte[16];
		com = Arrays.copyOf(commandType.getBytes(), 16);
		byte[] file = new byte[32];
		file = Arrays.copyOf(filename.getBytes(), 32);
		byte[] isFirst = new byte[16];
		if(b)
			isFirst = Arrays.copyOf("true".getBytes(), 16);
		else
			isFirst = Arrays.copyOf("false".getBytes(), 16);
			
		//System.out.println(file.length);
		System.arraycopy(com, 0, result, 0, 16);
		System.arraycopy(isFirst, 0, result, 16, 16);
		System.arraycopy(file, 0, result, 32, 32);
		System.arraycopy(data, 0, result, 64, data.length);
		//result = this.concatenateByte(com, file);
		//result = this.concatenateByte(result, isFirst);
		//result = this.concatenateByte(result, data);
		return result;
	}
	

	//FAILURE DETECTED
	//rebalance system
	public byte[] rebalance() {
		//pick a file
		//then make sure there are at least replicationFactor copies
		byte[] result = null;
		//long currentTime = System.currentTimeMillis();
		//Set<String> activeKeys = this.gs.getMembershipList().getActiveKeys();
		File folder = new File(".");
		File[] listOfFiles = folder.listFiles();
		int replicationFactor = Integer.parseInt(this.fs.getGs().getProps().getProperty("replicationfactor"));
		
		for(int i=0;i<listOfFiles.length;i++) {
			//get a list of all files
			//if it has PART_ in it then see how many times it's replicated
			if(listOfFiles[i].isFile() && listOfFiles[i].toString().contains("PART_")) {
				//turn it into a byte array
				Path path = Paths.get(listOfFiles[i].toString());
				byte[] file = null;
				try {
					file = Files.readAllBytes(path);
				} catch (IOException e) {
					System.out.println("could not turn file into bytes");
					e.printStackTrace();
				}
				String potentialProcess = this.fs.getGs().getSendToProcess(listOfFiles[i].toString());
				int replicationCount = 1;  //the current server has a copy
				int serverCount = 1;
				//see how many times the file is replicated
				for(int j=0;j<replicationFactor;j++) {
					serverCount++;
					if(!potentialProcess.equals(this.fs.getGs().getProcessId())) {
						//System.out.println("replication here");
						String hostname = this.fs.getGs().getMembershipList().getMember(potentialProcess).getIpAddress();
						int portNumber = this.fs.getGs().getMembershipList().getMember(potentialProcess).getFilePortNumber();
						//start a new socket and send a get
						//if it is empty then don't need to move the file
						Socket dlSocket;
						try {
							//System.out.println(hostname);
							//System.out.println(portNumber);
							dlSocket = new Socket(hostname, portNumber);
							byte[] command = FileServerProtocol.formCommand("get", listOfFiles[i].toString(), false, "".getBytes());
							OutputStream out = dlSocket.getOutputStream();
							DataOutputStream dos = new DataOutputStream(out);
							//this.fs.getGs().updateBytesUsed(command.length);
							dos.writeInt(command.length);
							dos.write(command);
							dos.flush();
	
							//get input
							InputStream in = dlSocket.getInputStream();
							DataInputStream dis = new DataInputStream(in);
							int len = dis.readInt();
							//System.out.println("rebalance here");
						    dis.close();
							dlSocket.close();
						    //file found
						    if (len > 0) {
								//System.out.println("rebalance no put");
						    	replicationCount ++;
						    }
						    else {
						    	//put the file
						    	//System.out.println("put files");
								//System.out.println("rebalance put");
								Socket dlSocket2 = new Socket(hostname, portNumber);
								OutputStream out2 = dlSocket2.getOutputStream();
								DataOutputStream dos2 = new DataOutputStream(out2);
								
								command = FileServerProtocol.formCommand("put", listOfFiles[i].toString(), false, file);
								//this.fs.getGs().updateBytesUsed(command.length);
								dos2.writeInt(command.length);
								dos2.write(command);
								dos2.flush();
								dlSocket2.close();
								replicationCount ++;
						    }
						} catch (UnknownHostException e) {
							e.printStackTrace();
						} catch (SocketException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						} 
					}
					if(replicationCount == replicationFactor || serverCount == this.fs.getGs().getMembershipList().getActiveKeys().size()) {
						break; //we've replicated enough times
					}
					potentialProcess = this.fs.getGs().getMembershipList().getMember(potentialProcess).getSuccessor();
				}
			}
		}
		result = "Rebalance Complete".getBytes();
		//long runTime = System.currentTimeMillis();
		//System.out.println("***********************");
		//System.out.println("** Replication took " + (runTime - currentTime) + " milliseconds.**");
		//System.out.println("***********************");
		return result;
	}	
}
