package com.kleck.MapleJuice;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MasterMapleJuiceServerProtocol {
	private FSServer fs;
	byte[] header = null;
	byte[] filedata = null;
	private String command;
	private String commandOptions;
	
	
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
		else if(command.trim().equals("maple")) {
			//get command options and send to master maple processor
			if(this.fs.getGs().getMembershipList().getMaster().equals(this.fs.getGs().getProcessId())) {
				this.commandOptions = new String(Arrays.copyOfRange(data, 0, data.length)).trim();
				//System.out.println("Im here");
				result = this.processMapleMaster();
			}
			else {
				result = "Not the master".getBytes();
			}
		}
		else if(command.trim().equals("juice")) {
			//get command options and send to master juice processor
			this.commandOptions = new String(Arrays.copyOfRange(data, 0, data.length)).trim();	
			result = this.processJuiceMaster();
		}
		
		
		return result;
	}

	private byte[] processMapleMaster() {
		byte[] result = "".getBytes();
		List<String> options = this.parseCommandOptions(this.commandOptions);
		List<DFSClientThread> mapleThreads = new ArrayList<DFSClientThread>();
		
		//System.out.println("Options size" + options.size());
		//first option is the jar file
		String jarFile = options.get(1);
		
		//2nd option is the file prefix
		String prefix = options.get(2);
		
		//rest of the options are what we need to run maple on
		for(int i=3;i<options.size();i++) {
			//choose a maple worker and tell it to start

			//send the command to a random server
			byte[] command = this.formMJCommand("maple", jarFile, options.get(i), prefix);
			String ipAddress = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getSendToProcess(options.get(i))).getIpAddress();
			int portNumber = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getSendToProcess(options.get(i))).getPortNumber();
			
			mapleThreads.add(new DFSClientThread(ipAddress, portNumber, "maple none", command));
			mapleThreads.get(i-3).start();
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
	    
	    //see if there were any maple worker failures
	    result = "Maple Master Complete".getBytes();
		return result;
		
	}

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
		filenameByte = Arrays.copyOf(interFile.getBytes(), 200);
		
		//System.out.println(file.length);
		System.arraycopy(com, 0, result, 0, 16);
		System.arraycopy(jarFileByte, 0, result, 16, 96);
		System.arraycopy(filenameByte, 0, result, 112, 200);
		System.arraycopy(interFileByte, 0, result, 312, 200);
		return result;
	}
	
}
