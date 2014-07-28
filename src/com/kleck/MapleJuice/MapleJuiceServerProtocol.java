package com.kleck.MapleJuice;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MapleJuiceServerProtocol {
	private FSServer fs;
	byte[] header = null;
	byte[] filedata = null;
	private String command;
	private String jarFile;
	private String filename;
	private String interFile;

	public byte[] processInput(byte[] data, FSServer fs) {
		this.fs = fs;
		byte[] result = "".getBytes();
		//this.filedata = Arrays.copyOfRange(data, 64, data.length);
		
		//need to get the first x bytes of data for the command
		this.command = new String(Arrays.copyOfRange(data, 0, 16)).trim();

		
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
			//if you are not the master just do what the master says
			//System.out.println("maple worker received");
			this.header = Arrays.copyOfRange(data, 0, 512);
			this.jarFile = new String(Arrays.copyOfRange(header, 16, 112)).trim();
			this.filename = new String(Arrays.copyOfRange(header, 112, 312)).trim();
			this.interFile = new String(Arrays.copyOfRange(header, 312, 512)).trim();
			//System.out.println(jarFile + filename + interFile);
			result = this.processMaple();
		}
		else if(command.trim().equals("juice none")) {
			//if you are not the master just do what the master says
			this.header = Arrays.copyOfRange(data, 0, 512);
			this.jarFile = new String(Arrays.copyOfRange(header, 16, 112)).trim();
			this.filename = new String(Arrays.copyOfRange(header, 112, 312)).trim();
			this.interFile = new String(Arrays.copyOfRange(header, 312, 512)).trim();
			result = this.processJuice();
		}
		
		return result;
	}
	
	//mapler
	public byte[] processMaple() {
		byte[] result = "".getBytes();
		boolean processingError = false;
		//System.out.println("Maple Worker started");
		//byte[] result = "".getBytes();
		//1 maple task per filename
		//need to get the files from the master if it doesn't exist in order to do the maple
		this.getFileFromMaster(this.jarFile);
		//get the file we will maple on
		this.getFileFromMaster(this.filename);
		//System.out.println("Maple exec = " + this.jarFile);
		//System.out.println("Filename to maple = " + this.filename);
		//System.out.println("intermediate file prefix = " + this.interFile);
		//got the files now execute the maple
		ConcurrentMap<String, PrintWriter> pw = new ConcurrentHashMap<String, PrintWriter>();
		List<String> filenames = new ArrayList<String>();  
		try {
			LoggerThread lt = new LoggerThread(this.fs.getGs().getProcessId(), "#WORKER_MAP_FILE#" + this.filename);
			lt.start();	 
			//Process proc=Runtime.getRuntime().exec(new String[]{"java","-jar","Maple.jar","wordcount.txt"});
			Process proc = Runtime.getRuntime().exec("java -jar " + this.jarFile + " " + this.filename);
			//proc.waitFor();
			BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			BufferedReader err = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
			String s;
			
			//whiler there is output write it to the appropriate keyfile
			while ((s = in.readLine()) != null) {
				//spin up a new Print Writer if necessary
				String key = s.split(",")[0].replace("(", "").trim();
				if(!pw.containsKey(key)) {
					String newFile = this.filename + "_DELIM_" + this.interFile + "_DELIM_" + key;
					pw.putIfAbsent(key, new PrintWriter(newFile, "UTF-8"));
					filenames.add(newFile);
				}
				pw.get(key).println(s);
            }
            while ((s = err.readLine()) != null) {
                System.out.println(s);
                result = "ERROR: JARfailed".getBytes();
                processingError = true;
                //System.out.println("***jar file did not work***");
            }
		} catch (IOException e) {
			result = "ERROR: IOException".getBytes();
			processingError = true;
			e.printStackTrace();
		}
		
		//we are done so rename files accordingly so everyone knows this is done
		//close print writers
		for(String key: pw.keySet()) {
			pw.get(key).close();
		}
		
		if(!processingError) {
			//put them on the dfs
			for(int i=0;i<filenames.size();i++) {
				File copyMe = new File(filenames.get(i));
				File target = new File("MAPCOMPLETE_" + "_DELIM_" + filenames.get(i));
				copyMe.renameTo(target);
				result = this.concatenateByte(result, (" MAPCOMPLETE_" + "_DELIM_" + filenames.get(i)).getBytes());
				//put the files onto the dfs
				this.putFileOnDFS("MAPCOMPLETE_" + "_DELIM_" + filenames.get(i));
				//done now delete the files
				copyMe.delete();
				target.delete();
			}
		}
		
		return result;
	}
	
	
	//juicer
	public byte[] processJuice() {
		String result = "";
		//get the files we need
		this.getFileFromMaster(this.jarFile);
		this.getFileFromMaster(this.filename);
		PrintWriter pw = null;
		String newFile = "";
		//got the files now execute the juice
		try {
			newFile = this.filename + "_DELIM_" + this.interFile;
			pw = new PrintWriter(newFile);
			//System.out.println("java -jar " + this.jarFile + " " + this.filename);
			LoggerThread lt = new LoggerThread(this.fs.getGs().getProcessId(), "#WORKER_JUICE_FILE#" + newFile);
			lt.start();	 
			Process proc = Runtime.getRuntime().exec("java -jar " + this.jarFile + " " + this.filename);
			BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			BufferedReader err = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
			String s;
			
			//while there is output write it to the output file
			while ((s = in.readLine()) != null) {
				pw.println(s);
            }
            while ((s = err.readLine()) != null) {
                System.out.println(s);
            }
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		pw.close();
		File copyMe = new File(newFile);
		File target = new File("JUICOMPLETE_" + "_DELIM_" + newFile);
		copyMe.renameTo(target);
		this.putFileOnDFS(target.toString());
		result += " " + target.toString();
		
		//result = "Juice Complete".getBytes();
		return result.getBytes();
		
	}
	
	//retrieve the file needed directly from the master node
	private void getFileFromMaster(String filename) {
		if(!this.checkLocalFilename(filename)) {
			byte[] command = FileServerProtocol.formCommand("get", filename, true, "".getBytes());
			String ipAddress = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getMembershipList().getMaster()).getIpAddress();
			int portNumber = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getMembershipList().getMaster()).getFilePortNumber();
			//save the file
			try {
				FileOutputStream fos = new FileOutputStream(filename);
				fos.write(this.getServerResponse(ipAddress, portNumber, command));
				fos.close();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	//get the file that is passed back
	private byte[] getServerResponse(String ipAddress, int portNumber, byte[] data) throws UnknownHostException, IOException {
		Socket dlSocket = new Socket(ipAddress, portNumber);
		OutputStream out = dlSocket.getOutputStream();
		DataOutputStream dos = new DataOutputStream(out);
		dos.writeInt(data.length);
		dos.write(data);
		dos.flush();
		
		InputStream in = dlSocket.getInputStream();
		DataInputStream dis = new DataInputStream(in);
		int len = dis.readInt();
		byte[] result = new byte[len];
		if (len > 0) {
		    dis.readFully(result);
		}
		//System.out.println(new String(data));
		dlSocket.close();
		return result;
	}
	
	//see if file is valid
	private boolean checkLocalFilename(String filePath) {
		File f = new File(filePath);
		boolean result = f.exists() && !f.isDirectory();
		return result;
	}
	
	//put the file directly on the dfs
	private byte[] putFileOnDFS(String filename) {
		byte[] result = "".getBytes();
		Path path = Paths.get(filename);
		byte[] data = null;
		try {
			data = Files.readAllBytes(path);
			byte[] command = FileServerProtocol.formCommand("put", filename, true, data);
			String ipAddress = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getMembershipList().getMaster()).getIpAddress();
			int portNumber = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getMembershipList().getMaster()).getFilePortNumber();
			result = getServerResponse(ipAddress, portNumber, command);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	//concat byte arrays
	public byte[] concatenateByte (byte[] a, byte[] b) {
		byte[] result;
		if(a == null) {
			result = new byte[b.length];
			System.arraycopy(b, 0, result, 0, b.length);
		}
		else {
			result = new byte[a.length + b.length];
			System.arraycopy(a, 0, result, 0, a.length);
			System.arraycopy(b, 0, result, a.length, b.length);
		}
		return result;
	}
	
	/*
	//not needed
	//put the file directly on the master node
	private byte[] putFileOnMaster(String filename) {
		byte[] result = "".getBytes();
		Path path = Paths.get(filename);
		byte[] data = "".getBytes();
		try {
			data = Files.readAllBytes(path);
			byte[] command = FileServerProtocol.formCommand("put", filename, false, data);
			String ipAddress = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getMembershipList().getMaster()).getIpAddress();
			int portNumber = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getMembershipList().getMaster()).getFilePortNumber();
			result = getServerResponse(ipAddress, portNumber, command);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
	*/
}
