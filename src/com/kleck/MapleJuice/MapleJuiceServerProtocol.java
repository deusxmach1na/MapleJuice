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
		byte[] result = null;
		//this.filedata = Arrays.copyOfRange(data, 64, data.length);
		
		//need to get the first x bytes of data for the command
		this.command = new String(Arrays.copyOfRange(data, 0, 16)).trim();

		
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
			this.header = Arrays.copyOfRange(data, 0, 112);
			this.jarFile = new String(Arrays.copyOfRange(header, 16, 48)).trim();
			this.filename = new String(Arrays.copyOfRange(header, 48, 80)).trim();
			this.interFile = new String(Arrays.copyOfRange(header, 80, 112)).trim();
			result = this.processMaple();
		}
		else if(command.trim().equals("juice")) {
			this.header = Arrays.copyOfRange(data, 0, 112);
			this.jarFile = new String(Arrays.copyOfRange(header, 16, 48)).trim();
			this.filename = new String(Arrays.copyOfRange(header, 48, 80)).trim();
			this.interFile = new String(Arrays.copyOfRange(header, 80, 112)).trim();
			result = this.processJuice();
		}
		
		return result;
	}
	
	//mapler
	public byte[] processMaple() {
		//byte[] result = "".getBytes();
		//1 maple task per filename
		//need to get the files from the master if it doesn't exist in order to do the maple
		this.getFileFromMaster(this.jarFile);
		//get the file we will maple on
		this.getFileFromMaster(this.filename);

		//got the files now execute the maple
		ConcurrentMap<String, PrintWriter> pw = new ConcurrentHashMap<String, PrintWriter>();
		List<String> filenames = new ArrayList<String>();   //is this needed?
		try {
			Process proc = Runtime.getRuntime().exec("java -jar " + this.jarFile);
			BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			BufferedReader err = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
			String s;
			
			//whiler there is output write it to the appropriate keyfile
			while ((s = in.readLine()) != null) {
				//spin up a new Print Writer if necessary
				String key = s.split(",")[0].replace("(", "").trim();
				if(!pw.containsKey(key)) {
					pw.putIfAbsent(key, new PrintWriter(this.filename + this.interFile + "_" + key, "UTF-8"));
					filenames.add(this.filename + this.interFile + "_" + key);
				}
				pw.get(key).println(s);
            }
            while ((s = err.readLine()) != null) {
                System.out.println(s);
            }
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(String key: pw.keySet()) {
			pw.get(key).close();
		}
		
		return "Maple Complete".getBytes();
	}
	
	
	//juicer
	public byte[] processJuice() {
		//get the files we need
		this.getFileFromMaster(this.jarFile);
		this.getFileFromMaster(this.filename);
		PrintWriter pw = null;
		//got the files now execute the juice
		try {
			pw = new PrintWriter(this.interFile);
			Process proc = Runtime.getRuntime().exec("java -jar " + this.jarFile);
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
		
		return "Juice Complete".getBytes();
		
	}
	
	private void getFileFromMaster(String filename) {
		if(!this.checkLocalFilename(filename)) {
			byte[] command = FileServerProtocol.formCommand("get", filename, true, "".getBytes());
			String ipAddress = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getMembershipList().getMaster()).getIpAddress();
			int portNumber = this.fs.getGs().getMembershipList().getMember(this.fs.getGs().getMembershipList().getMaster()).getFilePortNumber();
			//save the file
			try {
				FileOutputStream fos = new FileOutputStream(this.jarFile);
				fos.write(this.getServerResponse(ipAddress, portNumber, command));
				fos.close();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
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
}
