package com.kleck.MapleJuice;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;

public class DFSClientThread extends Thread {
	private String ipAddress;
	private int portNumber;
	private String commandType;
	private byte[] data;
	private String fileToSaveAs;
	
	
	public DFSClientThread(String ipAddress, int portNumber, String command, byte[] data) {
		this.ipAddress = ipAddress;
		this.portNumber = portNumber;
		this.commandType = command.split(" ")[0];
		this.fileToSaveAs = command.split(" ")[1];
		this.data = data;
	}

	public void run() {		
		
		//open a socket to the server and send data
		try {
			//get the user command (put, get, delete)		
			if(this.commandType.equals("put") || this.commandType.equals("del") || this.commandType.equals("reb")) {
				//System.out.println("issuing put to server");
				getServerResponse();
			}
			else if(this.commandType.equals("get")) {
				//save the file
				FileOutputStream fos = new FileOutputStream("FromDFS/" + this.fileToSaveAs);
				fos.write(this.getServerResponse());
				fos.close();
			}
		} catch (ConnectException e) {
			//issue connecting to server assume it failed
			//e.printStackTrace();	
			Thread.currentThread().interrupt();
			return;	
		} catch (EOFException e) {
			//issue connecting to server assume it failed
			//e.printStackTrace();	
			//if(this.commandType.equals("get")) {
			//	System.out.println("DFS file not found.");
			//}
			Thread.currentThread().interrupt();
			return;	
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//opens a socket to the server and sends the command 
	private byte[] getServerResponse() throws UnknownHostException, IOException {
		Socket dlSocket = new Socket(this.ipAddress, this.portNumber);
		OutputStream out = dlSocket.getOutputStream();
		DataOutputStream dos = new DataOutputStream(out);
		dos.writeInt(this.data.length);
		dos.write(this.data);
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
}
