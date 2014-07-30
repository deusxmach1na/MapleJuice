package com.kleck.MapleJuice;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;

public class DFSClientThread extends Thread {
	private String ipAddress;
	private int portNumber;
	private String commandType;
	private byte[] data;
	private String fileToSaveAs;
	private String serverResponse;
	
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
			if(this.commandType.equals("put") || this.commandType.equals("del") 
					|| this.commandType.equals("reb") || this.commandType.equals("maple")
					|| this.commandType.equals("find") || this.commandType.equals("juice")) {
				//System.out.println("issuing " + commandType + " to server");
				this.setServerResponse(getServerResponse());
			}
			else if(this.commandType.equals("get")) {
				//save the file
				FileOutputStream fos = new FileOutputStream("FromDFS/" + this.fileToSaveAs);
				fos.write(this.getServerResponse());
				fos.close();
			}
			else if(this.commandType.equals("getserv")) {
				//save the file
				//System.out.println(this.fileToSaveAs);
				FileOutputStream fos = new FileOutputStream(this.fileToSaveAs);
				//System.out.println("server says" + this.getServerResponse());
				fos.write(this.getServerResponse());
				fos.close();
			}
		} catch (ConnectException e) {
			this.setServerResponse("ConnectionException".getBytes());
			//issue connecting to server assume it failed
			//e.printStackTrace();	
			Thread.currentThread().interrupt();
			return;	
		} catch (EOFException e) {
			this.setServerResponse("EOFException".getBytes());
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
		//System.out.println(this.commandType);
		Socket dlSocket = new Socket(this.ipAddress, this.portNumber);
		BufferedOutputStream out = new BufferedOutputStream(dlSocket.getOutputStream());
		DataOutputStream dos = new DataOutputStream(out);
		dos.writeInt(this.data.length);
		dos.write(this.data);
		dos.flush();
		
		BufferedInputStream in = new BufferedInputStream(dlSocket.getInputStream());
		DataInputStream dis = new DataInputStream(in);
		int len = dis.readInt();
		byte[] result = new byte[len];
		if (len > 0) {
		    dis.readFully(result);
		}
		//System.out.println(new String(data));
		out.close();
		dos.close();
		in.close();
		dis.close();
		dlSocket.close();
		return result;
	}
	
	//set the server response for the thread
	//used in maple tasks to get completed file names
	private void setServerResponse(byte[] input) {
		this.serverResponse = new String(Arrays.copyOfRange(input, 0, input.length)).trim();
	}
	
	public String getResponse() {
		return this.serverResponse;
	}
}
