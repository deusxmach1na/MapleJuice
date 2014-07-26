package com.kleck.MapleJuice;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;



public class FSServerThread extends Thread {
	//initialize socket and in and out buffers
	private Socket clientSocket = null;
	private FSServer fs = null;
	
	public FSServerThread (Socket clientSocket, FSServer fsServer) {
		super("Thread For Server Number " + fsServer.getServerNumber());
		this.fs = fsServer;
		this.clientSocket = clientSocket;
	}
	
	//1 thread of the server
	//needs to do 3 things
	//1.  accept input from client socket
	//2.  process input using LoggingServerProtocol
	//3.  output results to client socket
	public void run() {
		try {
			OutputStream out = clientSocket.getOutputStream();
			DataOutputStream dos = new DataOutputStream(out);
			
			InputStream in = clientSocket.getInputStream();
			DataInputStream dis = new DataInputStream(in);
			
			int len = dis.readInt();
		    //System.out.println(len);
		    byte[] data = new byte[len];
		    if (len > 0) {
		        dis.readFully(data);
		    }
		    
		    //System.out.println("here");
			//get client input and send to LoggingServerProtocol
		    MasterMapleJuiceServerProtocol mmsp = new MasterMapleJuiceServerProtocol();
		    byte[] outputToClient = mmsp.processInput(data, this.fs);
		    
			dos.writeInt(outputToClient.length);
			dos.write(outputToClient);
			out.flush();
		}
		catch (SocketException e) {
			//do nothing
		}
		catch (NullPointerException e) {
			//e.printStackTrace();
			//System.out.println("herenow");
		}
		catch (EOFException e) {
			//e.printStackTrace();
			//System.out.println("herenow");
		}
		catch (IOException e) {
			//e.printStackTrace();
			//System.out.println("herenow");
		}
	    //System.out.println("closing client socket on server");
		//close the client socket
		try {
			clientSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	
}
