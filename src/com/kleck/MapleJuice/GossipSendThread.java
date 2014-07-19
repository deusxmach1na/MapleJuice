package com.kleck.MapleJuice;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
//import java.util.Random;

public class GossipSendThread extends Thread{
	private String ipAddress;
	private int portNumber;
	private Object gossipObject;
	//private GroupServer gs;
	
	public GossipSendThread (String ipAddress, int portNumber, Object gossipObject) {
		this.ipAddress = ipAddress;
		this.portNumber = portNumber;
		this.gossipObject = gossipObject;
		//this.gs = gs;
	}

	public void run() {
		try {
			//int bytesUsed = 0;
			DatagramSocket clientSocket = new DatagramSocket();
			InetAddress IPAddress = InetAddress.getByName(this.ipAddress);
			
			//prepare to send packet
			byte[] sendData = new byte[20000];
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutput out = new ObjectOutputStream(bos); 
			//send the object
			out.writeObject(gossipObject); 
			sendData = bos.toByteArray();
			
			//send the packet to the appropriate place
			DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, this.portNumber);
			clientSocket.send(sendPacket);
			
			//simulate packet loss
			/*
			Random rand = new Random();
			if(rand.nextInt(100) > 49) {  //0-99  need 1, 5, 15, 50
				clientSocket.send(sendPacket);
			}
			*/
			
			//measurements
			//bytesUsed = bos.size();
			//this.gs.updateBytesUsed(bytesUsed);
			//this.gs.updateRunTime();
			
			//clean up
			clientSocket.close();
			bos.close();
			out.close();
			
		}
		catch (IOException e) {
			System.out.println("I/O Exception with server.");
			e.printStackTrace();
		}
	}

}
