package com.kleck.MapleJuice;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.net.BindException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

public class GossipListenThread extends Thread {
	private DatagramSocket server;
	private int portNumber;
	private GroupServer gs;
	private boolean stop;


	public GossipListenThread(int portNumber, GroupServer groupServer) {
		this.portNumber = portNumber;
		this.gs = groupServer;
		this.stop = false;
		try {
			this.setServer(new DatagramSocket(this.portNumber));
		} catch (BindException e) {
			System.out.println("Could not start server.  The port is in use.");
			System.exit(1);
		} catch (SocketException e) {
			System.out.println("Could not start server");
			e.printStackTrace();
		}
	}
	
	public void run() {
		byte[] receiveData = new byte[20000];
		while(true && !this.stop) {
			try {
				DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
				getServer().receive(receivePacket);
				ByteArrayInputStream bis = new ByteArrayInputStream(receivePacket.getData());
				ObjectInput in = new ObjectInputStream(bis);
				Object temp = null;
				try {
					temp = in.readObject();
				} catch (ClassNotFoundException e) {
					System.out.println("Did not find gossiped object.");
					e.printStackTrace();
				} catch (EOFException e) {
					System.out.println("EOF Exception");
					e.printStackTrace();
				}
				
				//needs to be able to take 1 inputs
				//1.  membership list
				MembershipList ml = (MembershipList) temp;				
				//spin up a new thread to merge the two lists
				MergeMembershipListThread mmlt = new MergeMembershipListThread(ml, this.gs);
				//System.out.println(this.gs.getMembershipList().toString());
				mmlt.start();
				bis.close();
				in.close();

			}      	
			catch(SocketException e) {
				//System.out.println("Gossip Socket Closed.\n");
			}       	
			catch(EOFException e) {
				System.out.println("End of File Error\n");
				e.printStackTrace();
			}
			catch(IOException e) {
				System.out.println("I/O issue on the server side");
				e.printStackTrace();
			}        	
		}
	}

	public DatagramSocket getServer() {
		return server;
	}

	public void setServer(DatagramSocket server) {
		this.server = server;
	}	
	
	public boolean isStop() {
		return stop;
	}

	public void setStop(boolean stop) {
		this.stop = stop;
	}
}
