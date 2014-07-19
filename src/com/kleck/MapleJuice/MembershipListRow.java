package com.kleck.MapleJuice;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;


//1 entry in a MembershipList
class MembershipListRow implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String processId;
	private String ipAddress;
	private int portNumber;
	private int filePortNumber;
	private int hbCounter;
	private long timeStamp;
	private boolean isDeletable;
	private boolean hasLeft;
	private boolean isContact;
	private boolean isMaster;
	private long hashKey;
	private String successor;

	private HashMap<String, String> mr;

	public MembershipListRow(String processId, int portNumber, boolean isContact) {
		this.processId = processId;
		this.setPortNumber(portNumber);
		this.hbCounter = 0;
		this.isDeletable = false;
		this.isContact = isContact;
		this.setHasLeft(false);
		setIpAddress();
		setTimeStamp();
		mr = new HashMap<String, String>();
		updateHashMap();
		this.isMaster = false;
	}
	
	//mutations
	public String getProcessId() {
		return processId;
	}

	public void setProcessId(String processId) {
		this.processId = processId;
	}

	public int getHbCounter() {
		return hbCounter;
	}

	public void setHbCounter(int hbCounter) {
		this.hbCounter = hbCounter;
	}
	
	public void incrementHbCounter() {
		this.hbCounter += 1;
	}
	
	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp() {
		this.timeStamp = System.currentTimeMillis();
	}

	public boolean isDeletable() {
		return isDeletable;
	}

	public void setDeletable(boolean isDeletable) {
		this.isDeletable = isDeletable;
	}	
	
	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress() {
		try {
			this.ipAddress = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			System.out.println("Could not get host address");
			e.printStackTrace();
		}
	}
	
	public HashMap<String, String> getHashMap() {
		return this.mr;
	}
	
	public void updateHashMap() {
		this.mr.put("processId", this.processId);
		this.mr.put("ipAddress", this.ipAddress);
		this.mr.put("hbCounter", Integer.toString(this.hbCounter));
		this.mr.put("timeStamp", Long.toString(this.hbCounter));
		this.mr.put("isDeletable", Boolean.toString(this.isDeletable));
	}
	
	public String toString() {
		return "ProcessId = " + this.processId 
				+ "\nPortNumber = " + this.portNumber 
				+ "\nFile PortNumber = " + this.filePortNumber 
				+ "\nHearbeat = " + this.hbCounter 
				+ "\nTimeStamp = " + this.timeStamp 
				+ "\nIsDeletable = " + this.isDeletable 
				+ "\nIsHasLeft = " + this.hasLeft 
				+ "\nHashKey = " + this.hashKey 
				+ "\nSuccessor = " + this.successor
				+ "\nisContact = " + this.isContact
				+ "\nisMaster = " + this.isMaster;
	}

	public int getPortNumber() {
		return portNumber;
	}

	public void setPortNumber(int portNumber) {
		this.portNumber = portNumber;
	}

	public boolean isHasLeft() {
		return hasLeft;
	}

	public void setHasLeft(boolean hasLeft) {
		this.hasLeft = hasLeft;
	}

	public boolean isContact() {
		return isContact;
	}

	public void setContact(boolean isContact) {
		this.isContact = isContact;
	}

	public long getHashKey() {
		return hashKey;
	}

	public void setHashKey(long hashKey) {
		this.hashKey = hashKey;
	}

	public void setSuccessor(String result) {
		this.successor = result;	
	}
	
	public String getSuccessor() {
		return this.successor;
	}

	public boolean isMaster() {
		return isMaster;
	}

	public void setMaster(boolean isMaster) {
		this.isMaster = isMaster;
	}

	public int getFilePortNumber() {
		return filePortNumber;
	}

	public void setFilePortNumber(int filePortNumber) {
		this.filePortNumber = filePortNumber;
	}
}
