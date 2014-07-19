package com.kleck.MapleJuice;

import java.util.ArrayList;
import java.util.Random;
import java.util.Set;

/*
 * needs to choose log(n) server(s) at random to gossip to
 * 
 * 
 */

public class RandomGossipThread extends Thread {
	private GroupServer gs;
	private int numGossips;
	
	public RandomGossipThread (GroupServer gs) {
		this.gs = gs;
		//this.numGossips = 2;
		
	}
	
	public void run() {
		Set<String> keys = this.gs.getMembershipList().getKeys();
		ArrayList<String> randomKeys = new ArrayList<String>();
		ArrayList<String> completedGossips = new ArrayList<String>();
		Random rand = new Random();
		int failedContactServers = 0;
		
		//convert to arraylist for easier randomization
		for(String key: keys) {
			MembershipListRow member = this.gs.getMembershipList().getMember(key);
			if(member.isContact() && (member.isHasLeft() || member.isDeletable())) {
				//System.out.println("here");
				failedContactServers += 1;
				String ipAddress = member.getIpAddress();
				int portNumber = member.getPortNumber();
				GossipSendThread gst = new GossipSendThread(ipAddress, portNumber, this.gs.getMembershipList());
				gst.start();
			}
			else {
				randomKeys.add(key);
			}
		}
		
		//number of gossips = LOG2(N)
		this.numGossips = (int)(Math.log(keys.size() - failedContactServers)/Math.log(2));
		//System.out.println(numGossips);
		
		while(completedGossips.size() < this.numGossips) {
			int n = rand.nextInt(keys.size() - failedContactServers);
			String randomId = randomKeys.get(n);
			if(!completedGossips.contains(randomId) && !randomId.equals(this.gs.getProcessId())) {
				String ipAddress = this.gs.getMembershipList().getMember(randomId).getIpAddress();
				int portNumber = this.gs.getMembershipList().getMember(randomId).getPortNumber();
				GossipSendThread gst = new GossipSendThread(ipAddress, portNumber, this.gs.getMembershipList());
				completedGossips.add(randomId);
				//System.out.println("sending thread started");
				gst.start();
			}
		}
		
		//if you have no one to gossip to then gossip to the contact server to try and join
		if(keys.size() == 1 && this.gs.getMembershipList().getMember(this.gs.getProcessId()).getProcessId().equals(this.gs.getProcessId())) {
			this.gs.sendToContactServer();
		}
		
		
	}

}
