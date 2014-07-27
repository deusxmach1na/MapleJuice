package com.kleck.MapleJuice;

public class FailRecoveryThread extends Thread {
	private GroupServer gs;
	
	public FailRecoveryThread(GroupServer gs) {
		this.gs = gs;
	}
	
	public void run() {
		System.out.println("System Rebalancing");
		//long currentTime = System.currentTimeMillis();
		this.gs.getMembershipList().setMaster();
		this.gs.getMembershipList().setSuccessors();
		//long masterTime = System.currentTimeMillis();
		//System.out.println("***********************");
		//System.out.println("** Master Selection took " + (masterTime - currentTime) + " milliseconds.**");
		//System.out.println("***********************");
		this.gs.replicateFiles("");
		
	}

}
