package com.kleck.MapleJuice;

public class FailRecoveryThread extends Thread {
	private GroupServer gs;
	private String failedProcess;
	
	public FailRecoveryThread(GroupServer gs, String failedProcess) {
		this.gs = gs;
		this.failedProcess = failedProcess;
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
		this.gs.replicateFiles(failedProcess);
		
	}

}
