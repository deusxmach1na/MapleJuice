package com.kleck.MapleJuice;

/*
 * real simple thread just needs to update the heartbeat
 * and setTimeStamp
 */
public class UpdateHeartbeatThread implements Runnable {
	private GroupServer gs;
	
	public UpdateHeartbeatThread(GroupServer gs) {
		this.gs = gs;
	}
	
	public void run() {
		this.gs.getMembershipList().getMember(this.gs.getProcessId()).incrementHbCounter();
		this.gs.getMembershipList().getMember(this.gs.getProcessId()).setTimeStamp();
	}

}
