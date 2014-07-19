package com.kleck.MapleJuice;

/*
 * contains logic for marking membership list entries as 
 * isDeletable and actually removes old entries
 * 
 * 
 */
public class UpdateMembershipListThread extends Thread {
	private GroupServer gs;
	private long timeFail;
	
	public UpdateMembershipListThread(GroupServer gs, long timeFail) {
		this.gs = gs;	
		this.timeFail = timeFail;
	}
	
	public void run() {
		int contactServers = 0;	
		boolean needsRebalance = false;
		
		
		//if there are multiple contact servers stop trying to communicate with the one that left
		for(String key: this.gs.getMembershipList().getKeys()) {
			MembershipListRow member = this.gs.getMembershipList().getMember(key);
			if(member.isContact()) { 
				contactServers += 1;
			}
		}
		
		if(contactServers > 1) {
			for(String key: this.gs.getMembershipList().getKeys()) {
				MembershipListRow member = this.gs.getMembershipList().getMember(key);
				if(member.isContact() && (member.isHasLeft() || member.isDeletable())) {
					LoggerThread lt = new LoggerThread(this.gs.getProcessId(), "#REMOVED_OLD_CONTACT#" + key);
					lt.start();	 
					this.gs.getMembershipList().removeMember(key);
				}
			}
		}
		
		
		//mark things as deletable if they have not been updated 
		//in timeFail milliseconds
		for(String key: this.gs.getMembershipList().getKeys()) {	
			MembershipListRow member = this.gs.getMembershipList().getMember(key);
			long compareTime = member.getTimeStamp();	
			long currentTime = System.currentTimeMillis();
			
			
			//mark as deletable if it's been timeFail milliseconds
			//DELETE
			if((currentTime - compareTime) > timeFail && !key.equals(this.gs.getProcessId())) {
				//if(!member.isContact()) {
					//LoggerThread lt = new LoggerThread(this.gs.getProcessId(), "#PROCESS_FAILING#" + key);
					//lt.start();
				//}
				
				//if its not marked deletable yet then do a preemptive rebalance
				if(!(member.isDeletable() || member.isHasLeft())) {
					needsRebalance = true;
				}
				member.setDeletable(true);
				//System.out.println(key + " DELETE");
			}	
			
			//mark as do NOT DELETE if it has responded
			if((currentTime - compareTime) <= timeFail) {
				member.setDeletable(false);
				//this.gs.getMembershipList().setMaster();
			}
			
			
			//if process has left voluntarily mark it as such
			//LEFT VOLUNTARILY
			if((currentTime - compareTime) > 2 * timeFail && member.isDeletable() && member.isHasLeft()) {
				//if its the contact server that left keep sending to it occasionally
				if(!member.isContact()) {
					needsRebalance = true;
					LoggerThread lt = new LoggerThread(this.gs.getProcessId(), "#PROCESS_LEFT_VOLUNTARILY#" + key);
					lt.start();	
					this.gs.getMembershipList().removeMember(key);
				}
				else if(currentTime - compareTime <= 3 * timeFail) {
					needsRebalance = true;
					LoggerThread lt = new LoggerThread(this.gs.getProcessId(), "#CONTACT_LEFT_VOLUNTARILY#" + key);
					lt.start();	
				}
			}
			
			//delete if it is marked as isDeletable and has passed 2 * timeFail milliseconds
			//REMOVE
			if((currentTime - compareTime) > 2 * timeFail && member.isDeletable() && !member.isHasLeft()) {
				if(!member.isContact()) {
					needsRebalance = true;
					LoggerThread lt = new LoggerThread(this.gs.getProcessId(), "#REMOVED_FAILED_PROCESS#" + key);
					lt.start();	
					this.gs.getMembershipList().removeMember(key);
				}
				//System.out.println(key + " REMOVE");
				else if((currentTime - compareTime) <= 3 * timeFail) {
					needsRebalance = true;
					LoggerThread lt = new LoggerThread(this.gs.getProcessId(), "#REMOVED_FAILED_CONTACT#" + key);
					lt.start();	
				}
			}		
		}
		if(needsRebalance) {
			FailRecoveryThread frt = new FailRecoveryThread(this.gs);
			frt.start();
		}
		//this.gs.updateSuccessors();
		/*
		//see if we need to start an election
		boolean needElection = true;
		for(String key: this.gs.getMembershipList().getKeys()) {
			if(this.gs.getMembershipList().getMember(key).isMaster() && !this.gs.getMembershipList().getMember(key).isDeletable()) {
				needElection = false;
			}	
		}
		
		//send election thread if needed
		if(needElection) {
			ElectionMessage em = new ElectionMessage(this.gs.getProcessId(), false);
			SendElectionThread set = new SendElectionThread(this.gs, em);
			set.start();
		}
		*/
		this.gs.updateRunTime();
	}
}
