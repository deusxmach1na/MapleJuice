package com.kleck.MapleJuice;

import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

//stores the membershiplist for 1 process
//access by processId
public class MembershipList implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private ConcurrentMap<String, MembershipListRow> ml;
	
	public MembershipList() {
		this.ml = new ConcurrentHashMap<String, MembershipListRow>();	
	}
	
	public void addNewMember(String processId, int portNumber, boolean isContact) {
		this.ml.putIfAbsent(processId, new MembershipListRow(processId, portNumber, isContact));
	}
	
	public void removeMember(String processId) {
		this.ml.remove(processId);
	}
	
	public void updateMember(String processId, MembershipListRow mr) {
		this.ml.put(processId, mr);
	}

	public MembershipListRow getMember(String processId) {
		return this.ml.get(processId);
	}
	
	public Set<String> getKeys() {
		return this.ml.keySet();
	}
	
	public Set<String> getActiveKeys() {
		Set<String> result = new HashSet<String>();
		for(String key:this.getKeys()) {
			if(!this.getMember(key).isDeletable()) {
				result.add(key);
			}
		}
		return result;
	}
	
	public boolean hasKey(String processId) {
		return this.ml.containsKey(processId);
	}
	
	public int size() {
		return this.ml.size();
	}
	
	//may use this function to hash file names later
	public static long getHashValue (String processId) {
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("md5");
		} catch (NoSuchAlgorithmException e) {
			System.out.println("could not find algorithm");
			//e.printStackTrace();
		}
		BigInteger hashValue = new BigInteger(md.digest(processId.getBytes()));
		long finalHash = hashValue.longValue() % 1000007L;
		return Math.abs(finalHash);
		
	}
	
	//set the successors for every active key
	public void setSuccessors() {	
		for(String key1: this.getActiveKeys()) {
			long min = 10000007L;
			long max = 0L;
			long temp = 10000007L;
			String result = "";
			for(String key: this.getActiveKeys()) {
				if(!this.getMember(key).isDeletable()) {
					if(getHashValue(key) < min) {
						min = getHashValue(key);
					}
					if(max < getHashValue(key)) {
						max = getHashValue(key);
					}
					if(getHashValue(key) > getHashValue(key1) && getHashValue(key) < temp) {
						temp = getHashValue(key);
					}	
				}
			}
			//if max = processId then set successor to min
			if(getHashValue(key1) == max) {
				result = getProcessIdByHash(min);
			}
			else {
				result = getProcessIdByHash(temp);
			}
			this.getMember(key1).setHashKey(getHashValue(key1));
			this.getMember(key1).setSuccessor(result);
		}
	}	
	
	
	public String getProcessIdByHash(long temp) {
		String result = "";
		for(String key:this.getKeys()) {
			if(getHashValue(key) == temp) {
				result = key;
			}
		}
		return result;
	}

	
	public String toString() {
		String ret = "";
		for(String key: this.ml.keySet()) {
			ret += "\n" + this.ml.get(key).toString() + "\n";
		}
		return ret;
	}

	//master election 4th credit
	public void setMaster() {
		for(String key: this.ml.keySet()) {
			if(key.equals(this.getMaster())) {
				this.ml.get(key).setMaster(true);
			}
			else {
				this.ml.get(key).setMaster(false);
			}
		}
	}

	//get the process that has been around the longest
	public String getMaster() {
		long min = 9223372036854775807L;
		String result = "";
		//return the oldest process
		for(String key:this.getActiveKeys()) {
			long time = Long.parseLong(this.getMember(key).getProcessId().replace("-" + this.getMember(key).getIpAddress(), ""));
			if(time < min) {
				min = time;
				result = key;
			}
		}
		return result;
	}
	
}

