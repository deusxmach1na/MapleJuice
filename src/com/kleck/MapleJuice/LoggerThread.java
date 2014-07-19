package com.kleck.MapleJuice;


//import org.apache.log4j.Logger;
//import org.apache.log4j.BasicConfigurator;
//import java.io.IOException;
//import java.io.IOException;
//import java.util.logging.FileHandler;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//import java.util.logging.SimpleFormatter;

public class LoggerThread extends Thread {
	//private Logger logger; 
    //private FileHandler fileHandler;  
    private String logMe;
    private String processId;
	private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getRootLogger();
    
	public LoggerThread(String processId, String logMe) {
		//this.logger = Logger.getLogger("MyLog");
		this.processId = processId;
		this.logMe = logMe;
	}
	
	public void run() {
    	
	    try {  
	    	//this.fileHandler = new FileHandler(this.processId + ".log", true);
	        //logger.addHandler(fileHandler);  
	        //logger.setLevel(Level.ALL);  
	        //SimpleFormatter formatter = new SimpleFormatter();  
	        //fileHandler.setFormatter(formatter);  
	        //System.out.println(this.processId + " - " + this.logMe);
	        //logger.log(Level.ALL, processId + this.logMe);  
	        log.info(this.processId + " - " + this.logMe);
	        //this.fileHandler.close();
	    } catch (SecurityException e) {  
	        e.printStackTrace();  
	    }
	    /*
	     catch (IOException e) {
	    	e.printStackTrace();
	    }
	    */
	}
}
