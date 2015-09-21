package main;

import java.net.MalformedURLException;

import org.apache.log4j.PropertyConfigurator;


public class Main {
	public static void main(String [] args) throws MalformedURLException {
		String log4jConfPath = "./log4j.properties";
		PropertyConfigurator.configure(log4jConfPath);
		MigrationTool testManager = new MigrationTool();
		
		testManager.migrateFacilities();
		//testManager.migrateDatafileParameters();
		
		testManager.testCreate();
	}
}