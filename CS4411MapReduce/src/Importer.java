import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Class to import data from text file into MongoDB Database
 * @author Michael Horlick
 *
 */
public class Importer
{
	private static final String CONNECTIONSTRING = "localhost";
	
	private DBCollection coll;	//DBcollection used
	//private long startTime;
	//private long endTime;
	
	/**
	 * Constructor to initialize the program to pull input data.
	 * @param filePath Path the data file to be MapReduced is located.
	 */
	public Importer(String filePath)
	{
		populateDB(filePath);
	}
	
	private void run()
	{
		//startTime = System.currentTimeMillis();
		mapReduceDB();
		loadData();
	}

	/**
	 * Create database connection, populateReads the information from the input file and populates the MongoDB database.
	 */
	private void populateDB(String filePath)
	{
		BasicDBObject doc;
		DB db;
		Mongo mongoClient;

		Scanner reader;
		String line;
		File inputFile = new File(filePath);
		
		try
		{
			
			mongoClient = new Mongo(CONNECTIONSTRING);
			db = mongoClient.getDB("myDB");
			coll = db.getCollection("UWONetworkMap");
			
			//zero database
			coll.remove(new BasicDBObject());
			reader = new Scanner(inputFile).useDelimiter("\u0000");
			while(reader.hasNext())
			{
				line = reader.next();
				String[] inputLine = line.split("\t");
				double byteSize = Double.parseDouble(inputLine[0]);
				doc = new BasicDBObject("size_in_bytes", byteSize).append("owner", inputLine[1] ).append("group", inputLine[2]).append("last_mod_date", inputLine[3]).append("path", inputLine[4]);
				coll.insert(doc);
			}
			reader.close();
		}
		catch (FileNotFoundException e)
		{
			System.out.println("File Not Found");
		}
		catch (IOException e)
		{
			System.out.println("IO Error");
		}
	}
	
	/**
	 * Creates DB connection and performs MapReduce on the data contained within the MongoDB database.
	 * The results of mapReduce are written to a file.
	 */
	private void mapReduceDB()
	{
		MapReduceCommand mrc;
		MapReduceOutput mro;
		
		FileWriter fstream;
		BufferedWriter out = null;
		
		String mapFunction = "function(){ var line = this.path; if (line.charAt(0) == \'/\') { emit(\"/\", this.size_in_bytes); line = line.substring(1,line.length); var arrayOfStrings = line.split(\"/\"); if (arrayOfStrings.length > 1) { var temp = \"/\" + arrayOfStrings[0]; for (var i = arrayOfStrings.length-1; i > 0; i--) { for (var j = 1; j < i; j++) { temp = temp + \"/\" + arrayOfStrings[j]; } emit(temp, this.size_in_bytes); temp = \"/\" + arrayOfStrings[0]; }}}}";
		String reduceFunction = "function(keyPath, sizeSum){ return Array.sum(sizeSum); };";
		
		try
		{
			mrc = new MapReduceCommand(coll, mapFunction, reduceFunction, null, MapReduceCommand.OutputType.INLINE, null);
			mro = coll.mapReduce(mrc);
			
			fstream = new FileWriter("MapReduceResult.txt");
			out = new BufferedWriter(fstream);
			for (DBObject o : mro.results())
			{	 
				out.write(o.toString());
				out.write("\n");
			}
			//endTime = System.currentTimeMillis();
			//long totalTime = endTime-startTime;
			
			out.close();
		}
		catch (IOException e)
		{
			System.out.println(e.getMessage());
		}
	}
	
	/**
	 * Method to load data into the JSON creator
	 */
	private void loadData()
	{
		
		DataLoader dataLoader = new DataLoader("MapReduceResult.txt");
		dataLoader.writeJSON();
	}
	
	public static void main(String[] args) 
	{
		Importer processInputFile = new Importer("idx-arion");
		processInputFile.run();
	}

}