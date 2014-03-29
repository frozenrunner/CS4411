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
	private static final boolean DEBUG = true;
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
			db = mongoClient.getDB("mikeDB");
			coll = db.getCollection("CS4411MapReduce");
			
			//zero database
			coll.remove(new BasicDBObject());
			reader = new Scanner(inputFile);
			while(reader.hasNext())
			{
				line = reader.next();
				if (DEBUG)
					System.out.println(line);
				
				String[] inputLine = line.split(",");
				if (DEBUG)
				{
					for(int i=0; i<inputLine.length; i++)
						System.out.println(inputLine[i]);
				}
				double salary = Double.parseDouble(inputLine[4]);
				doc = new BasicDBObject("yearID", inputLine[0]).append("teamID", inputLine[1] ).append("lgID", inputLine[2]).append("playerID", inputLine[3]).append("salary", salary);
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
		
		String mapFunction = "function () { emit( this.teamID, this.salary );}";
		String reduceFunction = "function(keyTeam, salary) { return Array.sum(salary); };";
		
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
			
			out.close();
		}
		catch (IOException e)
		{
			System.out.println(e.getMessage());
		}
	}

	
	public static void main(String[] args) 
	{
		Importer processInputFile = new Importer("Salaries.csv");
		processInputFile.run();
	}

}