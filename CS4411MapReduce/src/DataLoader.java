import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

import com.google.gson.Gson;

/**
 * Class to load MapReduce output data and convert it to the JSON format
 * @author Michael Horlick
 *
 */
public class DataLoader
{
	private final static boolean DEBUG = false;
	private PathData json;
	
	/**
	 * Constructor
	 * @param filePath	File path of the input file
	 */
	public DataLoader(String filePath)
	{
		createJSON(filePath);
	}
	
	/**
	 * Method to parse the input data and create the JSON file
	 * @param filePath	File path of the input file
	 */
	private void createJSON(String filePath)
	{
		Scanner reader;
		String line;
		File inputFile = new File(filePath);
		
		try
		{
			String temp;
			reader = new Scanner(inputFile);
			reader.useDelimiter("\n");
			line = reader.next();
			String[] inputLine = line.split("\"");
			String[] inputPath;
			temp = inputLine[6];
			temp = temp.substring(3, temp.length()-1);
			
			json = new PathData(inputLine[3], temp);
			while(reader.hasNext())
			{
				line = reader.next();
				inputLine = line.split("\"");
				
				temp = inputLine[6];
				temp = temp.substring(3, temp.length()-1);
				
				inputPath = inputLine[3].split("/");
				
				PathData tempPath;
				PathData tempParent = json;
	
				for(int i=1; i<inputPath.length; i++)
				{
					tempPath = tempParent.findChild(inputPath[i]);
					if(DEBUG)
					{
						System.out.println("value of i: " + i);
						System.out.println("inputPath[i]: " + inputPath[i] + "\n");
					}
					if (tempPath == null)
					{
						if (i == 1)
						{
							if(DEBUG)
							{
								System.out.println("Level directly below root");
								System.out.println(i);
								System.out.println(inputPath[i] + "\n");
							}
							json.addChild(inputPath[i],temp);
						}
						else
						{
							if (DEBUG)
								System.out.println("inputPath[i-1]: " + inputPath[i-1] + "\n");
							tempParent.addChild(inputPath[i], temp);
						}
					}
					else
					{
						if (DEBUG)
							System.out.println("tempPath folderName:" + tempPath.getFolderName() + "\n");
						tempParent = tempPath;
					}
				}
			}
		reader.close();
		}
		catch (FileNotFoundException e)
		{
			System.out.println("File Not Found: " + e.toString());
		}
	}
	
	/**
	 * Method to write the JSON file to disk under name 'uwonetwork.json'
	 */
	public void writeJSON()
	{
		Gson gson = new Gson();
		FileWriter fstream;
		BufferedWriter out = null;
		
		if (DEBUG)
			System.out.println(gson.toJson(json));
		
		try 
		{
			fstream = new FileWriter("uwonetwork.json");
			out = new BufferedWriter(fstream);
			out.write(gson.toJson(json));
			out.close();
		} catch (IOException e)
		{
			System.out.println("IO Error: " + e.toString());
		}

	}
	/*
	public static void main(String []args)
	{
		DataLoader processInputFile = new DataLoader("MapReduceResult.txt");
	}*/
}
