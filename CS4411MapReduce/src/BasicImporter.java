import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Scanner;

public class BasicImporter {

	private static final boolean DEBUG = false;
	private String[][] fileLine;
	private long startTime;
	private long endTime;
	
	public BasicImporter(String filePath)
	{
		processFile(filePath);
	}
	
	private void processFile(String filePath)
	{
		startTime = System.currentTimeMillis();
		Scanner reader;
		String line;
		File inputFile = new File(filePath);
		
		fileLine= new String[50][2];
		try
		{
			reader = new Scanner(inputFile);
			while(reader.hasNext())
			{
				line = reader.next();
				
				String[] inputLine = line.split(",");
				
				String salary = inputLine[4];
				String team = inputLine[1];
				
				int checkTeam = teamExists(team);
				
				if (checkTeam != -1)
				{
					long temp1 = Long.parseLong(salary);
					long temp2 = Long.parseLong(fileLine[checkTeam][1]);
					temp2 = temp1 + temp2;
					fileLine[checkTeam][1] = String.valueOf(temp2);
				}
				else
				{

					for (int i = 0; i<fileLine.length; i++)
					{
						if (fileLine[i][0] == null)
						{
							fileLine[i][0] = team;
							fileLine[i][1] = salary;
							break;
						}
					}
				}
			}
			reader.close();
			
			FileWriter fstream;
			BufferedWriter out = null;
			
			fstream = new FileWriter("BadReduceResult.txt");
			out = new BufferedWriter(fstream);
			for (int i = 0; i<fileLine.length; i++)
			{
				out.write("Team: " + fileLine[i][0] + " | " + "Salary: " + fileLine[i][1]);
				out.write("\n");
			}				
				out.close();
				endTime = System.currentTimeMillis();
				System.out.println(endTime-startTime);
		}
		catch(Exception e)
		{
			System.out.println("Error Message: " + e.getMessage());
		}
	}
	
	private int teamExists(String team)
	{
		
		for (int i = 0; i<fileLine.length; i++)
		{
			if (fileLine[i][0] == null)
			{
				return -1;
			}
			else if (fileLine[i][0].compareTo(team) == 0)
			{	
				return i;
			}
		}
		return -1;
	}
	
	public static void main(String[] args)
	{
		BasicImporter processInputFile = new BasicImporter("Salaries.csv");
	}
}
