import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Iterator;
import java.util.Vector;

/**
 * Class representing the path and value data for our MapReduce data set.
 * @author Michael Horlick
 *
 */
public class PathData
{
	private String folderName;
	private double value;
	private Vector<PathData> children;
	private static final boolean DEBUG = false;
	
	/**
	 * Constructor when only a folder name is available
	 * @param folderName	The name of the folder
	 */
	public PathData(String folderName)
	{
		this.folderName = folderName;
		//children = new Vector<PathData>();
	}
	
	/**
	 * Constructor when folder name and size are available
	 * @param folderName	The name of the folder
	 * @param value			The value (size) of the folder, in bytes
	 */
	public PathData(String folderName, String value)
	{
		this.folderName = folderName;
		this.value = Double.parseDouble(value);
		//this.value = convertFromSciNote(value);
		//children = new Vector<PathData>();
	}
	
	/**
	 * 
	 * @param folderName	The name of the folder
	 * @param value			The value (size) of the folder, in bytes
	 * @param children
	 */
	public PathData(String folderName, String value, Vector<PathData> children)
	{
		this.folderName = folderName;
		this.value = Double.parseDouble(value);
		//this.value = convertFromSciNote(value);
		this.children = children;
	}
	
	/**
	 * Method to get the folder name of this PathData object
	 * @return	The folder name
	 */
	public String getFolderName()
	{
		return folderName;
	}
	
	/**
	 * Method to set the folder name of this PathData object
	 * @param folderName	The name of the folder
	 */
	public void setFolderName(String folderName)
	{
		this.folderName = folderName;
	}
	
	/**
	 * Method to get the value(size) in bytes of the folder represented by this PathData object
	 * @return	The string representing the value(size)
	 */
	public double getValue()
	{
		return value;
	}
	
	/**
	 * Method to set the value(size) in bytes of the folder represented by this PathData object
	 * @param value	The value(size) in bytes of the folder
	 */
	public void setValue(String value)
	{
		this.value = Double.parseDouble(value);
		//this.value = convertFromSciNote(value);
	}
	
	/**
	 * Method to add child PathData object to this PathData object
	 * @param folderName	The folder name of the child
	 */
	public void addChild(String folderName)
	{
		PathData temp = new PathData(folderName);
		
		if (children == null)
			children = new Vector<PathData>();
		
		children.add(temp);
	}
	
	/**
	 * Method to add child PathData object to this PathData object
	 * @param folderName	The folder name of the child object
	 * @param value			The value(size) in bytes of the child object
	 */
	public void addChild(String folderName, String value)
	{
		PathData temp = new PathData(folderName, convertFromSciNote(value));
		if (children == null)
			children = new Vector<PathData>();
		
		children.add(temp);
	}
	
	/**
	 * Method to iterate through this object's child objects and find a specific child
	 * @param childName	The name of the child to be located
	 * @return			A pointer to the child object
	 */
	public PathData findChild(String childName)
	{
		if (children != null)
		{
		Iterator<PathData> it = children.iterator();
			
			while (it.hasNext())
			{
				PathData temp = it.next();
				if (temp.folderName.compareTo(childName) == 0)
				{
					if(DEBUG)
					{
						System.out.println("Class: PathData");
						System.out.println("childName: " + childName);
						System.out.println("temp.folderName: " + temp.folderName + "\n");
					}
					return temp;
				}
			}
		}
		return null;
	}
	
	/**
	 * Method to convert scientific notation to full digit number
	 * @param value	number to be converted
	 */
	private String convertFromSciNote(String value)
	{
		double d = Double.parseDouble(value);
		NumberFormat format = new DecimalFormat("#");
		String f = format.format(d);
		return f;
	}
	
	/**
	 * Method to create the String representation of this object and it's child objects
	 */
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("Folder:" + folderName + "," + "Value:" + value + "\n");
		System.out.println(value);
		if (children.size() != 0)
		{
			for (int i=0; i<children.size(); i++ )
			{
				sb.append(children.toString() + "\n");
			}
		}
		return(sb.toString());
	}
}
