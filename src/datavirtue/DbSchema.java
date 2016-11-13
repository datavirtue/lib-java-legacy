/**
	DbSchema 1.2
     Sean Anderson - Data Virtue May 2005 - 2007
         CopyRight Data Virtue 2005, 2006, 2007  All Rights Reserved.
 *
     Purpose:
     ---------------------
	
	This object is instantiated by DbEngine 1.0 to provide the proper information 
	for accessing its binary .db files.
	
	* Working ! *
	
	DEV NOTES:
	---------------
	
 * Need Support for (String?) auto-incr fields, default value->,  ??  
 *
 *
 *CHANGES IN 1.2 CORE+
 *
 *Made a slight change in here to size calculation for strings
 *
*/
package datavirtue;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.BufferedReader;  //Only import the needed classes
import java.io.FileReader;

public class DbSchema	{

/**
Provide a filename (.sch) to process while creating DbSchema object.

*/
    private boolean unicode = false;
    DbSchema (String SCH, boolean utf)	{
        
        /* provide the path\SCH filename and DbSchema will compile it */
	schemaFile = SCH;
        unicode = utf;
	init (true);
				
    } /* END CONST */
   /**Provide a custom path for the data store  */
    public DbSchema (String SCH, String p)	{
        
        /* provide the path\SCH filename and DbSchema will compile it */
	
        if (!changePath(p)) System.out.println("Custom path: "+p+" is invalid!");
        
        schemaFile = SCH;
	init (true);
				
    } /* END CONST */
    
    
    private String customPath="";
    
    
    private boolean verifyPath(String p){
        
        if (new File(p).exists()) return true;
        else {
            
            
            new File(p).mkdirs();
            if (new File(p).exists()) return true;
            else return false;
            
        }
        
        
    }
    

    public String getFieldName (int field)	{
        
        /* 0 is always Key */
	return mTable [0] [field];	
	
    }
	
    public int getFieldNumber (String field)	{
        
        for (int i =0; i < num_fields; i++) {
            if (mTable [0] [i].equals(field)) return i;
	}
		
	return -1;
    }
	
    public int getNumFields()	{
        
        /* Returns the number of fields */
	return num_fields;
	
    }
	
    public int getFieldSize (int field)	{/* Working ! */
        
        int size = Integer.parseInt(mTable [2] [field]); 
        //unicode field sizing is handled in sch parsing        
	return size;
    }
	
	/** Returns 1 for TXT, 2 for NUM(FLOAT), 4 for YESNO(boolean) */
	
    public int getFieldType(int field)	{
        
        String type = mTable [1] [field];
		
	if (type.equals("TXT")) return 1;	
		
            else if (type.equals("NUM")) return 2;
		
                else if (type.equals("INT")) return 3;
            
                    else if (type.equals("YESNO")) return 4;
                        //Version 1.5
                        else if (type.equals("LONG")) return 5;
                    
                        else if (type.equals("K")) return 0;
			
                        
					
	return -1; /* ERROR */	
				
    }
			
		
    public int getRecordSize ()	{	/* Working ! */
        
        return recordSize;
	
    }
	
    /** Returns dbname wo/ extension */
    public String getDbName ()	{	
		
        return dbname;
    }

    public String getPath(){
        
        return path;
    }
        
    public boolean changePath(String cPath) {
        
        /* TODO: Why isnt init() called!? */
        if (verifyPath(cPath)) {
            
            customPath = cPath;
            pathFile = customPath + dbname;
            //init(false);
            init(true);
            /* */
            //System.out.println(pathFile);
            /* */
            return true;
        }
        else {
            
            
            customPath = "";  //the custom path is invalid and the one from the schema will be used
            return false;
        }
        
    }
    
    
	/** Returns FULL path/filename.db w/ extension */
    public String getDbPath()	{	
        
        return pathFile+".db";	// eliminate extension addition??
    }
	
    public int isSolid()	{   //deprecate?	
	
        return SOLID;
    }
	

	
    public int nextKey (RandomAccessFile KRAF, boolean incr)	{
        
        /** Set incr to true for an increment after Return */
	/* DO NOT CHANGE */
		
	if (incr) {
            int tmp = key;
            incrementKey(KRAF);
            return tmp; 
        }
			 
		/* This looks strange but the DbEngine keeps the next key ready */
	return key;
    }
	
	
    public boolean isDataCorrect (Object [] data) {
        
        /* check Object [] aginst schema */
	if (data.length != num_fields) { DV.writeFile("db.err", "sch data mismatch :" + System.getProperty("line.separator"), true); return false; }
		
	for (int i =1; i < data.length; i++) {
			
            int x = getFieldType(i);
            
            /* Spruce this up with more intelligence ? */
            
            if (x == 0 || x == 3) { /* KEY */
                
                if (!DV.isInteger(data[i])) return false;
            }
			
            if (x == 1) { /* TXT */
	
                if (!DV.isString(data[i])) return false; 
            }
			
            if (x == 2) { /* NUM */
                
                if (!DV.isFloat(data[i])) return false; 
            }
								
            if (x == 4) { /* YESNO */
                
                if (!DV.isBoolean(data[i])) return false; 
            }
            
            //Version 1.5
            if (x == 5) { /*LONG */
                
                if (!DV.isLong(data[i])) return false; 
            }
	
        }	
	
	return true; /* Return true at the end */
        
    }
	
	
/* ------------------- PRIVATE -----------------------*/
	
    private void init (boolean parse)    {
        
        if (parse) parseSCH();
		
	File db = new File(pathFile+".db");
		
	if (!db.exists()) {
            try {
                db.createNewFile();
		RandomAccessFile KRAF = new RandomAccessFile (pathFile+".db", "rw");
				
		KRAF.writeInt(1); /* Next Key */
		KRAF.close();
		key = 1;
                KRAF = null;
            }catch (Exception e) {DV.writeFile("db.err", "sch Problem Creating db :" + pathFile + System.getProperty("line.separator"), true); }
                        
	}else { 
            try {
                RandomAccessFile KRAF = new RandomAccessFile (pathFile+".db", "rw");
                grabNextKey (KRAF); KRAF.close(); KRAF = null; 
                        
            } catch (Exception e) {DV.writeFile("db.err", "sch Problem getting key :" + pathFile + System.getProperty("line.separator"), true); }
        }   
    }
	
		
	
    private void parseSCH ()	{	/* Object Specific Method (OSM) */
		
        File SCH = new File (schemaFile);
		
	
	if (SCH.exists())	{
			
            try {
                BufferedReader in = new BufferedReader(new FileReader (SCH));
		dbname = in.readLine();
		path = in.readLine();
		if (customPath.equals("")) pathFile = path + dbname;
                else pathFile = customPath + dbname;
			
		num_fields = Integer.parseInt(in.readLine());
									
		mTable = new String [3] [num_fields+1];
					
		/* KEY is grafted into the "users" design at runtime */
		mTable [0] [0] = "KEY";
		mTable [1] [0] = "K";
		mTable [2] [0] = "4";
			
		String temp = null;  /* Remember to re-null */
		int tmp = 0;
			
		int sizeOfRec = 0;
			
		for (int i=1; i <= num_fields; i++)	{
                                                        /* Parsing Loop */
                    for (int j=0; j < 3; j++)	{
                        
			/* CAREFUL! IN HERE */
			temp=in.readLine();		
						
			mTable [j] [i] = temp;
						
			if (j == 2) {   // Sets size of field in bytes
                            
                            if (mTable [j-1] [i].equals("$")) mTable [j] [i] = "4";
                            
                                else if (mTable [j-1] [i].equals("INT")) mTable [j] [i] = "4";  
							
                                    else if (mTable [j-1] [i].equals("NUM")) mTable [j] [i] = "4";
								
                                        else if (mTable [j-1] [i].equals("YESNO")) mTable [j] [i] = "1";
                                            //Version 1.5
                                            else if (mTable [j-1] [i].equals("LONG")) mTable [j] [i] = "8";

                            
                            
                                            else  { 
                                                int size = Integer.parseInt(temp);  //sets the size of the string fields  removed * 2
                                                if (unicode) size = size * 2;
                                                mTable [j] [i] = Integer.toString(size);  /*FIXME*/
                                            }
						
                            sizeOfRec += Integer.parseInt(mTable [j] [i]);
						
                        }
												
                    }
				
                }
			
            in.close();
            recordSize = sizeOfRec+4;
            num_fields++; /* Increment num_fields to compensate for KEY */
            SOLID = 1;
            temp = null;
			
            }catch (Exception e) { DV.writeFile("db.err", "sch Problem parsing :" + schemaFile + System.getProperty("line.separator"), true); }
				
        }else { DV.writeFile("db.err", "sch Schema missing! :" + pathFile + System.getProperty("line.separator"), true); }
	
    }	

    private void grabNextKey(RandomAccessFile KRAF)	{
        
        /* Used Before Recording */
	
	try {
            long bookMark = KRAF.getFilePointer();
            KRAF.seek(0);
            key = KRAF.readInt(); /* Next Key */
            KRAF.seek(bookMark);
        
        }catch (Exception e) {DV.writeFile("db.err", "sch grabNextKey :" +KRAF.toString()+ System.getProperty("line.separator"), true); }
		
    }
	
    private void incrementKey(RandomAccessFile KRAF)	{
        
        /* Used After successful recording */
	
	try {
            long bookMark = KRAF.getFilePointer();
            KRAF.seek(0);
            key = KRAF.readInt(); /* Next Key */
            key++;
            KRAF.seek(0);
            KRAF.writeInt(key);
            KRAF.seek(bookMark);
            
        } catch (Exception e) {DV.writeFile("db.err", "sch Problem incr key :" +KRAF.toString()+ System.getProperty("line.separator"), true); }

		
    }
	
	
private String [] [] mTable=null;	/* memory table */
/*				      0      1	   2   */   
/* mTable Structure (for each field):name, type, size */

private String schemaFile = "_";
private String dbname = "_"; /* dbname no .ext */
private int recordSize = 0;
private String path = "_";  //path no file name
private String pathFile = "_"; /* working data-path w/ filename */
private int num_fields = 0;
private int key = 0;  /* next key for use */
private int SOLID = 0; /* Marker to determine if the Object is OK ??????*/

} /* END CLASS */
