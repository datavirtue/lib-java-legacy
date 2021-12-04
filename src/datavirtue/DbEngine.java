
/****************************************************************************
     DbEngine 1.2 CORE+
     Created by Sean Kristen Anderson - Data Virtue 2005 - 2007
     CopyRight Data Virtue 2005, 2006, 2007  All Rights Reserved.
 *  
 * Started May 2005

	TO DO:  ??
  --------------------------------------------------------------------------
  Create report object?
 * desgin a method to create a data control-layer Java Bean?
 * Create index system for searching large files?
 * Output DTD and XML files for EDI?
 * Based on searches you can export a set of keys to XML and then generate a DTD from that
 * make utility to tranzfer to SQL Engine
 * support transactions??
 * buld method to return Object[] filled with data from a specified column
 * make method to search a list of keys passed in as an ArrayList
 * create browser dialog object for return of specified column selected row value
 * handle combo boxes somehow
 * build logging system?
 * build method to find AND return certain columns of data from a record
 * make a public method to set data location  <<>>
 * --------------------------------------------------------------------------

     System Features:

 * Can load as many databases as you wish (Data System)  [.dsf file]
 * Databases are easily definable through text files (Schema Files) [.sch file]
 * Supports relational data structuring (you can do whatever you want programatically)
 * Import/Export of common .csv files /
 * Ready-made methods to generate sorted TableModels for displaying data & search results
 * Supports 50000+ records per file
 * Easy backup and restoration of data
 * Contains methods for general and limited advanced searching
 * Instant record deletes with no data loss on failure
 * Easily integrates into any project with super small learning curve
 * 
 *
CHANGES IN 1.2 CORE+
 *Changed recording scheme for stings, version 1.0 was UTF encoded strings which took up too much space
 *Converted from UTF to bytes with top half dropped  - RandomAccessFile.writeBytes(String s) method
 *Used to use RandomAccess File writeChar(char c) which sucks down 2 bytes for each one
 *
 *Changed file mode scheme to only be in the mode needed.  For example when getting records the db is in Read Only mode
 *When saving records the file is in Read/Write mode. this is done to increase speed and reliability for data queries (searches)
 *
*****************************************************************************/
package datavirtue;

import RuntimeManagement.GlobalApplicationDaemon;
import RuntimeManagement.RuntimeIncident;
import java.text.*;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.BufferedReader;  //Only import the needed classes
import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.io.FileReader;
//import java.io.FileWriter;
import java.util.ArrayList;
import javax.swing.table.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.regex.PatternSyntaxException;
import javax.swing.*;

//import de.schlichtherle.io.FileWriter;

public class DbEngine	  {
private boolean debug = false;
private GlobalApplicationDaemon application;
 public DbEngine () {

    //better use loadSchema after calling with this constructor

}/* END CONSTRUCTOR */

private boolean unicode = false;

 public DbEngine (String DSF, GlobalApplicationDaemon g, boolean utf) {
	// make another constructor to handle a single .sch
    /* Process Database System File */
    application = g;
    unicode = utf;
    loadDSF(DSF);

}/* END CONSTRUCTOR */

/*-------------- PUBLIC  API Data Processing Methods ------------*/

/* These methods work on the "currentDb" global */

    /** Cycles through each record and keeps all but the one record
         that has been specified in 'int key'*/

 public void setUnicode(boolean u, String DSF){
     
     unicode = u;
     schList = new ArrayList(0);//  add this back if changing sch read scheme
     this.currentDb = "_";
     this.currentSchema = null;
     this.dbFile = null;
     this.nowdb = "_";
     RAF=null;
     sorter = null;
     num_files = 0;
     this.current_mode = "rw";
     
     this.loadDSF(DSF);
    /* this.dbFile = null;
     this.currentDb = "_";
     this.currentSchema = null;*/
     
 }
 
    public boolean removeRecords (String dbname, int col, int key) {  //not in use
   /*  //this override is used to delete records that contain a reference to another .db/record
     //look through db to find actual key of the record which spec'd col contains key
        use_db(dbname);

        ArrayList keys = scanColumn ( col, Integer.toString(key), false, true);
        if (keys.size() == 0) return true;
        boolean b;
        for (int i=0; i < keys.size(); i++) {

           //b = removeRecord (dbname, Long.valueOf( (Long) keys.get(i) ) );
            if (b == false) return false;

            //this is SLOWWWWWWW!!!!!
        }
*/

            return true;
    }

  
    /**
     *In use
     */
    public boolean removeRecord (String dbname, int key) {
        
        /* Launch this in a new thread ? */
        /* move this crap to packdb(String dbname, String username, String Password)  */
        try {
            use_db(dbname, "r"); //we are only going to read
            
            RAF.seek(0);  

            File newFile = new File(currentSchema.getDbPath()+"x");

            RandomAccessFile newRAF = new RandomAccessFile (newFile, "rw");

            long h = getNumOfRecs();

            Object [] xRecord = new Object [currentSchema.getNumFields()];

            int xKey = 0;
            int ftype;
            newRAF.writeInt(RAF.readInt()); //Copy the key meta first

            for (int x=0; x < h; x++ ) { //cycle through all records in .db

                xRecord = readRecord(); //read record from .db
                Integer H = (Integer) xRecord[0];
                xKey = H.intValue();
                
                if (xKey != key) { //check key then write
                               // write record to .dbx
                    int rl = xRecord.length;

                        for (int i = 0; i < rl ; i++) {
                             ftype = currentSchema.getFieldType(i);

                            switch(ftype){

                                case 0:
                                case 3:
                                    newRAF.writeInt(((Integer) xRecord[i]));
                                    continue;
                                case 1:
                                    newRAF.writeBytes(padString(((String) xRecord[i]), currentSchema.getFieldSize(i)));
                                    continue;
                                case 2:
                                    newRAF.writeFloat(((Float) xRecord[i]));
                                    continue;
                                case 4:
                                    newRAF.writeBoolean(((Boolean) xRecord[i]));
                                    continue;
                                case 5:
                                    newRAF.writeLong(((Long) xRecord[i]));
                                    continue;
                            }
                       }
                }
       }

    newRAF.close();
    newRAF = null;
    closeDb();
    dbFile.delete();
    File goodDb = new File (currentSchema.getDbPath());
    
    //System.out.println("Proposed rename: "+currentSchema.getDbPath());
    newFile.renameTo(goodDb);

    } catch (Exception e) {

        application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "removeRecord: General Error.", true));

        DV.writeFile("db.err", "removeRecord() error :" +currentSchema.getDbName()+".db" + nl , true ); return false;


    }

    return true;
}


    /** Reads and returns a record from disk of the specified dbname - key */
    public Object [] getRecord (String dbname, int key) {
        if (key == 0) {
            //System.out.println("key == 0 "+dbname+" KEY:"+key);
            application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "getRecord: Zero Key was attempted.", true));
            return null;
        }
        use_db(dbname, "r");  /* CALL FUCKING use_db() FIRST ASSHOLE!!!!! */
        
        //System.out.println("Check number of records: "+dbname+" ~KEY: "+key);
        if (this.getNumOfRecs() == 0) return null;
        //System.out.println("past getnumrecs in getRecord "+dbname+ "KEY: "+key);
        /*
         * enclose this method in a loop based on use_db
         * use_db checks to see if it is locked if not the action is performed
         * if it is locked use_db returns a certain val that can be checked 
         * and logic branched accordingly
         */
        
        if (seekToKey(key)) return readRecord();
	
        return null;

     }
     
    /* Added for performance reasons */
    public Object [] getRecord (String dbname, long pos){  
        
        
        use_db(dbname, "r");
        
        if (seekTo(pos)){
                    
            return readRecord();
            
        }else return null;
        
        
    }
    
    
    /** Saves (insert - update) the record you provide in 'data'.  The first
     * Object [0] must be Integer ZERO for new record or an exsisting key within the
     * database.  This method returns the key used */
    public int saveRecord (String dbname, Object [] data, boolean unique) {

        use_db (dbname);

	if (!dbVerify()) { 
           javax.swing.JOptionPane.showMessageDialog(null, 
                   dbname + " appears Corrupted." + nl +
                   "Try restoring "+dbname+".db from backup.");
            
           return -1;
        }

        if (isRecordValid(data)) {
            if (unique) {
                if (isRecordUnique(data)) {
                    try{
                        return writeRecord(data);
                    }catch (Exception e){

                        
                    status = "error:22:The application had a probem writing a record to " + currentDb + nl +
                    ", this could be a damaged file.";

                    DV.writeFile("db.err", "writeRecord() error :" + nl, true);
                    return -1;

                    }
		}else return 0;  //this means record was specified unique but was not
            }

            try {
                return writeRecord(data);
            }catch (Exception e){

                       
                    application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "saveRecord: Problem writing record.", true));

                    DV.writeFile("db.err", "writeRecord() error :" + nl, true);
                    return -1;


            }

	} else return -1;  //bad data

    }


    /* ---------- ADD-ON PUBLIC METHODS ----------- */
    public ArrayList search (String dbname, int col, String searchText, Boolean substring) {

        use_db(dbname,"r");
	return scanColumn (col, searchText, substring, false);

    }
    
   public ArrayList nSearch (String dbname, int col, int headValue, int tailValue, Boolean substring) {

        use_db(dbname,"r");
	return scanIntColumn (col, headValue, tailValue, false);

    }

    
    public ArrayList nSearch (String dbname, int col, long headValue, long tailValue, Boolean substring) {

        use_db(dbname,"r");
	return scanLongColumn (col, headValue, tailValue, false);

    }

    public ArrayList nSearch (String dbname, int col, float headValue, float tailValue, Boolean substring) {

        use_db(dbname,"r");
	return scanFloatColumn (col, headValue, tailValue, false);

    }

    public ArrayList searchFast (String dbname, int col, String searchText, Boolean substring) {

        use_db(dbname, "r");
	return scanColumn (col, searchText, substring, true);

    }
    
    /*This method override uses the header fro mthe supplied JTable in the
     newly returned tableModel - takes a list of keys*/
    public TableModel createTableModel (String db, ArrayList list, JTable jt) {

        use_db (db, "r");
        int recs = list.size();
        int numFields = currentSchema.getNumFields();

        Object [] [] data = new Object [recs] [numFields];
        String [] headers = new String [numFields];

        Object [] record = new Object [numFields];
         
        int [] l = new int [recs];


        for (int row = 0; row < recs; row++) {
            l[row] = Integer.valueOf((Integer)list.get(row));

        }

        list.clear();
         list = null;

        for (int h = 0; h < headers.length; h++) {
            headers [h] = currentSchema.getFieldName(h);
        }

        //try { RAF.seek(4); } catch (Exception e) {e.printStackTrace();};
        skipDbHeader();
        
        for (int row = 0; row < recs; row++) {

            seekToKey(l[row], true); //if the database passes a needced key  without consuming it; the key will not be found

            record = readRecord ();
            System.arraycopy(record, 0, data[row], 0, numFields);

        }

        if (jt != null){
        sorter = new TableSorter(new DefaultTableModel(data,headers));
        sorter.setTableHeader(jt.getTableHeader());
        data = null;
        return sorter;

        }else {

            return new DefaultTableModel(data, headers);


        }
    }

    
    
    public TableModel createTableModel (String db, ArrayList list, boolean sort) {

        if (list == null) return null;  //do not pass a null value in

        use_db (db, "r");

        int recs = list.size();
        int numFields = currentSchema.getNumFields();

        Object [] [] data = new Object [recs] [numFields];
        String [] headers = new String [numFields];

        Object [] record = new Object [numFields];
         int s = list.size();


        int [] l = new int [s];


        for (int row = 0; row < s; row++) {  //put all the keys into an 
            l[row] = Integer.valueOf((Integer)list.get(row));

        }

        list.clear();
         list = null;

        for (int h = 0; h < headers.length; h++) {
            headers [h] = currentSchema.getFieldName(h);
        }


        skipDbHeader();
        
        for (int row = 0; row < s; row++) {


            seekToKey(l[row], true);


            record = readRecord ();
            System.arraycopy(record, 0, data[row], 0, numFields);

        }
        if (sort){

            sorter = new TableSorter(new DefaultTableModel(data, headers));
            //data = null;
            return sorter;


        }else {

            return new DefaultTableModel(data,headers){
             public Class getColumnClass(int column) {
                return DV.idObject(this.getValueAt(0,column));
            }
         };
        }

    }

        /** Do not pass in a null ArrayList */
        public TableModel createTableModelFast (String db, ArrayList list, boolean sort) {

        if (list == null) return null;  //do not pass a null value in!!!!

        use_db (db, "r");

        int recs = list.size();
        int numFields = currentSchema.getNumFields();

        Object [] [] data = new Object [recs] [numFields];
        String [] headers = new String [numFields];

        Object [] record = new Object [numFields];
         int s = list.size();


        long [] l = new long [s];


        for (int row = 0; row < s; row++) {  //put all the keys into an 
            l[row] = Long.valueOf((Long)list.get(row));

        }

        list.clear();
         list = null;

        for (int h = 0; h < headers.length; h++) {
            headers [h] = currentSchema.getFieldName(h);
        }

        skipDbHeader();
        

        for (int row = 0; row < s; row++) {
           seekTo(l[row]);

            record = readRecord ();
            System.arraycopy(record, 0, data[row], 0, numFields);

        }
        if (sort){

            sorter = new TableSorter(new DefaultTableModel(data, headers));
            //data = null;
            return sorter;

        }else {

            return new DefaultTableModel(data,headers){
             public Class getColumnClass(int column) {  //this little area makes sure the proper renderer is used
                return DV.idObject(this.getValueAt(0,column));
            }
         };
        }

    }

    public TableModel createTableModel (String db, JTable jt) {

        use_db (db, "r");
        int recs = (int) getNumOfRecs();
        if (recs < 1) return (TableModel) new DefaultTableModel();
        int numFields = currentSchema.getNumFields();
        
        // build TableModel with data and headers

        Object [] [] data = new Object [recs] [numFields];
        String [] headers = new String [numFields];

        Object [] record = new Object [numFields];

        for (int h = 0; h < headers.length; h++) {
            headers [h] = currentSchema.getFieldName(h);
        }

        skipDbHeader();

        for (int row = 0; row < recs; row++) {
                record = readRecord ();
                System.arraycopy(record, 0, data[row], 0, numFields);

            }

        sorter = new TableSorter(new DefaultTableModel(data,headers));
        
        if (jt == null)  sorter.setTableHeader(null);
            else sorter.setTableHeader(jt.getTableHeader());
        
        data = null;
        return sorter;
    }


    public TableModel createTableModel (String db) {

        use_db (db, "r");
        int recs = (int) getNumOfRecs();
        int numFields = currentSchema.getNumFields();
          //System.out.println("Num of records: "+recs+" number of Firlds "+numFields);
        // build TableModel with data and headers

        Object [] [] data = new Object [recs] [numFields];
        String [] headers = new String [numFields];

        Object [] record = new Object [numFields];

        for (int h = 0; h < headers.length; h++) {
            headers [h] = currentSchema.getFieldName(h);
        }

        skipDbHeader();

        for (int row = 0; row < recs; row++) {
                record = readRecord ();
            System.arraycopy(record, 0, data[row], 0, numFields);
            }
        return new DefaultTableModel(data,headers);
    }

    /** Checks to see if the desired db is "loaded" */
    public boolean isDbAvailable(String dbname) {
        String table = dbname.toLowerCase();
        for (int i = 0; i < schList.size(); i++) {
            DbSchema s = (DbSchema) schList.get(i);
            if (s.getDbName().toLowerCase().equals(table)) return true;
	}

	return false;
    }

public int csvExport (String dbname, File f, int [] fields) {
    use_db (dbname, "r");

    de.schlichtherle.io.File csv = new de.schlichtherle.io.File(f);
    
    int recs = (int) getNumOfRecs();
    
    
    int numFields = currentSchema.getNumFields();
    int numProcessed = 0;

    StringBuilder sb = new StringBuilder();
    
    for (int i = 0; i < numFields; i++ ){
        
        if (i < numFields-1){
            
            sb.append(currentSchema.getFieldName(i)+',');
            
        }else sb.append(currentSchema.getFieldName(i));
        
        
    }
    
    Object [] record = new Object [numFields];

    //File csv = new File (destPathFile);

    PrintWriter out= null;
    
    try {

            skipDbHeader();
            out = new PrintWriter (new BufferedWriter(new de.schlichtherle.io.FileWriter (csv)));

            out.println(sb.toString());  //header
            
                for (int row = 0; row < recs; row++) {

                    record = readRecord ();

                    out.println(toComma(record, fields));
                    numProcessed++;
                    //pd.progress(numProcessed);
            }
            
    }catch (Exception e) {

        e.printStackTrace();
        application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "csvExport: General Error.", true));
        DV.writeFile("db.err", "cvsExport() error :" + System.getProperty("line.separator") , true ) ;
        javax.swing.JOptionPane.showMessageDialog(null, "There was a problem exporting; try using a different folder.");
    }
     finally {
         
         if (out != null) out.close();
         
     
         //pd.close();
     }

    return numProcessed;

}


public int csvExport (String dbname, File f, int [] fields, ProgressDialogInterface pd) {
    
    use_db (dbname, "r");

    de.schlichtherle.io.File csv = new de.schlichtherle.io.File(f);
    
    int recs = (int) getNumOfRecs();
    
    pd.setBarMax(recs);
    pd.updateBar(0);
    
    int numFields = currentSchema.getNumFields();
    int numProcessed = 0;

    StringBuilder sb = new StringBuilder();
   
    /* Build export header */ 
    
    for (int i = 0; i < numFields; i++ ){
        
        if (i < numFields-1){
            
            sb.append(currentSchema.getFieldName(i)+',');
            
        }else sb.append(currentSchema.getFieldName(i));
        
    }
    
    Object [] record = new Object [numFields];
    
    PrintWriter out= null;
    
    try {

            skipDbHeader();
            out = new PrintWriter (new BufferedWriter(new de.schlichtherle.io.FileWriter (csv)));

            out.println(sb.toString());  //header
            
                for (int row = 0; row < recs; row++) {

                    record = readRecord ();

                    out.println(toComma(record, fields));
                    numProcessed++;
                    //System.out.println("ARE WE GTTING HERE?!!");
                    
                    pd.updateBar(row);
                    //pd.progress(numProcessed);
            }
            
    }catch (Exception e) {
        application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "csvExport: General Error (PD)", true));
        DV.writeFile("db.err", "cvsExport() error :" + System.getProperty("line.separator") , true ) ; 
        javax.swing.JOptionPane.showMessageDialog(null, "There was a problem trying to export; try using a different folder?");
     
         
    }
     finally {
         
         if (out != null) out.close();
         
         
     }

    
     pd.close();
     
     return numProcessed;
    
    
}


/** If the return value is over 0 it means an error (mismatch) has occured & denotes the line number of error*/
public int csvImport (String dbname, File f, boolean skipHeader, int [] toFields, boolean overwrite){

    String line = new String();
    use_db(dbname);
    int count = 0;
   
    de.schlichtherle.io.File inputFile = new de.schlichtherle.io.File(f);
    
    BufferedReader in=null;
    try {
                in = new BufferedReader(new de.schlichtherle.io.FileReader(inputFile));
                
                if (skipHeader) in.readLine();
                
                line = in.readLine();
                                                              
                Object [] tmp;

                while (line != null)    {

                    count++;
                   
                    tmp = fromComma (line, toFields) ;  //power method

                    if (tmp == null){  //we tried to put data in the wrong "hole"
                        
                        in.close();
                        in = null;    //clean up
                        line = null;
                        return count;  //didn't match import fields properly (tried to put a Float into Integer fields, etc..)
                        
                    }
                    
                    int t = 0;
                                        
                    /* Cycle through tmp to make sure every field 
                     is initialized to at least blank values */
                    for (int i = 1; i < tmp.length; i++){
                        
                        t = currentSchema.getFieldType(i);
                        
                        if (t == 1) if (!(tmp[i] instanceof String)){
                            tmp[i] = new String ("");                            
                        }
                        if (t == 2) if (!(tmp[i] instanceof Float)){
                            tmp[i] = new Float(0.00f);                            
                        }
                        if (t == 3) if (!(tmp[i] instanceof Integer)){
                            tmp[i] = new Integer(0);
                        }
                        if (t == 4) if (!(tmp[i] instanceof Boolean)){
                            tmp[i] = new Boolean(false);
                        }
                        
                    }
                        
                    if (!overwrite) tmp[0] = new Integer(0);
                        saveRecord (dbname, tmp, false);

                                      
                   line = in.readLine();
                   //processed =+ line.length();
                //pd.setProgress(processed);
                
                }


    }catch (Exception e) {
        application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "csvImport: General Error.", true));
        e.printStackTrace();
        
    }finally {try {
                     
                if (in != null) in.close();
                in = null;
                line = null;
                //pd.close();
                
            } catch (IOException ex) {
                application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "csvImport: Error closing input file.", true));
                ex.printStackTrace();
            }}
        
    return 0;
}

public int csvImport (String dbname, File f, boolean skipHeader, int [] toFields, ProgressDialogInterface id, boolean overwrite){
    return csvImport(dbname, f, skipHeader, toFields, id, overwrite, false);
}
public int csvImport (String dbname, File f, boolean skipHeader, int [] toFields, ProgressDialogInterface id, boolean overwrite, boolean defbool){

    
    String line = new String();
    use_db(dbname);
    int count = 0;

    int size = 0;
        
    de.schlichtherle.io.File inputFile = new de.schlichtherle.io.File(f);
    
   
    id.setBarMax((int)f.length());
    
    id.updateBar(0);
    
    BufferedReader in=null;
    try {
                in = new BufferedReader(new de.schlichtherle.io.FileReader(inputFile));
                
                if (skipHeader){
                    
                    line = in.readLine();
                    size += line.length();
                }
                
                line = in.readLine();
                if (line == null) return 0;
                
                    size += line.length();
                    
                    id.updateBar(size);
                    
                Object [] tmp;

                while (line != null)    {

                    count++;
                   
                    //System.out.println(line);
                    
                    tmp = fromComma (line, toFields) ;  //power method

                    if (tmp == null){  //we tried to put data in the wrong "hole"
                        
                        in.close();
                        in = null;    //clean up
                        line = null;
                        return count;  //didn't match import fields properly (tried to put a Float into Integer fields, etc..)
                        
                    }
                    
                    int t = 0;
                                        
                    /* Cycle through tmp to make sure every field 
                     is initialized to at least blank values */
                    for (int i = 1; i < tmp.length; i++){
                        
                        t = currentSchema.getFieldType(i);
                        
                        if (t == 1) if (!(tmp[i] instanceof String)){
                            tmp[i] = new String ("");                            
                        }
                        if (t == 2) if (!(tmp[i] instanceof Float)){
                            tmp[i] = new Float(0.00f);                            
                        }
                        if (t == 3) if (!(tmp[i] instanceof Integer)){
                            tmp[i] = new Integer(0);
                        }
                        if (t == 4) if (!(tmp[i] instanceof Boolean)){
                            tmp[i] = new Boolean(defbool);
                        }
                        if (t == 5) if (!(tmp[i] instanceof Long)){
                            tmp[i] = new Long(0);
                        }
                        
                    }
                        
                    if (!overwrite) tmp[0] = new Integer(0);
                    //DV.expose(tmp);
                    saveRecord (dbname, tmp, false);

                                      
                   line = in.readLine();
                   if (line != null) size += line.length();
                   id.updateBar(size);
                   
                }
                

    }catch (Exception e) {
        application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "csvImport: General Error (PD)", true));
        e.printStackTrace();
        
    }finally {try {
                     
                if (in != null) in.close();
                in = null;
                line = null;
                //pd.close();
                id.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }}
        
    return 0;
}




/* ----------------------- PRIVATE INTERNAL ENGINE METHODS --------------------*/
/* These methods all work on the currentDb and currentSchema  */

    /* Data and Environment Control Methods */

    private void closeDb()	{

        try {

            if (RAF != null) { RAF.close(); RAF = null;}
            nowdb = "_";

        }catch (Exception e) {

            javax.swing.JOptionPane.showMessageDialog(null,
                   "There was a problem closing "+currentDb + nl +
                   "Contact technical support (software@datavirtue.com) for help.");

           application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "closeDb: Problem closing "+currentDb, true));

            DV.writeFile("db.err", "closeDb() error :" + nl, true);  return;

        }

        return;

    }

    private void use_db (String db_name){
       
        use_db(db_name, "rw");  //default read/write mode
        
    }
    private void use_db (String db_name, String mode)	 {


		/* selects db to perform operations on */
		/* initializes currentDb and currentSchema */
		/* Setup of RAF to open currentDb  */

        /* TODO: Incorect behavior for use with changePath  */
        //if (db_name.equals(nowdb) && mode.equals(current_mode)) return;	// <---- Get out of here if trying to use same db

        if (isDbAvailable(db_name)) {

            try {

                closeDb();
                currentSchema = getSchemaObject(db_name);

                currentDb = currentSchema.getDbPath();
                if (debug) System.out.println("use_db: currentDb_Path: "+ currentDb);
                dbFile = new File(currentDb);
                /* Make sure dbFile exists, if it doesn't create the file and set/write the first record number */
                 
                if (!dbFile.exists() || dbFile.length() == 0){
                    
                    dbFile.createNewFile();
                    RandomAccessFile rf = new RandomAccessFile(dbFile, "rw");
                    rf.writeInt(new Integer(1));
                    rf.close();
                    rf = null;
                    
                    if (debug) System.out.println("Created "+dbFile.getPath()+" Key 1");
                    
                }
                RAF = new RandomAccessFile (dbFile, mode);
                
                current_mode = mode;  // set current file access mode
                
                nowdb = currentSchema.getDbName();
             
            } catch (Exception e) {

               application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "use_db: Error accessing "+db_name, true));

                DV.writeFile("db.err", "use_db() error :"+ currentSchema.getDbPath() + nl, true);
                e.printStackTrace();

            }

        }else { DV.writeFile("db.err",db_name + " is not available!" + nl, true); return; }

	return;
       
     }

      
    /** Moves the file pointer to the first data after the meta */
    private void skipDbHeader (){

        try {
            RAF.seek(4);
        }catch (Exception e) {
        javax.swing.JOptionPane.showMessageDialog(null,
                   "There was a problem while trying to traverse "+currentDb + nl +
                   "Contact technical support (software@datavirtue.com) for help.");

            application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "skipDbHeader: Problem travering "+currentDb, true));
                e.printStackTrace();
            DV.writeFile("db.err", "problem skipping header :" +currentDb+ System.getProperty("line.separator"), true);   }

    }


    private String toComma (Object [] obj){

        StringBuilder temp = new StringBuilder();

        for (int i=0; i < obj.length; i++ ) {  //Start at one (1) to skip key

             if (DV.whatIsIt(obj [i]) == 1) {  // string

                temp.append((String) obj [i]);
            }
             
             if (DV.whatIsIt(obj [i]) == 4) {  // bool

                temp.append(new Boolean((Boolean) obj [i]).toString());
                                
            }
             
             if (DV.whatIsIt(obj [i]) == 2) {  // integer

                temp.append(new Integer((Integer) obj [i]).toString());
                                
            }
             
             if (DV.whatIsIt(obj [i]) == 3) {  // float
                temp.append(new Float((Float) obj [i]).toString());
                                
            }
             

            if (obj.length > 2  && i != obj.length-1) temp.append(",");
        }


        return temp.toString();
    }

    /**
     With this method you can leave out, repeat, reorder any "field" you want from an Object [] ("record")
     */
    private String toComma (Object [] obj, int [] fields){

        StringBuilder temp = new StringBuilder();

        boolean hit = false;
        String tmp;
        
        for (int i=0; i < fields.length; i++ ) {  //Start at one (1) to skip key

                      
            if (fields[i] == 0 ) {
                
                temp.append(new Integer((Integer) obj[0]).toString());
                hit = true;
            } 
            
            if (!hit && DV.whatIsIt(obj [fields[i]]) == 1) {  // string

                tmp = (String) obj [fields[i]];
                                
                temp.append( tmp.replace(',', '.').replace(nl,"|") );
                hit = true;
            }
             
             if (!hit && DV.whatIsIt(obj [fields[i]]) == 4) {  // bool

                temp.append(new Boolean((Boolean) obj [fields[i]]).toString());
                hit = true;

                
            }
             
              if (!hit && DV.whatIsIt(obj [fields[i]]) == 2) {  // integer

                temp.append(new Integer((Integer) obj[fields[i]]).toString());
                hit = true;                
            }
             
             if (!hit && DV.whatIsIt(obj [fields[i]]) == 3) {  // float
                temp.append(new Float((Float) obj[fields[i]]).toString());
                hit = true;
                                
            }

            if (!hit && DV.whatIsIt(obj [fields[i]]) == 5) {  // long (date)
                temp.append(new Long((Long) obj[fields[i]]).toString());
                hit = true;

            }

            hit = false;
            
            if (fields.length > 1  && i != fields.length-1) temp.append(",");
        }


        return temp.toString();
    }

    
    

    private Object [] fromComma (String csv)  {

        int numfields = currentSchema.getNumFields();

        Object [] record = new Object [numfields];

        String [] stringRecord = new String [numfields-1];  //minu one for key, not any more
        
        char [] line = csv.toCharArray();
        //System.out.println(line.length);

        StringBuilder temp = new StringBuilder (line.length);

        /* This loop grabs each comma separated value */
            int index = 0;
        for (int i = 0; i < stringRecord.length; i++){

                do {
                    
                    if (index >= line.length) break;
                    if (line[index] == ',' ) break;
                      if (line[index] != '"' ) temp.append(line[index]);  //this took a while to straighten out
                      index++;


          }
                while (  index < line.length && line[index] != ',' );


          temp.trimToSize();
          stringRecord [i] = temp.toString();

            temp.delete(0, temp.length());
            index++;

    }

        //convert strRecord to Object []
        int type=0;
        record [0] = new Integer(0);

        for (int j = 1; j <= stringRecord.length; j++){

            type = currentSchema.getFieldType(j);

            if (type == 0 || type == 3) record[j] = new Integer (Integer.parseInt((String)stringRecord[j-1]));
            if (type == 4) {
                
               record[j] = new Boolean (Boolean.parseBoolean((String) stringRecord[j-1]));
            }
            if (type == 2) record[j] = new Float (Float.parseFloat((String) stringRecord[j-1]));
            if (type == 1) record[j] = new String (stringRecord[j-1]);
        }


        //Free Memory
        temp = null;
        stringRecord = null;
        line = null;

        return record;


    }
    
        private synchronized Object [] fromComma (String csv, int [] toFields)  {

        if (debug) System.out.println("fromComma, string: "+csv);

        int numfields = currentSchema.getNumFields();

        Object [] record = new Object [numfields];

        String [] stringRecord = new String [numfields];
        
        char [] line = csv.toCharArray();

        if (debug){
        System.out.println(line.length);
        System.out.println("fromComma, num Fields: "+numfields);
        }

        /* temp holds each field between the commas as the line is processed */
        StringBuilder temp = new StringBuilder (line.length);

        /* This loop grabs each comma separated value */
            int index = 0;
        for (int i = 0; i < stringRecord.length; i++){
                //System.out.println("fromComma, LineLoop: "+ i);
                do {                    
                    if (index >= line.length) break;  //<--- Rare bug without this line
                    if (line[index] == ',' ) break;
                        if (line[index] == '"' && index+1 >= line.length && line[index+1] != ','){
                            temp.append(line[index]);
                            if (index >= line.length) break;  //<--- Rare bug without this line
                            index++;
                            continue;
                        }else {
                            if (line[index] != '"') temp.append(line[index]);  //this took a while to straighten out
                            if (index >= line.length) break;  //<--- Rare bug without this line
                            index++;  //no matter what index is incr
                        }
                        
                        //System.out.println("fromComma: index: "+index);

          }while (  index < line.length && line[index] != ',' );


          temp.trimToSize();

          if (debug) System.out.println(temp.toString());

          stringRecord [i] = temp.toString().replace("|", System.getProperty("line.separator"));

            temp.delete(0, temp.length());
            if (index < line.length) index++;

    }            
            //System.out.println("string record length "+stringRecord.length);

            //System.out.println("to Fields length"+toFields.length);
        //convert strRecord to Object []
        int type=0;
        

        for (int j = 0; j < toFields.length; j++){

            type = currentSchema.getFieldType(j);
            if (type == 1) record[j] = new String (stringRecord[toFields[j]]);
            else { 
                
                try {

                if (type == 0 || type == 3) record[j] = new Integer (DV.parseInt((String)stringRecord[toFields[j]]));
                if (type == 4) {
                    
                   record[j] = new Boolean (DV.parseBool((String) stringRecord[toFields[j]], false));
                }
                if (type == 2){
                    if (DV.validFloatString((String) stringRecord[toFields[j]])){
                        record[j] = new Float (Float.parseFloat((String) stringRecord[toFields[j]]));
                    }else {
                        record[j] = new Float(0.0);
                    }
                    
                }
                
                //Nevitium Version 1.5
                /*Try to grab a long otherwise grab string and convert to long date format  */
                if (type == 5){
                    
                    try {
                        
                        record[j] = new Long (Long.parseLong((String)stringRecord[toFields[j]]));
                        
                    }catch(NumberFormatException nfe){
                        application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "fromComma: Error parsing dates for an import.", true));
                        String dateString = new String (stringRecord[toFields[j]]);
                        long date = DV.stringToDate(dateString);
                        if (date == 0) {

                            if (debug){
                            System.out.println("Error parsing date in:");
                            System.out.println(csv);
                            }
                            
                        }
                        record[j] = new Long (date);
                        
                    }
                }
                
            } catch (NumberFormatException ex) {
                application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "fromComma: Error converting numbers for an import.", true));
                temp = null;
                stringRecord = null;
                line = null;
                return null;
                
            }            
            }            
        }
        //Free Memory
        temp = null;
        stringRecord = null;
        line = null;

        if (record[0] != null && record[0] instanceof Integer ) return record;
        else {
            
            record [0] = new Integer(0);
            return record;
            
        }


    }


    public String [] getFieldNames (String dbname) {

        use_db(dbname);
        
        int a = currentSchema.getNumFields();
        String [] names = new String [a];

        for (int j = 0; j < names.length;  j++) {

            names [j] = currentSchema.getFieldName (j);

        }

        return names;
    }

    public int getFieldSize (String dbname, int col) {

        use_db(dbname);

     return currentSchema.getFieldSize(col); /*FIXME*/

    }


    private Object [] readRecord() {

        int numfields = currentSchema.getNumFields();

	int type = 0;

	int fs = 0;  //fieldsize

	Object [] data = new Object [numfields];

	try {

            for (int i = 0; i < numfields; i++)  {

                type = currentSchema.getFieldType(i);


                switch(type){

                    case 0:
                    case 3:
                       data[i] = new Integer(RAF.readInt());
                       break;
                    case 1:
                        fs = currentSchema.getFieldSize(i);
                        data[i] = readStr(fs);
                        break;
                    case 2:
                       data[i] = new Float(RAF.readFloat());
                       break;
                    case 5:
                       data[i] = new Long(RAF.readLong());
                       break;
                    case 4:
                       data[i] = RAF.readBoolean();
                       break;
                    
                }

            }

	} catch (Exception e) {

            
            application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "readRecord: Possible data corruption. "+currentDb, true));

            DV.writeFile("db.err", "readRecord :" + currentSchema.getDbPath()+ System.getProperty("line.separator"), true);
            e.printStackTrace();
        }

        /*System.out.println("-- readRecord() --");
        DV.expose(data);*/
        return data;
    }

    private int writeRecord (Object [] data) throws Exception{

        Integer K = (Integer) data[0];
        int key = K.intValue();
                
        try {
            if (key == 0) {

                data[0] = new Integer(currentSchema.nextKey(RAF,true));
                
                if (debug) System.out.println("schema reported next key: "+(Integer)data[0] );
                
		RAF.seek(dbFile.length()); /* Move to EOF */
		K = (Integer) data[0];
                key = K.intValue();

            }else { 
                
                if (debug) System.out.println("Recording record with key: "+key );
                
                boolean keyFound = seekToKey(key);  

                if (!keyFound) throw new Exception();
            }

            int nf = data.length;


            for (int i = 0; i < nf ; i++) {

		int ftype = currentSchema.getFieldType(i);

                /* STRING */
		if (ftype == 1) {

                    String s = (String) data[i];
                    
                    //byte[] str = s.getBytes(); /*CHANGE*/

                    //int fsize = currentSchema.getFieldSize(i) / 2 ;
                    int fsize = currentSchema.getFieldSize(i); /*CHANGE*/
                    if (unicode)  fsize = fsize / 2;
                    
                    //for (int c = 0; c < fsize ; c++) { //this was removed do to added features in RAFs

                      //  if (c < s.length()) RAF.writeChar(s.charAt(c)); 
                      //  else RAF.writeChar(0);
                        
                    if (s.length() > fsize) s = s.substring(0, fsize);
                    if (s.length() < fsize) s = padString(s, fsize);
                    
                        if (unicode){
                            
                            RAF.writeChars(s);
                        }else{
                            RAF.writeBytes(s);
                        }
                        
                        
                   // }

		}

			/* Integer - KEY */

		if (ftype == 0 || ftype == 3) {

                    Integer I = (Integer) data[i];
                    RAF.writeInt(I.intValue());

		}

                        /* MONEY $$ */
		if (ftype == 2) { /* NUM */

                    Float f = (Float) data[i];
                    RAF.writeFloat(f.floatValue());
		}


                    /* YES/NO */
		if (ftype == 4) { /* YESNO */

                    Boolean b = (Boolean) data[i];
                    RAF.writeBoolean(b.booleanValue());

        	}
                
                if (ftype == 5) { /* LONG */

                    Long l = (Long) data[i];
                    RAF.writeLong(l.longValue());
                    
		}
                

            }

        closeDb();

        } catch (Exception e) {

            
            application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "writeRecord: Problem writing record for "+currentDb, true));
            e.printStackTrace();
            DV.writeFile("db.err", "writeRecord :" + currentDb+ nl, true); return -1;
            
        }

	return key;

    }

    private String readStr(int size)  {
        
	byte [] str = new byte[size];
        char [] chr = new char[size/2];
               
	try {                
            if (unicode) {
                //RAF.read(str);
                for (int i = 0; i < chr.length; i++){
                    chr[i] = RAF.readChar();
                }
                return new String(chr).trim();
                
            }else {
                RAF.read(str);
            }
            
            
	} catch (Exception e) {

                application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "readStr: Possible data corruption in "+currentDb, true));
                e.printStackTrace();
            }
           
	return new String(str).trim();

    }

    private boolean seekTo (long pos){
        try {
        
        
            RAF.seek(pos);
        } catch (IOException ex) {
            javax.swing.JOptionPane.showMessageDialog(null,
                   "There was a seek problem accessing "+currentDb + nl +
                   "Contact technical support (software@datavirtue.com) for help.");

            application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "seekTo: Problem moving through "+currentDb, true));
                ex.printStackTrace();
            return false;
            
        }
                
        return true;
        
    }
    
    /** Simple method moves the file pointer to the proper record.  It starts from the begining each time. */
    private boolean seekToKey (int key) {

        try {
            
            skipDbHeader(); //moves file pointer up 4 bytes
            long numrecs = getNumOfRecs();
            if (numrecs == 0) return false;
            int recsize = currentSchema.getRecordSize();
            int jump = recsize - 4;  //used after int (key) read
            int k;  //scratch space

            // Divide and process.
            int multiple = 0;
            if (optimized){
                if (numrecs > 1000 && key > (numrecs / 2)) multiple = (int)(numrecs / 2);
                if (numrecs > 9000 && key > (numrecs / 4)){
                
                    if ( key >= (numrecs /2) )multiple = ((int)(numrecs / 2));
                    //System.out.println("multiple 9k .50" + multiple);
                    if ( key >= (numrecs /4) * 3 )multiple = ((int)(numrecs / 4)*3);
                    //System.out.println("multiple 9k .75" + multiple);
                }
            }
//                System.out.println("numrecs" + numrecs);
//                System.out.println("recsize" + recsize);
//                System.out.println("multiple" + multiple);
                long start = (long)(multiple * recsize);
                long chk = start % recsize;
                if (chk != 0) System.out.println("Problems with search jump calc. ");;
                start += 4;
                //System.out.println("seekpoint" + start);
                RAF.seek(start);
                //int c = 0;

                for (long i = multiple; i <= numrecs; i++)  {
                    //System.out.println("seekToKey iterations: "+c);
                    k = RAF.readInt();
                    if (k == key) { RAF.seek(RAF.getFilePointer() - 4); return true;  }
                    else RAF.seek(RAF.getFilePointer() + jump);
                    //c++;
                }
                
           
	} catch (Exception e) {

            
            application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "seekToKey: An error occured trying to move to a location in "+currentDb, true));
                e.printStackTrace();
            DV.writeFile("db.err", "seekToKey :" + currentDb+ System.getProperty("line.separator"), true);
            return false;
            }

	return false;
    }



    /** Use this to prevent a file pointer reset every time you call seekToKey() in a loop.  */
    private boolean seekToKey (int key, boolean reset) {

        try {
            if (reset) {
            
            skipDbHeader();
            }

            long numrecs = getNumOfRecs();
            int recsize = currentSchema.getRecordSize();
            int jump = recsize - 4;
            int k = 0;

            for (int i = 0; i < numrecs; i++)  {
                
                try {
                    
                k = RAF.readInt();
                
                } catch (Exception e) { RAF.seek(4); }
                
		if (k == key) { RAF.seek(RAF.getFilePointer() - 4); return true;  }
                    else RAF.seek(RAF.getFilePointer() + jump);

            }

	} catch (Exception e) {

            application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "seekToKey(fast): There was a problem accessing "+currentDb, true));

            DV.writeFile("db.err", "seekToKey(r) :" + currentDb+ nl, true); return false;

        }

	return false;
    }

    private boolean isRecordUnique(Object [] data) {

        /* NOT Working */
	/* Search db for equal record */

        /* Application level search for like occurances should be conducted if ness.*/
        return false;

    }

    /** Takes a record and checks it aginst the schema to make sure field types match  */
    private boolean isRecordValid (Object [] data) {

        return currentSchema.isDataCorrect(data);

    }

    private boolean dbVerify () {

        long totalsize = dbFile.length()-4; //-4 remove nextkey bytes from equation

        if (debug) System.out.println("dbVERIFY totalsize:"+totalsize);

	if (totalsize == 4) return true;  //empty but setup with nextkey

	int rowsize = currentSchema.getRecordSize() ;

        if (debug) System.out.println("dbVERIFY RecordSize:"+rowsize);

	long remainder = totalsize % rowsize;

        if (debug) System.out.println("dbVERIFY Remainder:"+remainder);

	if (remainder == 0) return true;

        application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "dbVerify: A potentially serious error has occurred in "+currentDb, true));
        
        DV.writeFile("db.err", "db is corrupt err2:" + currentDb +" totalsize:"+ totalsize +" rowsize:"+ rowsize +" remainder:"+ remainder+':' + nl, true);

        return false;  /* if the db is "off" return false */

     }

    private long getNumOfRecs () {
        if (debug) System.err.println("getNumOfRecs called: ");
        if (dbVerify()) return dbFile.length() / currentSchema.getRecordSize();
	return -1;   //error

    }

	/* ---------- ADD-ON METHODS ----------- */

    /**regex and targetCols must be the same length.  Supply a regex for each column you want to search, 
     * then assign a target col for each regex by placing a column number at the same index of targetCols as the regex String.
       Supply the String type "AND" or "&" or "" (leave blank) when you require all the regexes to return true.
       Supply the String type "OR" or "|" if only one needs to be true.*/
    public ArrayList regexSearch (String dbname, String [] regex, int [] targetCols, String type){
        
        boolean and = true;
        type = type.trim();
                
        if (type.equalsIgnoreCase("and") || type.equals("") || type.equals("&&") || type.equals("&")) and = true;
            else and = false;  //OR
        
        return regexCols(dbname, regex, targetCols, and);
        
    }
    
    private ArrayList regexCols (String dbname, String [] regex, int [] targetCols, boolean and) {
        
        use_db (dbname, "r");

        ArrayList al = new ArrayList();
    
        Pattern [] p = new Pattern [targetCols.length];
        Matcher [] m = new Matcher[targetCols.length];
        
        for (int i = 0; i < p.length; i++){
          
                try {
                
                    p[i] = Pattern.compile(regex[i]);
                
                }catch (PatternSyntaxException se){
                    
                    System.out.println("Pattern "+ (i+1) + " is invalid.");
                    se.printStackTrace();
                    return null;
                }
            
        }
        
       int recs = (int) getNumOfRecs();
       Object [] record;
       String [] strRec = new String [targetCols.length];
        
       skipDbHeader();
        
       boolean miss = false;
         boolean match = false;
         
       for (int r = 0; r < recs; r++){  //big loop
                
           record = readRecord ();
           
           /*Convert the whole record to strings  */
           for (int x = 0; x < targetCols.length; x++){
               
               strRec[x] = DV.convertToString(record[targetCols[x]]);
               
           }
           
           for (int i = 0; i < m.length; i++){
            
               m[i] = p[i].matcher(strRec[i]);
            
           }
           
           miss = false;
           match = false;
           
           for (int i = 0; i < m.length; i++){
                          
               if (!m[i].matches()) miss = true;
               else match = true;
           }
           
           if (!and && match) al.add((Integer)record[0]) ;
           else if (and && !miss) al.add((Integer)record[0]);
                   
       }
            
       al.trimToSize();
       
       return al;
        
    }
    
    
    /** Returns the key(s) of the records found to contain 'text'
        in the specified column  
     
     Need to create a a new mthod to pick records from a certain date range.
     *Currently allows the search for a single date.
     *
     */
    private ArrayList scanColumn (int column, String txt, boolean substring, boolean fast)  {
        
        //if fast return positions instead of keys
        if (txt.equals("")) return null;

        ArrayList results = new ArrayList ();
        
        String text = "";
        //new
        int x = 0;
        float flt = 0f;
        boolean boo = true;
        
        String date="";
        
        try {

		/* This method takes a string and compares it to the value
                   obtained from the specified column.  Basically all
                   values from any column are converted to a string
                   for comparison.

                 This is a high-performance search method; it basically skips
                 through the disk touching only the data needed for comparison
                 
                 
                 ><><><>< Need to make overridden methods for this that except all data types.
                            To eliminate conversion overhead.
                 
                 */


            DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT);
            long numrecs = getNumOfRecs();

            int numfields = currentSchema.getNumFields();

            int fieldsize = currentSchema.getFieldSize(column);
            
            int type = currentSchema.getFieldType(column);
            
            
            if (type == 1) text = txt.trim().toUpperCase();  //new
            if (type == 2 ) flt = Float.parseFloat(txt);
            if (type == 3) x = Integer.parseInt(txt);
            if (type == 4) boo = Boolean.parseBoolean(txt);
            if (type == 5) date = txt; //Version 1.5
            // calculate bytes before and after search field for each record

                        
            int bytesbefore = 0;

            for (int i = 0; i < column; i++) {
                bytesbefore += currentSchema.getFieldSize(i);
            }

            int bytesbetween = bytesbefore - 4;

            int bytesafter =0;

            if (column < numfields-1) {

                for (int i = column+1; i < numfields; i++) {
                    bytesafter += currentSchema.getFieldSize(i);
                }
            }

            int k = 0;
            Float f;
            Boolean b;
            String tmp;
            Integer z;

            Long l; //Version 1.5

            skipDbHeader();

            for (int i = 0; i < numrecs; i++)  {

		k = RAF.readInt();  //read key - advance 4 bytes
		RAF.seek(RAF.getFilePointer() + bytesbetween); //advance to search field
		//z = new Integer (k);

                if (type == 1) {

                    tmp = readStr(fieldsize).trim().toUpperCase(); //read compare return

                    if (substring) {

                        if (tmp.contains(text)){
                            
                           if (fast){
                            
                               long rv = RAF.getFilePointer() - bytesbetween - 4 - fieldsize;
                               
                            results.add(rv);
                           
                           }else results.add(k);
                           
                        }

                    }else if (tmp.equals(text)){
                        
                        if (fast) {
                            
                            long rv = RAF.getFilePointer() - bytesbetween - 4 - fieldsize;
                            results.add(rv);
                            
                        }else results.add(k);
                    }
                }

                if (type == 2) { //float

                    f = (Float) RAF.readFloat();
                    //tmp = f.toString().toUpperCase();
                    //if (tmp.equals(text)) results.add(k);
                   if (f == flt){
                        
                        if (fast){
                            
                            long rv = RAF.getFilePointer() - bytesbetween - 4 - fieldsize;
                            results.add(rv);
                            
                        }else {
                            
                            results.add(k);
                            
                        }
                   }
                }

                if (type == 3) { //int

                    z = (Integer) RAF.readInt();
                    //tmp = z.toString().toUpperCase();
                    //if (tmp.equals(text)) results.add(k);
                    if (z == x) {
                        
                        if (fast){
                            
                            long rv = RAF.getFilePointer() - bytesbetween - 4 - fieldsize;
                            results.add(rv);
                            
                        }else {
                            
                            results.add(k);
                            
                        }
                    }
                }

                if (type == 4) { //boolean

                    b = (Boolean) RAF.readBoolean();
                    //tmp = b.toString().toUpperCase();
                    //if (tmp.equals(text)) results.add(k);
                    if (boo == b){
                        if (fast){
                            
                            long rv = RAF.getFilePointer() - bytesbetween - 4 - fieldsize;
                            results.add(rv);
                            
                        }else {
                        
                            results.add(k);
                            
                        }
                    }
                }
                
                //Version 1.5
                if (type == 5) { //Long

                    //l = (Long) RAF.readLong();
                    l = RAF.readLong();
                   if (debug) System.out.println("scan col: "+l);
                   
                   
                    if (df.format(new java.util.Date(l)).equals(date)){
                        if (fast){
                            
                            long rv = RAF.getFilePointer() - bytesbetween - 4 - fieldsize;
                            results.add(rv);
                            
                        }else {
                        
                            results.add(k);
                            
                        }
                        
                    }

                }
                
                
                RAF.seek(RAF.getFilePointer() + bytesafter); //move to beginning of first field
            }
		results.trimToSize();  //clean up
                if (results.size() < 1) return null; //results.add( new Integer(0) ); //check for junk
                return results;

        } catch (Exception e) {

            e.printStackTrace();
            application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "scanColumn: A problem was encountered scanning "+currentDb, true));

            DV.writeFile("db.err", "scanCol :" + currentDb+ nl, true);

        }

        results.trimToSize();// clean up ArrayList before returning nothing
        //System.out.println( results.size() );
        if (results.size() < 1) return null;
        return results;

    }


    /* This method will return a list of keys/offsets to records containing the
     specified range of values in headValue and tailValue.  Originally built to 
     search for date ranges.*/
    private ArrayList scanLongColumn (int column, long headValue, long tailValue, boolean fast)  {

        //if fast, return positions instead of keys
        if (headValue == 0 && tailValue == 0) return null;
        if (tailValue < headValue) tailValue = headValue;

        ArrayList results = new ArrayList ();

        try {

		    long numrecs = getNumOfRecs();

            int numfields = currentSchema.getNumFields();

            int fieldsize = currentSchema.getFieldSize(column);

            int type = currentSchema.getFieldType(column);


            // calculate bytes before and after search field for each record


            int bytesbefore = 0;

            for (int i = 0; i < column; i++) {
                bytesbefore += currentSchema.getFieldSize(i);
            }

            int bytesbetween = bytesbefore - 4;

            int bytesafter =0;

            if (column < numfields-1) {

                for (int i = column+1; i < numfields; i++) {
                    bytesafter += currentSchema.getFieldSize(i);
                    
                }

            }

            int k = 0;

            Long l; //Version 1.5

            skipDbHeader();

            for (int i = 0; i < numrecs; i++)  {

		k = RAF.readInt();  //read key - advance 4 bytes
		RAF.seek(RAF.getFilePointer() + bytesbetween); //advance to search field
		    //Version 1.5
               
                    l = RAF.readLong();
                   //System.out.println("scan long col: "+l);


                    if (l >= headValue && l <= tailValue){
                        if (fast){

                            long rv = RAF.getFilePointer() - bytesbetween - 4 - fieldsize;
                            results.add(rv);

                        }else {

                            results.add(k);

                        }

                    }

                
                RAF.seek(RAF.getFilePointer() + bytesafter); //move to beginning of first field
            }
		results.trimToSize();  //clean up
                if (results.size() < 1) return null; //results.add( new Integer(0) ); //check for junk
                return results;

        } catch (Exception e) {

            e.printStackTrace();
            application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "scanLongColumn: A problem was encountered scanning "+currentDb, true));

        }

        results.trimToSize();// clean up ArrayList before returning nothing
        //System.out.println( results.size() );
        if (results.size() < 1) return null;
        return results;

    }


        private ArrayList scanFloatColumn (int column, float headValue, float tailValue, boolean fast)  {

        //if fast, return positions instead of keys
        if (headValue == 0 && tailValue == 0) return null;
        if (tailValue < headValue) tailValue = headValue;

        ArrayList results = new ArrayList ();

        try {

		    long numrecs = getNumOfRecs();

            int numfields = currentSchema.getNumFields();

            int fieldsize = currentSchema.getFieldSize(column);

            int type = currentSchema.getFieldType(column);


            // calculate bytes before and after search field for each record


            int bytesbefore = 0;

            for (int i = 0; i < column; i++) {
                bytesbefore += currentSchema.getFieldSize(i);
            }

            int bytesbetween = bytesbefore - 4;

            int bytesafter =0;

            if (column < numfields-1) {

                for (int i = column+1; i < numfields; i++) {
                    bytesafter += currentSchema.getFieldSize(i);
                }

            }

            int k = 0;

            float f; //Version 1.5

            skipDbHeader();

            for (int i = 0; i < numrecs; i++)  {

		k = RAF.readInt();  //read key - advance 4 bytes
		RAF.seek(RAF.getFilePointer() + bytesbetween); //advance to search field
		    //Version 1.5
                
                    f = RAF.readFloat();
                   if (debug) System.out.println("scan float col: "+f);


                    if (f >= headValue && f <= tailValue){
                        if (fast){

                            long rv = RAF.getFilePointer() - bytesbetween - 4 - fieldsize;
                            results.add(rv);

                        }else {

                            results.add(k);

                        }

                    }

                
                RAF.seek(RAF.getFilePointer() + bytesafter); //move to beginning of first field
            }
		results.trimToSize();  //clean up
                if (results.size() < 1) return null; //results.add( new Integer(0) ); //check for junk
                return results;

        } catch (Exception e) {

            e.printStackTrace();
            application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "scanFloatColumn: A problem was encountered scanning "+currentDb, true));

        }

        results.trimToSize();// clean up ArrayList before returning nothing
        //System.out.println( results.size() );
        if (results.size() < 1) return null;
        return results;

    }

        private ArrayList scanIntColumn (int column, int headValue, int tailValue, boolean fast)  {

        //if fast, return positions instead of keys
        if (headValue == 0 && tailValue == 0) return null;
        if (tailValue < headValue) tailValue = headValue;

        ArrayList results = new ArrayList ();

        try {

		    long numrecs = getNumOfRecs();

            int numfields = currentSchema.getNumFields();

            int fieldsize = currentSchema.getFieldSize(column);

            int type = currentSchema.getFieldType(column);


            // calculate bytes before and after search field for each record


            int bytesbefore = 0;

            for (int i = 0; i < column; i++) {
                bytesbefore += currentSchema.getFieldSize(i);
            }

            int bytesbetween = bytesbefore - 4;

            int bytesafter =0;

            if (column < numfields-1) {

                for (int i = column+1; i < numfields; i++) {
                    bytesafter += currentSchema.getFieldSize(i);
                }

            }

            int k = 0;

            int v; //Version 1.5

            skipDbHeader();

            for (int i = 0; i < numrecs; i++)  {

		k = RAF.readInt();  //read key - advance 4 bytes
		RAF.seek(RAF.getFilePointer() + bytesbetween); //advance to search field
		    //Version 1.5
                

                    //l = (Long) RAF.readLong();
                    v = RAF.readInt();
                   if (debug) System.out.println("scan Int col: "+v);


                    if (v >= headValue && v <= tailValue){
                        if (fast){

                            long rv = RAF.getFilePointer() - bytesbetween - 4 - fieldsize;
                            results.add(rv);

                        }else {

                            results.add(k);

                        }

                    }

               
                RAF.seek(RAF.getFilePointer() + bytesafter); //move to beginning of first field
            }
		results.trimToSize();  //clean up
                if (results.size() < 1) return null; //results.add( new Integer(0) ); //check for junk
                return results;

        } catch (Exception e) {

            e.printStackTrace();
            application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "scanIntColumn: A problem was encountered scanning "+currentDb, true));

        }

        results.trimToSize();// clean up ArrayList before returning nothing
        //System.out.println( results.size() );
        if (results.size() < 1) return null;
        return results;

    }



/*-------------------- DbEngine System Methods --------------------*/

public void close() {

    closeDb();

}



    public boolean loadSchema (String filename)   {
    // adjust arraylist that holds schema references to hold one more
    // load new schema
    
        File SCH = new File (filename);
    //System.out.println(filename);
    if (SCH.exists() && SCH.canRead() && SCH.canWrite()) {

        schList.add(new DbSchema (filename, unicode));
        return true;

    }else {

        javax.swing.JOptionPane.showMessageDialog(null,
                   "There was a problem loading schema: " + filename + nl +
                   "Contact technical support (software@datavirtue.com) for help.");

        application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "loadSchema: There was a problem accessing "+filename, true));


        return false;  // error
    }


}
	/* Initializer Method */
    private void loadDSF (String filename)  {
        //System.out.println(filename);
	java.io.File DSF = new java.io.File (filename);

		/* PARSE */
	if (DSF.exists())   {
            
            BufferedReader in;
            
            try {

                in = new BufferedReader(new FileReader (DSF));

                num_files = Integer.parseInt(in.readLine());  //read the first line store the result INT
                
                //schList = new ArrayList(num_files);
                
		//schema = new DbSchema[num_files];

		for (int i=0; i < num_files; i++) {

                    loadSchema(in.readLine());
                    
		}
                
                in.close();  /* If forgot to close this for a couple years. :)  Lesson learned. */
                
            }catch (Exception e) {

                javax.swing.JOptionPane.showMessageDialog(null,
                   "There was a problem loading the DSF."+ nl +
                   "Contact technical support (software@datavirtue.com) for help.");

                application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "loadDSF: There was a problem loading the Data System "+filename, true));

                DV.writeFile("db.err", "DSF Error :" + currentDb+ nl, true);
                
            }
              
            
	}else {

            status = "error:00:DSF Missing! " + nl +
                            " It could have been deleted or damaged, restore from backup." +
                            nl + " Verify the permissions and hardware integrity.";

            DV.writeFile("db.err", "DSF missing :" + filename+ nl, true);
        }
        
	
    }


    private DbSchema getSchemaObject (String dbname)	{
        /* Awesome!! this crap caused a bug, or rather revelaed one when changing from unicode to ASCII and VV */
        int sl = schList.size() - 1;  /* Assign method return to var and count back for speed */
	DbSchema s;

        for (int i=sl; i >= 0; i--)	{
            s = (DbSchema) schList.get(i);
            if (s.getDbName().equalsIgnoreCase(dbname)) return s;

        }
        application.registerRuntimeIncident(new RuntimeIncident(application.getAppName(), "getSchemaObject: The application asked for a schema that doesn't exist: "+dbname, true));

        DV.writeFile("db.err", "Problem getting Schema :" + dbname+ nl, true);
        return null;

    }

public String getStatus () {

    return status;

}

private String padString (String s, int total_length){
    
    StringBuilder sb = new StringBuilder (total_length);
        sb.append(s);
        int length = (total_length) - s.length();

       for (int i = 0; i < length; i++) {

           sb.append(' ');  //append space 
       }
       
       return sb.toString();
       
}

public boolean changePaths(String newPath){
     String t= "";
    boolean stat = true;
    DbSchema sch;
  
    for (int i = 0; i < schList.size(); i++){
        sch = (DbSchema) schList.get(i);
        t = sch.getDbName();
        stat = this.changePath(t, newPath);
        if (stat == false) return false;        
    }
    return true;
}
public boolean changePath(String db, String newPath) {
  
    if (!isDbAvailable(db)) return false;
    if (debug) System.out.println("Changing path on: "+ db + " to: "+newPath);
    this.getSchemaObject(db).changePath(newPath);
   
    return true;
     
    /*this.use_db(db,"r");
    boolean changed = currentSchema.changePath(newPath);
    
    if (changed) {
        
        this.use_db(db);//change the mode address to generate a new file
        return changed;
    }
    
    return changed;*/
   
}

public void setOptimized(boolean o){
    optimized = o;
}
/* -------------  Global Objects & Variables ---------------*/

private File dbFile = null;  /* currentDb File Object */

private RandomAccessFile RAF = null; /* currentDb Random Access File Object */

private DbSchema currentSchema = null; /* Access this to get the current db name */
//private DbSchema [] schema; /* Schema-object-reference-list populated by loadDSF */
private ArrayList schList = new ArrayList (0);
private String currentDb = "_"; /* In here currentDb is the full path\filename.ext */

private int num_files = 0;   // in use?
private String nowdb = "_";

private TableSorter sorter = null;
private String nl = System.getProperty("line.separator");

private String status = "NO DATA ERRORS REPORTED";

private String current_mode = "rw";  
private boolean optimized = false;
}/*END DbEngine 1.2 CLASS*/
