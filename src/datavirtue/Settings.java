/*
 * Settings.java
 *
 * Created on October 18, 2006, 9:43 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

/**
 *
 * @author Administrator
 */
package datavirtue;

import java.util.ArrayList;
import java.io.*;

public class Settings {

    /** Creates a new instance of Settings */
    public Settings(String file) {
    
        setFile (file);
    
    }
    
    
    public String getProp (String key) {
        
        //read all items into memory, 
        ArrayList list = readProperties ();
        
        //search for key and return everyrthing after =
        return parseValue (search (key, list)).trim();
        
    }
    
    public void setProp (String key, String value) {
        
        ArrayList list = readProperties ();
        if (list == null) list = new ArrayList ();
        
        int index = find(key, list);
        
        if ( index == -1) list.add(key.toUpperCase() + "=" + value);
        else list.set(find(key, list), key.toUpperCase() + "=" + value);
        
        writeProperties (list);
      
        
    }
    
    
    private String parseValue (String s) {
        
        String r = "";
         
        if ( s != null  &&  s.length() > 0 ) {
            
            r =  s.substring( s.indexOf('=') + 1 ).trim(); 
            //System.out.println(r + "    *" + s);
            
        }
         
     return r; 
     
    }
    
    
    private int find (String s, ArrayList l) {
        
        String tmp="";
        int size = 0;
        if (l != null ) size = l.size();
        if (size < 1) return -1;
        
        for (int i = 0; i < size; i++) {
            
            if (l != null) {
                
                tmp = (String) l.get(i);
                tmp.trim();
            }
            
            if ( tmp.contains(s) ) return i;
            
            
        }
        
        return -1;  //couldnt find it
        
    }
    
    private String search (String s, ArrayList l) {
        
        if (l != null) {
        String tmp = "";
        int size = l.size();
        
        for (int i = 0; i < size; i++) {
            
            if (l != null) tmp = (String) l.get(i);
                       
            if (  tmp != null && tmp.contains(s) ) return tmp.trim();
            
            
        }
        }
        
       return ""; 
    }
    
        
    private ArrayList readProperties () {
        
        ArrayList list = new ArrayList();
                
        String line ="";
        
        try {
            
            boolean exists = (new File(filename)).exists();
        
                if (exists) {
            
                    File data = new File (filename);
                    BufferedReader in = new BufferedReader(
                                    new FileReader(data));
                 do {
                
                  line = in.readLine();
                  if (line != null) list.add( line );
                   //System.out.println(line);               
                } while (line != null);
            
                in.close();
            }else if (list.size() < 1) list = null;
            
            
        } catch (Exception e) {e.printStackTrace();}
        
        if (list != null) list.trimToSize();
        
        return list;
    }
    
    private boolean writeProperties (ArrayList list) {
        
        try {
                
            File data = new File (filename);
            if (!data.exists()) data.createNewFile();
            
            PrintWriter out = new PrintWriter(
                    new BufferedWriter( 
                     new FileWriter (data ) ) );
                //write text
            
                
            int size = list.size();
           
            for (int i = 0; i < size; i++) {
            
                out.println( (String) list.get(i) );
                
           }
                    
            
            out.flush();
            out.close();
            return true;
            
        } catch (Exception e) {e.printStackTrace(); return false;}
        
        
    
    }
    
    
    
    public void setFile (String f) {
        
        filename = f;
    }
    
    
    private String filename;
    
    
    
}
