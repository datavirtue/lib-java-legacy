/*
 * DVNET.java
 *
 * Created on December 29, 2008, 1:56 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package datavirtue;
import java.net.*;
import java.io.*;

/**
 *
 * @author Data Virtue
 */
public class DVNET {
    
   public static String HTTPGetFile (String urlString, String userErrMessage, boolean showErrors) {
       
       String result="";
       String nl = System.getProperty("line.separator");
               try
        {
	  
            // Create an URL instance
            URL url = new URL(urlString);

            // Get an input stream for reading
            InputStream in = url.openStream();

            // Create a buffered input stream for efficency
            BufferedInputStream bufIn = new BufferedInputStream(in);

            // Repeat until end of file
            for (;;)
            {
                int data = bufIn.read();

                // Check for EOF
                if (data == -1)
                    break;
                else result = result + (char)data;
                    
            }
            return result;
        }
        catch (MalformedURLException mue)
        {
            if (showErrors) javax.swing.JOptionPane.showMessageDialog(null, userErrMessage+ " ERROR:Bad URL");
            return userErrMessage + " ERROR:Bad URL";
            
        }
        catch (IOException ioe)
        {
            if (showErrors) javax.swing.JOptionPane.showMessageDialog(null, userErrMessage + " ERROR:No Internet Connection");
            return userErrMessage + " ERROR:No Internet Connection";
        }

      
   }
    
    
}
