/*
 * KeyCard.java
 *
 * Created on July 27, 2007, 10:50 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package RuntimeManagement;

/**
 *
 * @author Data Virtue
 */
public class KeyCard {
    
    /** Creates a new instance of KeyCard */
    public KeyCard(Object [] user) {
    
        userName = (String)user[1];
        master = (Boolean)user[3];
        
        if (!master){
        invtMetric = (Long)user[4];
        connMetric = (Long)user[5];
        invMetric = (Long)user[6];
        iManMetric = (Long)user[7];
        repMetric = (Long)user[8];
        chkMetric = (Long)user[9];
        expMetric = (Long)user[10];
        configMetric = (Long)user[11];
        }

    }

    public KeyCard() {
        userName = "DEFAULT";
        master = true;
    }
    private String userName;
    private boolean master;
    private long invtMetric, connMetric, invMetric, iManMetric, repMetric,
            chkMetric, expMetric, configMetric;


    public String getUserName() {
        return userName;    }

    public boolean isMaster() {
        return master;
      }
    
    public boolean checkInventory(int required){
        if (master) return true;
        if (invtMetric < required){
            return false;
        }else return true;
    }

    public boolean checkConnections(int required){
        if (master) return true;
        if (connMetric < required){
            return false;
        }else return true;
    }

    public boolean checkInvoice(int required){
        if (master) return true;
        if (invMetric < required){
            return false;
        }else return true;
    }

    public boolean checkManager(int required){
        if (master) return true;
        if (iManMetric < required){
            return false;
        }else return true;
    }

    public boolean checkReports(int required){
        if (master) return true;
        if (repMetric < required){
            return false;
        }else return true;
    }

    public boolean checkCheck(int required){
        if (master) return true;
        if (chkMetric < required){
            return false;
        }else return true;
    }

    public boolean checkExports(int required){
        if (master) return true;
        if (expMetric < required){
            return false;
        }else return true;
    }

    public boolean checkConfig(int required){
        if (master) return true;
        if (configMetric < required){
            return false;
        }else return true;
    }

    public void showMessage(String detail){

        javax.swing.JOptionPane.showMessageDialog(null,
                "You do not have permission to access this feature. ("+detail+")");

    }


}
