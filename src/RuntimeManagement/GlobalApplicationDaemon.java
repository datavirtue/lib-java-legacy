/*
 * This object changes to hold settings and state information that
 * need communicated between various apps residnig in the Nevitium System
 * This object will handle, error reporting and recovery, user settings (Properties),
 * screen state settings, security, etc
 * This prevents changing contructors when new objects need visibility.
 * Every object that interacts with the user must have access to the GlobalDaemon
 */

package RuntimeManagement;


import datavirtue.DbEngine;
import datavirtue.DbEngine;
import datavirtue.Settings;
import datavirtue.Settings;
import java.util.ArrayList;

/**
 *
 * @author dataVirtue
 */
public class GlobalApplicationDaemon {

    public GlobalApplicationDaemon(){

    }
    private String workingPath = ".";
    private DbEngine db;
    private Settings props;
    private KeyCard key_card=null;
    private ArrayList runtimeIncidents = new ArrayList();
    private Object jpa;
    private ArrayList mediators = new ArrayList();
    private ArrayList appReturnObjects;

    private int [] inventoryReturnValue=null;
    private int [] connectionsReturnValue=null;

    private boolean waiting = false;
    public void setWaiting(boolean wait){
        waiting = wait;
    }
    public boolean isWaiting(){
        
        return waiting;
    }

    public void setMediator(Mediator m){
        mediators.add(m);
    }
    public Mediator getMediator(){
        mediators.trimToSize();
        Mediator m = (Mediator)mediators.get(mediators.size()-1);
        /* return the last mediator in the list and remove it  */
        mediators.remove(mediators.size()-1);
        return m;
    }
    /**
     * @return the key_card
     */
    public KeyCard getKey_card() {
        if (key_card == null) return new KeyCard();
        return key_card;
    }

    /**
     * @param key_card the key_card to set
     */
    public void setKey_card(KeyCard key_card) {
        this.key_card = key_card;
    }

    public void setProps(Settings s){
        props = s;
    }
    public Settings getProps(){

        return props;
    }

    public void registerRuntimeIncident(RuntimeIncident rti){
        runtimeIncidents.add(rti);
        /* Add to error file log.*/
        if (rti.isShowStopper()){
            /* Tell the user and provide option to exit. */
            /* Provide other cleanup */
        }
    }
    public ArrayList getRuntimeIncidentList(){
        runtimeIncidents.trimToSize();
        return runtimeIncidents;
    }

    /**
     * @return the db
     */
    public DbEngine getDb() {
        return db;
    }

    /**
     * @param db the db to set
     */
    public void setDb(DbEngine db) {
        this.db = db;
    }

    public void setJPA(Object hibernate){
        jpa = hibernate;
    }
    public Object getJPA(){
        return jpa;
    }
    /**
     * @return the workingPath
     */
    public String getWorkingPath() {
        return workingPath;
    }

    /**
     * @param workingPath the workingPath to set
     */
    public void setWorkingPath(String workingPath) {
        this.workingPath = workingPath;
    }

    /**
     * @return the appReturnObjects
     */
    public ArrayList getAppReturnObjects() {
        if (appReturnObjects == null) return null;
        ArrayList a = new ArrayList(appReturnObjects);
        appReturnObjects = new ArrayList();
        return a;
    }

    /**
     * @param appReturnObjects the appReturnObjects to set
     */
    public void setAppReturnObjects(ArrayList appReturnObjects) {
        this.appReturnObjects = appReturnObjects;
    }

    private String app_name = "";

   public void setAppName(String app){
       app_name = app;
   }
   public String getAppName(){
       return app_name;
   }
}



