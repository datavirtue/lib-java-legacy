/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package RuntimeManagement;

/**
 *
 * @author dataVirtue
 */
public class RuntimeIncident {

    public RuntimeIncident(String app, String problem, boolean show_stopper){

        this.showStopper = show_stopper;
        this.app = app;
        this.problem = problem;

    }


    private String app="Nevitium";
    private String problem="None";
    private boolean showStopper = false;

    /**
     * @return the app
     */
    public String getApp() {
        return app;
    }

    /**
     * @return the problem
     */
    public String getProblem() {
        return problem;
    }

    /**
     * @return the show_stopper
     */
    public boolean isShowStopper() {
        return showStopper;
    }

}
