package distributeddb;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Constants used in both Client and Application Master
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class DDBConstants {


  /**
   * Environment key name pointing to the shell script's location
   */
  public static final String DDBLOCATION = "DISTRIBUTEDDATABASELOCATION";

  /**
   * Environment key name denoting the file timestamp for the shell script. 
   * Used to validate the local resource. 
   */
  public static final String DDBTIMESTAMP = "DISTRIBUTEDDATABASETIMESTAMP";

  /**
   * Environment key name denoting the file content length for the shell script. 
   * Used to validate the local resource. 
   */
  public static final String DDBLEN = "DISTRIBUTEDDATABASELEN";
}

