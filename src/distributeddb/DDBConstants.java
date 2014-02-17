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
  public static final String DDB_DB_LOCATION = "DISTRIBUTEDDATABASE_DB_LOCATION";
  public static final String DDB_CAPT_LOCATION = "DISTRIBUTEDDATABASE_CAPT_LOCATION";

  /**
   * Environment key name denoting the file timestamp for the shell script. 
   * Used to validate the local resource. 
   */
  public static final String DDB_DB_TIMESTAMP = "DISTRIBUTEDDATABASE_DB_TIMESTAMP";
  public static final String DDB_CAPT_TIMESTAMP = "DISTRIBUTEDDATABASE_CAPT_TIMESTAMP";

  /**
   * Environment key name denoting the file content length for the shell script. 
   * Used to validate the local resource. 
   */
  public static final String DDB_DB_LEN = "DISTRIBUTEDDATABASE_DB_LEN";
  public static final String DDB_CAPT_LEN = "DISTRIBUTEDDATABASE_CAPT_LEN";
  
  /**
   * Location of Script on all nodes
   */
  public static final String DB_SCRIPT_LOCATION = "exec_cmd.py";
  public static final String CAPT_SCRIPT_LOCATION = "getoutput.sh";
  
}

