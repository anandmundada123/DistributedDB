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
  public static final String DDB_WRAP_LOCATION = "DISTRIBUTEDDATABASE_WRAP_LOCATION";
  public static final String DDB_WRAP_QS_LOCATION = "DISTRIBUTEDDATABASE_WRAP_QS_LOCATION";

  /**
   * Environment key name denoting the file timestamp for the shell script. 
   * Used to validate the local resource. 
   */
  public static final String DDB_DB_TIMESTAMP = "DISTRIBUTEDDATABASE_DB_TIMESTAMP";
  public static final String DDB_WRAP_TIMESTAMP = "DISTRIBUTEDDATABASE_WRAP_TIMESTAMP";
  public static final String DDB_WRAP_QS_TIMESTAMP = "DISTRIBUTEDDATABASE_QS_WRAP_TIMESTAMP";

  /**
   * Environment key name denoting the file content length for the shell script. 
   * Used to validate the local resource. 
   */
  public static final String DDB_DB_LEN = "DISTRIBUTEDDATABASE_DB_LEN";
  public static final String DDB_WRAP_LEN = "DISTRIBUTEDDATABASE_WRAP_LEN";
  public static final String DDB_WRAP_QS_LEN = "DISTRIBUTEDDATABASE_QS_WRAP_LEN";

  /**
   * Location of Script on all nodes
   */
  public static final String DB_SCRIPT_LOCATION = "exec_cmd.py";
  public static final String WRAP_SCRIPT_LOCATION = "cont_net.py";
  public static final String WRAP_QS_SCRIPT_LOCATION = "exec_qs.py";
  
  /**
   * Port Number where App Master is listening 
   */
  /*public static final int CLIENT_PORT_NO  = 54000;
  public static final int APP_MASTER_PORT = 55000;*/
  
  /**
   * Command Line Constants 
   */
  
  public static final String CLIENT_HOST_NAME = "clientHostNameArg";
  public static final String CLIENT_PORT_NO = "clientPortNoArg";
  
  /**
   * Special Message Types
   */
  public static final String APP_MASTER_INFO = "APP_MASTER_INFO";
  
  /**
   * Database types 
   */
  public static final String QUICKSTEP_DB = "quickstep";
  public static final String SQLITE3_DB = "sqlite3";
}

