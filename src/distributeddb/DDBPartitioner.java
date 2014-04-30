package distributeddb;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DDBPartitioner {
	private List<String> nodes;
	private Map<String, Partition> tables;
	private Log LOG;
	
	private String logPrefix() {
		return "[DDBPARTITIONER]";
	}
	
	public DDBPartitioner(Log log) {
		this.nodes = new ArrayList<String>();
		this.LOG = log;
		//Try to load partition data from file
		if(!loadData()) {
			// Couldn't load, setup new Map
			LOG.info("Partition data not found, starting fresh!");
			this.tables = new HashMap<String, Partition>();
		} else {
			LOG.info("Partition data loaded from file!");
		}
	}

	public DDBPartitioner(List<String> nodes, Log log) {
		this.nodes = nodes;
		this.LOG = log;
		//Try to load partition data from file
		if(!loadData()) {
			// Couldn't load, setup new Map
			LOG.info("Partition data not found, starting fresh!");
			this.tables = new HashMap<String, Partition>();
		} else {
			LOG.info("Partition data loaded from file!");
		}
	}
	
	public void registerNodes(List<String> nodes) {
		this.nodes = nodes;
	}
	
	public void registerNode(String node) {
		this.nodes.add(node);
	}
	
	/**
	 * Saves the table map to disk so we can load it at next restart
	 */
	public void saveData() {
		LOG.info("Saving partition map to disk");
		try {
			ObjectOutputStream out = new ObjectOutputStream(
					new FileOutputStream("partitions.data"));
			out.writeObject(tables);
			out.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	public boolean loadData() {
		LOG.info("Loading partition map from disk");
		try {
			ObjectInputStream in = new ObjectInputStream(
					new FileInputStream("partitions.data"));
			tables = (Map<String, Partition>) in.readObject();
			in.close();
			return true;
		} catch(Exception e) {
			//Throwing an exception is OK (if the file didn't exist)
			return false;
		}
	}
	
	/**
	 * Dumps known partition info as a string
	 * @return string of info
	 */
	public String explain() {
		String out = "";
		Iterator<Map.Entry<String, Partition>> it = tables.entrySet().iterator();
		while(it.hasNext()) {
			Map.Entry<String, Partition> p = (Map.Entry<String, Partition>)it.next();
			out += p.getKey() + "\n";
			out += p.getValue().explain();
		}
		
		return out;
	}
	
	public String getSelectStr(String query) {
		Pattern pat = Pattern.compile("\\s*select(.*)from.*", Pattern.CASE_INSENSITIVE);
		Matcher mat = pat.matcher(query);
		if(mat.matches()) {
			return mat.group(1).trim();
		} else {
			return "";
		}
	}
	
	public String getTableStr(String query) {
		Pattern pat = Pattern.compile("\\s*(select.*from |create table|insert into)(.*)", Pattern.CASE_INSENSITIVE);
		Matcher mat = pat.matcher(query);
		if(mat.matches()) {
			String tmp = mat.group(2).trim();
			//If it contains spaces or parens deal with it
			if(tmp.contains(" ")) {
				return tmp.substring(0, tmp.indexOf(" "));
			} else if(tmp.contains("(")) {
				return tmp.substring(0, tmp.indexOf("("));
			} else {
				return tmp;
			}
		} else {
			return "";
		}
	}
	
	public String getWhereStr(String query) {
		Pattern pat = Pattern.compile("\\s*select.*from.*where(.*)", Pattern.CASE_INSENSITIVE);
		Matcher mat = pat.matcher(query);
		if(mat.matches()) {
			return mat.group(1).trim();
		} else {
			return "";
		}
	}
	
	/**
	 * Take the query and break it up into the components to send out
	 * to each node and also break the query down (if required).
	 * @param query
	 * @return Map<String, String> Where K is a node name and V is the query to send
	 * @throws Exception 
	 */
	public Map<String, String> parseQuery(String query) throws Exception {
		LOG.info(logPrefix() + " Query: '" + query + "'");
		/*
		 * First we must identify what kind of query it is (currently supported):
		 *   select
		 *   insert
		 *   create
		 *   drop
		 */
		
		// select
		Pattern selectPat = Pattern.compile("\\s*select.*", Pattern.CASE_INSENSITIVE);
		Matcher selectMat = selectPat.matcher(query);
		if(selectMat.matches()) {
			return parseSelectQuery(query);
		}

		// insert
		Pattern insertPat = Pattern.compile("\\s*insert.*", Pattern.CASE_INSENSITIVE);
		Matcher insertMat = insertPat.matcher(query);
		if(insertMat.matches()) {
			return parseInsertQuery(query);
		}
		
		// create
		Pattern createPat = Pattern.compile("\\s*create.*", Pattern.CASE_INSENSITIVE);
		Matcher createMat = createPat.matcher(query);
		if(createMat.matches()) {
			return parseCreateQuery(query);
		}	

		// drop
		Pattern dropPat = Pattern.compile("\\s*drop table.*", Pattern.CASE_INSENSITIVE);
		Matcher dropMat = dropPat.matcher(query);
		if(dropMat.matches()) {
			return parseDropQuery(query);
		}	
		
		/*
		 * We didn't match a supported query, throw a fit
		 */
		LOG.fatal("Query type not currently supported: " + query);
		return null;
	}

	private Map<String, String> parseCreateQuery(String query) throws Exception {
		System.out.println("CREATE match: ");
		// Pull out important pieces
		Pattern pat = Pattern.compile("\\s*create table (.*)\\((.*)\\) partition by (.*)", Pattern.CASE_INSENSITIVE);
		Matcher mat = pat.matcher(query);
		if(mat.matches()) {
			String table = mat.group(1);
			String attrs = mat.group(2);
			String part = mat.group(3);
			
			System.out.println("Table: " + table);
			System.out.println("Attrs: " + attrs);
			
			// Make sure the table doesn't already exist
			if(tables.containsKey(table)){
				LOG.fatal("ERROR create table, table already exists");
				throw new Exception("TableExists");
			}
			
			// Take the partition string and pass it to the parsing function
			// it will return a Partition object which we should store for this table
			Partition p = parsePartition(table, attrs, part);
			
			// Now save the partition to be used for this table from now on
			tables.put(table, p);
			
			// When we add partition data, save the partition to file
			saveData();
			
			// Now use the partition object to parse the query and return the proper query map
			List<String> initTables = p.initialize();
			System.out.println(initTables.toString());
			Map<String, String> qMap = new HashMap<String, String>();
			String cmd = "create table " + table + "(" + attrs + ")";
			for(String s: initTables) {
				qMap.put(s, cmd);
			}
			return qMap;
		} else {
			LOG.fatal("Select string initial match failed in the end");
			return null;
		}
	}

	private Map<String, String> parseDropQuery(String query) throws Exception {
		System.out.println("DROP match: ");
		// Pull out important pieces
		Pattern pat = Pattern.compile("\\s*drop table (.*)", Pattern.CASE_INSENSITIVE);
		Matcher mat = pat.matcher(query);
		if(mat.matches()) {
			String table = mat.group(1);
			
			System.out.println("Table: " + table);
			
			// Make sure the table doesn't already exist
			if(tables.containsKey(table)){
				//Delete the partition data for this table
				Partition p = tables.remove(table);
				//Save the partition data to disk
				saveData();
				//Respond with the list of nodes from the partition table
				//so a drop statement can be sent to all of them
                List<String> theNodes = p.chooseSelectNode();
				Map<String, String> qMap = new HashMap<String, String>();
                for(String s: theNodes) {
                    qMap.put(s, query);
                }
                return qMap;
				
			} else {
				LOG.fatal("ERROR drop table, table doesn't exists");
				throw new Exception("NoTableExists");
			}
		} else {
			LOG.fatal("Select string initial match failed in the end");
			return null;
		}
	}
	
	private Partition parsePartition(String table, String attrs, String part) throws Exception {
		System.out.println("Partition: " + part);
		/*
		 * The partition will match one of the following 4 options:
		 *   random
		 *   range
		 *   hash
		 *   roundrobin
		 */
		if(part.contains("random")) {
			Pattern pat = Pattern.compile("random\\((.*)\\)", Pattern.CASE_INSENSITIVE);
			Matcher mat = pat.matcher(part);
			//The user can specify either "random(NUM)" or "random", if random use all nodes, if NUM only use NUM nodes
			if(mat.matches()) {
				int nodeNum = Integer.parseInt(mat.group(1));
				return new RandomPartition(nodes, nodeNum);
			} else {
				return new RandomPartition(nodes, nodes.size());
			}
		}
		/*
		 * Range Partition
		 */
		else if(part.contains("range")) {
		
			return new RangePartition(nodes, "", "");
		}
		/*
		 * Hash Partition
		 */
		else if(part.contains("hash")) {
			Pattern pat;
			//Check if they specify partitions to make the regex easier
			if(part.toLowerCase().contains("partitions")){
				pat = Pattern.compile("hash\\((.*)\\) partitions \\((.*)\\)", Pattern.CASE_INSENSITIVE);
				Matcher mat = pat.matcher(part);
                if(mat.matches()) {
                    return new HashPartition(nodes, attrs, mat.group(1), mat.group(2));
                } else {
                    // The user has to specify the attr we are hashing on
                    throw new Exception("HashSyntaxError");
                }
			} else {
				pat = Pattern.compile("hash\\((.*)\\)", Pattern.CASE_INSENSITIVE);
				Matcher mat = pat.matcher(part);
                if(mat.matches()) {
                    return new HashPartition(nodes, attrs, mat.group(1), null);
                } else {
                    // The user has to specify the attr we are hashing on
                    throw new Exception("HashSyntaxError");
                }
			}
			
		
		}
		/*
		 * Round Robin Partition
		 */
		else if(part.contains("roundrobin")) {
			Pattern pat = Pattern.compile("roundrobin\\((.*)\\)", Pattern.CASE_INSENSITIVE);
			Matcher mat = pat.matcher(part);
			//The user can specify either "random(NUM)" or "random", if random use all nodes, if NUM only use NUM nodes
			if(mat.matches()) {
				return new RoundRobinPartition(nodes, mat.group(1));
			} else {
				return new RoundRobinPartition(nodes, null);
			}
			
		}
		//No type match, throw error
		throw new Exception("BadPartitionType");
	}

	private Map<String, String> parseInsertQuery(String query) throws Exception {
		System.out.println("INSERT match: ");
		// Pull out important pieces
		Pattern pat = Pattern.compile("\\s*insert into (.*) values \\((.*)\\)", Pattern.CASE_INSENSITIVE);
		Matcher mat = pat.matcher(query);
		if(mat.matches()) {
			String tblTmp = mat.group(1);
			String table = "";
			String tblCols = "";
			String theVals = mat.group(2);
			
			// The table might also contain where clause stuff, fix that
			if(tblTmp.contains("(")) {
				Pattern tPat = Pattern.compile("(.*)\\((.*)\\)", Pattern.CASE_INSENSITIVE);
				Matcher tMat = pat.matcher(tblTmp);
				if(tMat.matches()) {
					table = tMat.group(1);
					tblCols = tMat.group(2);
				}
			} else {
				table = tblTmp;
			}
			System.out.println("Table: " + table);
			System.out.println("TableCols: " + tblCols);
			System.out.println("Values: " + theVals);
			
			// If multiple value sets exist here, explode it into a list to pass to the chooseInsertNode method
            /*Pattern vPat = Pattern.compile("\\((.*)\\),.*", Pattern.CASE_INSENSITIVE);
            Matcher vMat = vPat.matcher(theVals);
            List<String> valList = null;
            while(vMat.find()) {
            	System.out.println("Valmatch: " + vMat.start() + ", " + vMat.end());
            }*/
			//For now throw an error TODO
			if(theVals.contains("(")) {
				throw new Exception("MultipleValuesNotSupported");
			}
			
			//Find a match in our tables map
			if(tables.containsKey(table)) {
				String selNode = tables.get(table).chooseInsertNode(theVals);
				Map<String, String> qMap = new HashMap<String, String>();
				// We only insert into 1 node
				qMap.put(selNode, query);

				return qMap;
			} else {
				throw new Exception("NoTableFound");
			}
		} else {
			LOG.fatal("Select string initial match failed in the end");
			return null;
		}
	}

	private Map<String, String> parseSelectQuery(String query) throws Exception {
		System.out.println("SELECT match: ");
		
		// Pull out important pieces from the select query
		Pattern pat = Pattern.compile("\\s*select (.*) from (.*)", Pattern.CASE_INSENSITIVE);
		Matcher mat = pat.matcher(query);
		if(mat.matches()) {
			String selectAttrs = mat.group(1);
			String tmp = mat.group(2);
			String table = "";
			String where = "";
			
			// The table might also contain where clause stuff, fix that
			if(tmp.contains(" ")) {
				table = tmp.substring(0, tmp.indexOf(" "));
				where = tmp.substring(tmp.indexOf(" ") + 1);
			} else {
				table = tmp;
			}
			System.out.println("Breakdown: " + selectAttrs);
			System.out.println("Table: " + table);
			System.out.println("Where: " + where);
			
			//Find a match in our tables map
			if(tables.containsKey(table)) {
				List <String> selNodes = tables.get(table).chooseSelectNode();
				Map<String, String> qMap = new HashMap<String, String>();
				String cmd = "select " + selectAttrs + " from " + table + " " + where;
				for(String s: selNodes) {
					qMap.put(s, cmd);
				}
				return qMap;
			} else {
				throw new Exception("NoTableFound");
			}
		} else {
			LOG.fatal("Select string initial match failed in the end");
			return null;
		}
	}
}


class UnitTestDDBPartitioner {
	
	private static final Log LOG = LogFactory.getLog(UnitTestDDBPartitioner.class);
	
	public static void main(String[] args) throws Exception {
		String query = "!cmd wah this is a test";
		List<String> tmp = new ArrayList<String>(Arrays.asList(query.split(" ")));
		//Drop "!cmd" element
		tmp.remove(0);
		String node = tmp.remove(0);
		
		String msg = tmp.remove(0);
		for(String s: tmp) {
			msg = msg.concat(" " + s);
		}
		
        System.out.println(node);
        System.out.println(msg);
		System.exit(0);
		System.out.println("Running DDBPartitioner test");
		DDBPartitioner p = new DDBPartitioner(LOG);
		
		System.out.println(p.explain());
		/*System.out.println(p.getTableStr("select * from test where stuff"));
		System.out.println(p.getTableStr("select * from test"));
		System.out.println(p.getTableStr("create table test(stuff)"));
		System.out.println(p.getTableStr("insert into test values (stuff)"));*/
		
		/*System.out.println(p.getSelectStr("select * from test"));
		System.out.println(p.getSelectStr("select id from test"));
		System.out.println(p.getSelectStr("select id, name from test"));
		System.out.println(p.getSelectStr("select the stuff from test"));
		System.out.println(p.getSelectStr("create table test(stuff)"));
		System.out.println(p.getSelectStr("insert into test values (stuff)"));*/
		
		/*System.out.println(p.getWhereStr("select id, name from test"));
		System.out.println(p.getWhereStr("select id, name from test where id > 10"));
		System.out.println(p.getWhereStr("select id, name from test where id > 10 and name = dale order by id"));*/

		p.registerNode("n0");
		p.registerNode("n1");
		p.registerNode("n2");
		p.registerNode("n3");
		p.registerNode("n4");
		/**************************************
		 * TODO: We need to send what the columns are when we create the Partition object
		 * because in partition schemes when we need to make a decision based on the value (range) it will be required
		 */
		
		/*
		 * Round robin test
		 */
		/*
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("create table rrtest1(i INTEGER, uname TEXT) partition by roundrobin"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("create table rrtest2(i INTEGER, uname TEXT) partition by roundrobin(n0,n1,n2)"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into rrtest1 values (0, 'test')"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into rrtest1 values (1, 'test1')"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into rrtest1 values (2, 'test1')"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into rrtest1 values (3, 'test1')"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into rrtest1 values (4, 'test1')"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into rrtest1 values (5, 'test1')"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into rrtest2 values (0, 'test1')"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into rrtest2 values (1, 'test1')"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into rrtest2 values (2, 'test1')"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("Select * from rrtest1"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("Select * from rrtest2"));
		*/

		/*
		 * Random test
		 */
		/*
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery(" create table rtest1(i INTEGER, j INTEGER) partition by random"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("create table rtest2(i INTEGER) partition by random(2)"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into rtest1 values (1, 9)"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into rtest1 values (2, 8)"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into rtest1 values (3, 7)"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into rtest1 values (4, 6)"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into rtest1 values (5, 5)"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into rtest1 values (6, 4)"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into rtest2 values (1, 9)"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into rtest2 values (2, 8)"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into rtest2 values (3, 7)"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("Select * from rtest1"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("Select * from rtest2"));
		*/
		
		/*
		 * Hash test
		 */
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("create table htest1(i INTEGER, uname TEXT) partition by hash(i)"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("create table htest2(i INTEGER, uname TEXT) partition by hash(uname) PARTITIONS (n0,n1,n2)"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("create table htest3(i INTEGER) partition by hash(i)"));
		//System.out.println("===============================================================================");
		//System.out.println(p.parseQuery("create table htest3(i INTEGER, uname TEXT) partition by hash(j)"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into htest1 values (1, 'test')"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into htest1 values (2, 'test')"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into htest1 values (3, 'test')"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into htest1 values (4, 'test')"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into htest1 values (5, 'test')"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into htest2 values (1, 'test')"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into htest2 values (2, 'asdf')"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into htest2 values (3, 'qwerty')"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into htest2 values (4, 'deadbeef')"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("Select * from htest1"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("Select * from htest2"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("drop table htest3"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("drop table htest2"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("drop table htest1"));
		
		
		/*System.out.println("===============================================================================");
		p.parseQuery("create table test1(i INTEGER, j INTEGER) partition by range(i) PARTITIONS (i < 20, i < MAX)");
		System.out.println("===============================================================================");
		p.parseQuery("create table test3(i INTEGER, uname TEXT) partition by hash(4)");

		/*
		 * Insert statements
		 */
		/*System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into test4 values (0, 10)"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("insert into test4 values (1, 9)"));
		System.out.println("===============================================================================");
		//System.out.println(p.parseQuery("insert into test4 values ((1, 9), (2, 8))"));*/

		/*
		 * Select statements
		 */
		/*System.out.println("===============================================================================");
		System.out.println(p.parseQuery("Select * from test4"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("Select * from test5"));
		System.out.println("===============================================================================");
		System.out.println(p.parseQuery("Select * from test6"));
		//System.out.println("===============================================================================");
		//p.parseQuery("Select id, name from test4");
		//System.out.println("===============================================================================");
		//p.parseQuery("Select id, name from test5 where id = 1");*/
		
	}
}
