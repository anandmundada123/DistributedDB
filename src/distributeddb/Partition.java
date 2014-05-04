package distributeddb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


interface Partition extends Serializable {
	List<String> initialize();
	String chooseInsertNode(String vals);
	List<String> chooseSelectNode(String whereClause);
	String explain();
}

class RandomPartition implements Partition {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private List<String> nodes;
	private int nodeNumber;
	private int[] distribution;

	public RandomPartition(List<String> nodes, int nodeNumber) throws Exception {
		this.nodes = new ArrayList<String>(nodes);
		this.nodeNumber = nodeNumber;
		if(nodes.size() < this.nodeNumber) {
			throw new Exception("BadPartitionSpecification");
		}
		//Check if we need to remove any nodes based on the number
		int numRemove = nodes.size() - nodeNumber;
		for(int i = numRemove; i > 0; i--) {
			// Pick a random node to remove from the list
			this.nodes.remove(0);
		}
		System.out.println("Nodes: " + this.nodes);
		// Keep track of where we choose to put the data
		distribution = new int[this.nodes.size()];
		//Init the dist
		for(int i = 0; i < this.nodes.size(); i++) {
			distribution[i] = 0;
		}
	}
	
	public String explain() {
		String out = "";
		for(int i = 0; i < nodes.size(); i++) {
			out += "\t" + nodes.get(i)  + "\t\t: " + distribution[i] + "\n";
		}
		return out;
	}
	
	/**
	 * Return a list of nodes that need to have the create table query sent to
	 */
	public List<String> initialize() {
		return nodes;
	}
	/**
	 * We randomly are inserting, so pick one node to return
	 */
	public String chooseInsertNode(String vals) {
		Random r = new Random();
		int ptr = r.nextInt(nodes.size());
		distribution[ptr]++;
		return nodes.get(ptr);
	}
	
	/**
	 * When looking for data we don't know where it would be so request from all
	 */
	public List<String> chooseSelectNode(String whereClause) {
		return nodes;
	}
}

class HashPartition implements Partition {
	private static final long serialVersionUID = 2L;
	private List<String> nodes;
	private int[] distribution;
	private String hashingAttr;
	//Store the index to where the hashing attr showed up
	//This is used for when an insert statement is made, it will help to determine where to send the data
	private int hashingAttrPosn;
	private String hashingType;
	private String declAttrs;
	private final String[] SUPPORTEDATTRTYPES = {"integer", "char", "decimal"};
	
	public HashPartition(List<String> nodes, String attrs, String hashOn, String reqNodes) throws Exception {
		//Keep track of what the declared attributes were
		declAttrs = attrs;

		//First check if the user requested nodes:
		if(reqNodes == null) {
			this.nodes = new ArrayList<String>(nodes);
		} else {
			this.nodes = new ArrayList<String>();
			//Parse their list for matches
			String[] tmpNodes = reqNodes.split(",");
			for(String n: tmpNodes) {
				//Make sure that the node the user requested is in the list of valid nodes
				if(nodes.contains(n)){
					this.nodes.add(n);
				} else {
					//The node they requested doesn't exist, throw an error
					System.out.println("[HASH] Requested node doesn't exist: " + n);
					throw new Exception("HashPartitionUnknownNode");
				}
			}
		}
		// Keep track of where we choose to put the data
		distribution = new int[this.nodes.size()];
		//Init the dist
		for(int i = 0; i < this.nodes.size(); i++) {
			distribution[i] = 0;
		}
		
		// Now we need to look through the attribute list to make sure their hash attr is in the list
		String[] theAttrs = attrs.split(",");
		hashingAttrPosn = 0;
        Pattern pat = Pattern.compile("(.*) (.*?)(\\([0-9]*\\)|)", Pattern.CASE_INSENSITIVE);
		for(String a: theAttrs) {
			a = a.trim();
			
			System.out.println("Initial: " + a);
            Matcher mat = pat.matcher(a);
            if(mat.matches()) {
            	String attrName = mat.group(1);
            	String attrType = mat.group(2);
            	//NOTE the regex gives us size if they declared it but we don't care
            	String attrSize = mat.group(3);
                
            	// The attr should look like "name type" so find that
                if(attrName == null || attrName.equals("") || attrType == null || attrType.equals("")) {
                    throw new Exception("HashPartitionInvalidAttributeDeclaration");
                }
                
                //Search for the match to the requested hash attr
                if(hashOn.equals(attrName)){
                    hashingAttr = attrName.toLowerCase();
                    hashingType = attrType.toLowerCase();
                    //Make sure the hashing type is supported
                    if(!Arrays.asList(SUPPORTEDATTRTYPES).contains(hashingType)){
                        throw new Exception("HashPartitionUnsupportedType");
                    }
                    break;
                }
                hashingAttrPosn++;
            } else {
            	throw new Exception("HashPartitionInvalidAttributeSyntax");
            }
		}
		if(hashingAttr == null) {
			throw new Exception("HashPartitionInvalidHashAttribute");
		}
		
		System.out.println("Hashing(" + hashingAttrPosn + "): '" + hashingAttr + "' " + hashingType);
	}

	public String explain() {
		String out = "\tHashing Attribute: " + hashingAttr + ", type: " + hashingType + "\n";
		for(int i = 0; i < nodes.size(); i++) {
			out += "\t" + nodes.get(i)  + "\t\t: " + distribution[i] + "\n";
		}
		return out;
	}

	public List<String> initialize() {
		return nodes;
	}

	/**
	 * Choose a node to insert the data into, for hashing we need
	 * to parse the values and find our hashing attribute from the list.
	 */
	public String chooseInsertNode(String vals) {
		String[] theVals = vals.split(",");
		//Our match *should* be the hashingAttrPosn index value!
		String hashValue = theVals[hashingAttrPosn];
		System.out.println("[HASHPARTITION] Hashing(" + hashingAttr + ") on " + hashValue);
		
		int ptr;
		
		// Now hash this value against our list of nodes
		if(hashingType.equals("integer")){
			int hashVal = Integer.parseInt(hashValue);
			ptr = hashVal % nodes.size();
		} else if (hashingType.equals("char")) {
			ptr = Math.abs(hashValue.hashCode()) % nodes.size();
		} else if (hashingType.equals("decimal")) {
			float hashVal = Float.parseFloat(hashValue);
			ptr = (int) (hashVal % nodes.size());
		} else {
			// NOTE: We already checked in the constructor if the type is valid so we can just ignore this
			System.out.println("[HASHPARTITION] THIS SHOULD NEVER EVER EVER EVER EVER SHOW UP!");
			return nodes.get(0);
		}
		
		//Track distribution
		distribution[ptr]++;
		
		return nodes.get(ptr);
	}
	
	/**
	 * For now just send selects to all nodes
	 */
	public List<String> chooseSelectNode(String whereClause) {
		// only support = where clause
		if(whereClause == null || whereClause.equals("") || !whereClause.contains("=")) {
			return nodes;
		}
		else {
			String [] tmp = whereClause.split("=");
			String attr = tmp[0].trim();
			if(attr.equals(hashingAttr)) {
				List<String> nodeList = new ArrayList<String>();
				int ptr;		
				// Now hash this value against our list of nodes
				if(hashingType.equals("integer")){
					int hashVal = Integer.parseInt(tmp[1].trim());
					ptr = hashVal % nodes.size();
				} else if (hashingType.equals("char")) {
					ptr = Math.abs(tmp[1].trim().hashCode()) % nodes.size();
				} else if (hashingType.equals("decimal")) {
					float hashVal = Float.parseFloat(tmp[1].trim());
					ptr = (int) (hashVal % nodes.size());
				} else {
					// NOTE: We already checked in the constructor if the type is valid so we can just ignore this
					System.out.println("[HASHPARTITION] THIS SHOULD NEVER EVER EVER EVER EVER SHOW UP!");
					return nodes;
				}
				nodeList.add(nodes.get(ptr));
				return nodeList;
			} else {
				return nodes;
			}
		}
	}
}

class RoundRobinPartition implements Partition {
	private List<String> nodes;
	private int nextNodePtr;
	private static final long serialVersionUID = 3L;
	
	public RoundRobinPartition(List<String> nodes, String reqNodes) throws Exception {
		// Since we are using round robin we need to keep a pointer of what node
		// to return, set it here
		nextNodePtr = 0;
		
		// reqnodes is either null (use all nodes) or a CSV of the nodes to use
		if(reqNodes != null) {
			//Create a new list to use 
			this.nodes = new ArrayList<String>();
			//System.out.println("[ROUNDROBIN] Identifying nodes: " + reqNodes);
			String[] tmpNodes = reqNodes.split(",");
			for(String n: tmpNodes) {
				//Make sure that the node the user requested is in the list of valid nodes
				if(nodes.contains(n)){
					this.nodes.add(n);
				} else {
					//The node they requested doesn't exist, throw an error
					System.out.println("[ROUNDROBIN] Requested node doesn't exist: " + n);
					throw new Exception("RoundRobinUnknownNode");
				}
			}
		} else {
			// If they didn't provide a list then all nodes go into the set
			this.nodes = new ArrayList<String>(nodes);
		}
	}
	public String explain() {
		String out = "\tNextNode: " + nodes.get(nextNodePtr) + "\n";
		for(String n : nodes) {
			out += "\t" + n  + "\n";
		}
		return out;
	}

	public List<String> initialize() {
		return nodes;
	}
	
	public String chooseInsertNode(String vals) {
		String tmp = nodes.get(nextNodePtr++);
		if(nextNodePtr >= nodes.size()){
			nextNodePtr = 0;
		}
		return tmp;
	}
	
	public List<String> chooseSelectNode(String whereClause) {
		return nodes;
	}
}

class Range implements Serializable{
	private int min;
	private int max;
	private String node;
	public Range(int min, int max) {
		this.min = min;
		this.max = max;
		this.node = "";
	}
	
	public void setNode(String node) {
		this.node = node;
	}
	public boolean isInRange(int no) {
		if (no > min && no <= max) {
			return true;
		} else {
			return false;
		}
	}
	
	public int getMin() {
		return min;
	}
	
	public int getMax() {
		return max;
	}
	
	public String getNode() {
		if(node.equals("")) {
			return null;
		}
		return node;
	}
}

class RangePartition implements Partition {
	private List<String> nodes;
	private String partAttr;
	private int partAttrPosn;
	private String attrType;
	private String declAttrs;
	private int[] distribution;
	private List <Range> rangeMap;
	private final String[] SUPPORTEDATTRTYPES = {"integer", "char", "decimal"};
	private static final long serialVersionUID = 4L;
	
	public RangePartition(List<String> nodes, String declAttrs, String partAttr, List<Range> rangeList) throws Exception {
		this.nodes = new ArrayList<String>();
		this.partAttr = partAttr;
		this.declAttrs = declAttrs;
		rangeMap = rangeList;
		if(nodes.size() < rangeList.size()) {
			throw new Exception("Range Exceed number of nodes");
		}
		
		// Assign RangList to all 
		for(int i = 0; i < rangeMap.size(); i++) {
			rangeMap.get(i).setNode(nodes.get(i));
			this.nodes.add(nodes.get(i));
		}
		
		// Keep track of where we choose to put the data
		distribution = new int[this.nodes.size()];
		//Init the dist
		for(int i = 0; i < this.nodes.size(); i++) {
			distribution[i] = 0;
		}
		
		// Now we need to look through the attribute list to make sure their hash attr is in the list
		String[] theAttrs = declAttrs.split(",");
		partAttrPosn = 0;
		for(String a: theAttrs) {
			a = a.trim();
			// The attr should look like "name type" so find that
			if(!a.contains(" ")){
				throw new Exception("RangePartitionInvalidAttributeDeclaration");
			}
			String[] tmp = a.split(" ");

			//Search for the match to the requested hash attr
			if(partAttr.equals(tmp[0])){
				this.partAttr = tmp[0];
				this.attrType = tmp[1].toLowerCase();
				//Make sure the hashing type is supported
				if(!Arrays.asList(SUPPORTEDATTRTYPES).contains(attrType)){
					throw new Exception("RangePartitionUnsupportedType");
				}
				break;
			}
			partAttrPosn++;
		}
		if(partAttr == null) {
			throw new Exception("RangePartitionInvalidRangeAttribute");
		}
	}
	public String explain() {
		return "";
	}

	public List<String> initialize() {
		return nodes;
	}
	
	private String selectNode(String attrValue) {
		if(attrType.equals("integer")){
			int val = Integer.parseInt(attrValue);
			for(int i = 0 ; i < rangeMap.size(); i++) {
				if(rangeMap.get(i).isInRange(val)) {
					return rangeMap.get(i).getNode();
				}
			}
			System.out.println("RangePartition No info matched");
			return nodes.get(0);
			//ptr = hashVal % nodes.size();
		} else if (attrType.equals("char")) {
			// TODO
			return nodes.get(0);
			//ptr = Math.abs(hashValue.hashCode()) % nodes.size();
		} else if (attrType.equals("decimal")) {
			// TODO
			return nodes.get(0);
			//float hashVal = Float.parseFloat(hashValue);
			//ptr = (int) (hashVal % nodes.size());
		} else {
			// NOTE: We already checked in the constructor if the type is valid so we can just ignore this
			System.out.println("[RANGEPARTITION] THIS SHOULD NEVER EVER EVER EVER EVER SHOW UP!");
			return nodes.get(0);
		}
	}

	public String chooseInsertNode(String vals) {
		String[] theVals = vals.split(",");
		//Our match *should* be the hashingAttrPosn index value!
		String attrValue = theVals[partAttrPosn];
		System.out.println("[RANGEPARTITION] given partition attribute (" + partAttr + ") is " + attrValue);
		return selectNode(attrValue);
	}
	
	public List<String> chooseSelectNode(String whereClause) {
		// only support = where clause
		if(whereClause == null || whereClause.equals("") || !whereClause.contains("=")) {
			return nodes;
		}
		else {
			//TODO: if the where clause looks like "i >= 1" we end up here, only thing that saves us
			//is the trim will look like "i >" is that legit enough or should we do more checks?
			String [] tmp = whereClause.split("=");
			String attr = tmp[0].trim();
			if(attr.equals(partAttr)) {
				List<String> nodeList = new ArrayList<String>();
				nodeList.add(selectNode(tmp[1].trim()));
				return(nodeList);
			}
			return nodes;
		}
	}
}