package distributeddb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

interface Partition extends Serializable {
	List<String> initialize();
	String chooseInsertNode(String vals);
	List<String> chooseSelectNode();
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
		Random r = new Random();
		for(int i = numRemove; i > 0; i--) {
			// Pick a random node to remove from the list
			nodes.remove(r.nextInt(nodes.size()));
		}
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
	public List<String> chooseSelectNode() {
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
	private final String[] SUPPORTEDATTRTYPES = {"integer", "text", "real"};
	
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
		for(String a: theAttrs) {
			a = a.trim();
			// The attr should look like "name type" so find that
			if(!a.contains(" ")){
				throw new Exception("HashPartitionInvalidAttributeDeclaration");
			}
			String[] tmp = a.split(" ");
			
			//Search for the match to the requested hash attr
			if(hashOn.equals(tmp[0])){
				hashingAttr = tmp[0];
				hashingType = tmp[1].toLowerCase();
				//Make sure the hashing type is supported
				if(!Arrays.asList(SUPPORTEDATTRTYPES).contains(hashingType)){
					throw new Exception("HashPartitionUnsupportedType");
				}
				break;
			}
			hashingAttrPosn++;
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
		} else if (hashingType.equals("text")) {
			ptr = Math.abs(hashValue.hashCode()) % nodes.size();
		} else if (hashingType.equals("real")) {
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
	public List<String> chooseSelectNode() {
		return nodes;
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
	
	public List<String> chooseSelectNode() {
		return nodes;
	}
}

class RangePartition implements Partition {
	private List<String> nodes;
	private String attr;
	private static final long serialVersionUID = 4L;
	
	public RangePartition(List<String> nodes, String attr, String args) {
		this.nodes = new ArrayList<String>(nodes);
		this.attr = attr;
	}
	public String explain() {
		return "";
	}

	public List<String> initialize() {
		return nodes;
	}
	public String chooseInsertNode(String vals) {
		return "";
	}
	public List<String> chooseSelectNode() {
		return null;
	}
}