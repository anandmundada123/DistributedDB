package distributeddb;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

interface Partition {
	List<String> initialize();
	String chooseInsertNode();
	List<String> chooseSelectNode();
	String explain();
}

class RandomPartition implements Partition {
	private List<String> nodes;
	private int nodeNumber;

	public RandomPartition(List<String> nodes, int nodeNumber) throws Exception {
		this.nodes = new ArrayList<String>(nodes);
		this.nodeNumber = nodeNumber;
		if(nodes.size() < nodeNumber) {
			throw new Exception("BadPartitionSpecification");
		}
	}
	
	public String explain() {
		String out = "";
		for(String n : nodes) {
			out += "\t" + n  + "\n";
		}
		return out;
	}
	
	/**
	 * Return a list of nodes that need to have the create table query sent to
	 */
	public List<String> initialize() {
		//Check if we need to remove any nodes based on the number
		int numRemove = nodes.size() - nodeNumber;
		Random r = new Random();
		for(int i = numRemove; i > 0; i--) {
			// Pick a random node to remove from the list
			nodes.remove(r.nextInt(nodes.size()));
		}
		return nodes;
	}
	/**
	 * We randomly are inserting, so pick one node to return
	 */
	public String chooseInsertNode() {
		Random r = new Random();
		return nodes.get(r.nextInt(nodes.size()));
	}
	
	/**
	 * When looking for data we don't know where it would be so request from all
	 */
	public List<String> chooseSelectNode() {
		return nodes;
	}
}

class HashPartition implements Partition {
	private List<String> nodes;
	private int hashSize;
	
	public HashPartition(List<String> nodes, int hashSize) throws Exception {
		this.nodes = new ArrayList<String>(nodes);
		this.hashSize = hashSize;
		if(nodes.size() < hashSize) {
			throw new Exception("BadPartitionSpecification");
		}
	}
	public String explain() {
		return "";
	}

	public List<String> initialize() {
		return nodes;
	}
	public String chooseInsertNode() {
		return "";
	}
	public List<String> chooseSelectNode() {
		return null;
	}

}

class RoundRobinPartition implements Partition {
	private List<String> nodes;
	private int nodeNumber;
	
	public RoundRobinPartition(List<String> nodes, int nodeNumber) throws Exception {
		this.nodes = new ArrayList<String>(nodes);
		this.nodeNumber = nodeNumber;
		if(nodes.size() < nodeNumber) {
			throw new Exception("BadPartitionSpecification");
		}
	}
	public String explain() {
		return "";
	}

	public List<String> initialize() {
		return nodes;
	}
	public String chooseInsertNode() {
		return "";
	}
	public List<String> chooseSelectNode() {
		return null;
	}
}

class RangePartition implements Partition {
	private List<String> nodes;
	private String attr;
	
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
	public String chooseInsertNode() {
		return "";
	}
	public List<String> chooseSelectNode() {
		return null;
	}
}