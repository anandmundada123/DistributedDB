package distributeddb;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

/**
 * An ApplicationMaster for executing shell commands on a set of launched
 * containers using the YARN framework.
 * 
 * <p>
 * This class is meant to act as an example on how to write yarn-based
 * application masters.
 * </p>
 * 
 * <p>
 * The ApplicationMaster is started on a container by the
 * <code>ResourceManager</code>'s launcher. The first thing that the
 * <code>ApplicationMaster</code> needs to do is to connect and register itself
 * with the <code>ResourceManager</code>. The registration sets up information
 * within the <code>ResourceManager</code> regarding what host:port the
 * ApplicationMaster is listening on to provide any form of functionality to a
 * client as well as a tracking url that a client can use to keep track of
 * status/job history if needed.
 * </p>
 * 
 * <p>
 * The <code>ApplicationMaster</code> needs to send a heartbeat to the
 * <code>ResourceManager</code> at regular intervals to inform the
 * <code>ResourceManager</code> that it is up and alive. The
 * {@link ApplicationMasterProtocol#allocate} to the <code>ResourceManager</code> from the
 * <code>ApplicationMaster</code> acts as a heartbeat.
 * 
 * <p>
 * For the actual handling of the job, the <code>ApplicationMaster</code> has to
 * request the <code>ResourceManager</code> via {@link AllocateRequest} for the
 * required no. of containers using {@link ResourceRequest} with the necessary
 * resource specifications such as node location, computational
 * (memory/disk/cpu) resource requirements. The <code>ResourceManager</code>
 * responds with an {@link AllocateResponse} that informs the
 * <code>ApplicationMaster</code> of the set of newly allocated containers,
 * completed containers as well as current state of available resources.
 * </p>
 * 
 * <p>
 * For each allocated container, the <code>ApplicationMaster</code> can then set
 * up the necessary launch context via {@link ContainerLaunchContext} to specify
 * the allocated container id, local resources required by the executable, the
 * environment to be setup for the executable, commands to execute, etc. and
 * submit a {@link StartContainerRequest} to the {@link ContainerManagementProtocol} to
 * launch and execute the defined commands on the given allocated container.
 * </p>
 * 
 * <p>
 * The <code>ApplicationMaster</code> can monitor the launched container by
 * either querying the <code>ResourceManager</code> using
 * {@link ApplicationMasterProtocol#allocate} to get updates on completed containers or via
 * the {@link ContainerManagementProtocol} by querying for the status of the allocated
 * container's {@link ContainerId}.
 *
 * <p>
 * After the job has been completed, the <code>ApplicationMaster</code> has to
 * send a {@link FinishApplicationMasterRequest} to the
 * <code>ResourceManager</code> to inform it that the
 * <code>ApplicationMaster</code> has been completed.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationMaster {

	private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

	// Configuration
	private Configuration conf;

	// Handle to communicate with the Resource Manager
	@SuppressWarnings("rawtypes")
	private AMRMClientAsync resourceManager;

	// Handle to communicate with the Node Manager
	private NMClientAsync nmClientAsync;
	// Listen to process the response from the Node Manager
	private NMCallbackHandler containerListener;

	// Application Attempt Id ( combination of attemptId and fail count )
	private ApplicationAttemptId appAttemptID;

	// TODO
	// For status update for clients - yet to be implemented
	// Hostname of the container
	private String appMasterHostname = "";
	// Port on which the app master listens for status updates from clients
	private int appMasterRpcPort = 0;
	// Tracking url to which app master publishes info for clients to monitor
	private String appMasterTrackingUrl = "";

	// App Master configuration
	// No. of containers to run shell command on
	private int numTotalContainers = 1;
	// Memory to request for the container on which the shell command will run
	private int containerMemory = 10;
	// Priority of the request
	private int requestPriority;

	// Counter for completed containers ( complete denotes successful or failed )
	private AtomicInteger numCompletedContainers = new AtomicInteger();
	// Allocated container count so that we know how many containers has the RM
	// allocated to us
	private AtomicInteger numAllocatedContainers = new AtomicInteger();
	// Count of failed containers
	private AtomicInteger numFailedContainers = new AtomicInteger();
	// Count of containers already requested from the RM
	// Needed as once requested, we should not request for containers again.
	// Only request for more if the original requirement changes.
	private AtomicInteger numRequestedContainers = new AtomicInteger();

	// Query to be executed
	private String query = "";
	
	// Anand node where you want to launch container 
	private String nodeList = "";

	// NEW: Variables which will store client host name, port number
	// and appMaster Host name, port number
	private String clientHostName;
	private int clientPortNo;
	private String appMasterHost;
	private int appMasterPortNo;

	// Location of shell script ( obtained from info set in env )
	// Shell script path in fs
	private String shellDbScriptPath = "";
	private String shellWrapScriptPath = "";
	//private String shellContainerScriptPath = "";
	
	// Timestamp needed for creating a local resource
	private long shellDbScriptPathTimestamp = 0;
	private long shellWrapScriptPathTimestamp = 0;
	//private long shellContainerScriptPathTimestamp = 0;
	
	// File length needed for local resource
	private long shellDbScriptPathLen = 0;
	private long shellWrapScriptPathLen = 0;
	//private long shellContainerScriptPathLen = 0;
	
	// Hardcoded path to shell script in launch container's local env
	private final String ExecDbShellStringPath = DDBConstants.DB_SCRIPT_LOCATION;
	//private final String ExecWrapShellStringPath = DDBConstants.WRAP_SCRIPT_LOCATION;
	private final String ExecWrapShellStringPath = "cont_net.py";
	//private final String ExecContainerStringPath = DDBConstants.CONTAINER_SCRIPT_LOCATION;
	
	private volatile boolean done;
	private volatile boolean success;

	// Launch threads
	private List<Thread> launchThreads = new ArrayList<Thread>();

	/**
	 * @param args Command line args
	 */
	public static void main(String[] args) {

		try {
			ApplicationMaster appMaster = new ApplicationMaster();
			LOG.info("Initializing ApplicationMaster");
			boolean doRun = appMaster.init(args);
			if (!doRun) {
				System.exit(0);
			}
			appMaster.startAppMasterServer();

		} catch (Throwable t) {
			LOG.fatal("Error running ApplicationMaster", t);
			System.exit(1);
		}

	}

	private String getNextMsg(TCPServer tcpServer) {
		String query1 = null;
		while (true) {
			query1 = tcpServer.getNextQuery();
			if(query1 == null) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					LOG.warn("Thread interrupted from sleep?" + e.getLocalizedMessage());
				}
				continue;
			} else {
				query1 = query1.trim();
				break;
			}
		}
		return query1;
	}
	
	
	/**
	 * NEW: This function will send message to Client with host name and port number
	 * Then it will start a port in listen port.
	 * @throws YarnException
	 * @throws IOException
	 */
	private void startAppMasterServer() throws YarnException, IOException {
		appMasterHost = DDBUtil.getHostName();
		appMasterPortNo = DDBUtil.findFreePort();
		
		/*
		 * Start server
		 */
		
		// create client connection with Yarn-Client
		System.out.println("Client Host Name: " + clientHostName + " Client Port No: " + clientPortNo);
		// Start AM Server and if you get exception send exit message to client
		TCPServer tcpServer = null;
		try {
			tcpServer = new TCPServer(appMasterHost, appMasterPortNo, LOG);
			System.out.println("Starting AppMaster TCP Server on port " + appMasterPortNo);
			tcpServer.run();
		} catch (Exception e) {
			System.err.println(e.getLocalizedMessage());
			TCPClient client = new TCPClient(clientHostName, clientPortNo);
			client.init();
			client.sendMsg("exit");
			client.closeConnection();
			tcpServer.close();
			System.exit(-1);
		}
		
		/*
		 * NEW: Initialize resource manager and register Application manager to resource manager 
		 */
		initializeAppMaster();
		
		/* 
		 * Send message to client 
		 */
		
		String msg = DDBConstants.APP_MASTER_INFO + " " + appMasterHost + " " + appMasterPortNo;
		TCPClient client = new TCPClient(clientHostName, clientPortNo);
		client.init();
		client.sendMsg(msg);
		
		/*
		 * Launch container on master node
		 * FIXME: Here we have to launch containers on every node in future
		 */
		
		for(String n: nodeList.split(",")) {
			boolean result = false;
			result = run(n);
			if (result) {
				System.out.println("Container successfully launched on "+ n + "..");
			} else {
				System.out.println("Launching Container failed on " + n + "..");
				client.closeConnection();
				client = new TCPClient(clientHostName, clientPortNo);
				client.init();
				client.sendMsg("exit");
				//client.closeConnection();
				tcpServer.close();
				exitAppMaster();
				System.exit(0);
			}
		}
		
		System.out.println("waiting for query from client..");
		
		while (true) {
			// Wait to get query from Client
			msg = getNextMsg(tcpServer);
			System.out.println("Got Query:"+msg+":");
			if(msg.startsWith("exit")) {
				System.out.println("Exiting as got exit from Client");
				
				/*containerClient.closeConnection();
				containerClient = new TCPClient(contHost, contPort);
				containerClient.init();
				containerClient.sendMsg("exit");*/
				//containerClient.closeConnection();
	
				tcpServer.close();
				exitAppMaster();
				System.exit(0);
			} else {
				// Send Query to Container
				/*containerClient.closeConnection();
				containerClient = new TCPClient(contHost, contPort);
				containerClient.init();
				containerClient.sendMsg(msg);*/
				
				
				// Wait to get message from container
				/*msg = getNextMsg(tcpServer);
				System.out.println("Got reply from container: "+ msg);
				client.closeConnection();
				client = new TCPClient(clientHostName, clientPortNo);
				client.init();
				client.sendMsg(msg);*/
				// TODO Add code here to launch container on a given node
				System.out.println("Client is requested to launch container on "+ msg);
			}
		}
		
	}

	/**
	 * Dump out contents of $CWD and the environment to stdout for debugging
	 */
	private void dumpOutDebugInfo() {

		LOG.info("Dump debug output");
		Map<String, String> envs = System.getenv();
		for (Map.Entry<String, String> env : envs.entrySet()) {
			LOG.info("System env: key=" + env.getKey() + ", val=" + env.getValue());
			System.out.println("System env: key=" + env.getKey() + ", val="
					+ env.getValue());
		}

		String cmd = "ls -al";
		Runtime run = Runtime.getRuntime();
		Process pr = null;
		try {
			pr = run.exec(cmd);
			pr.waitFor();

			BufferedReader buf = new BufferedReader(new InputStreamReader(
					pr.getInputStream()));
			String line = "";
			while ((line = buf.readLine()) != null) {
				LOG.info("System CWD content: " + line);
				System.out.println("System CWD content: " + line);
			}
			buf.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public ApplicationMaster() throws Exception {
		// Set up the configuration and RPC
		conf = new YarnConfiguration();
	}

	/**
	 * Parse command line options
	 *
	 * @param args Command line args
	 * @return Whether init successful and run should be invoked
	 * @throws ParseException
	 * @throws IOException
	 */
	public boolean init(String[] args) throws ParseException, IOException {

		LOG.info("Starting ApplicationMaster INIT");
		Options opts = new Options();
		opts.addOption("app_attempt_id", true,
				"App Attempt ID. Not to be used unless for testing purposes");
		//opts.addOption("query", true,
		//  "Query to be executed by the Application Master");
		// Anand
		//opts.addOption("node", true,
		//      "Node where containers should be launch");
		opts.addOption("container_memory", true,
				"Amount of memory in MB to be requested to run the shell command");
		opts.addOption("num_containers", true,
				"No. of containers on which the shell command needs to be executed");
		opts.addOption("priority", true, "Application Priority. Default 0");
		opts.addOption("debug", false, "Dump out debug information");
		/**
		 * NEW: Appmaster will get Client Host Name and Client Port number
		 */
		opts.addOption(DDBConstants.CLIENT_HOST_NAME, true, "Client Host Name");
		opts.addOption(DDBConstants.CLIENT_PORT_NO, true, "Client port number");
		opts.addOption("nodes", true, "List of nodes where containers has to be launched");
		opts.addOption("help", false, "Print usage");
		CommandLine cliParser = new GnuParser().parse(opts, args);

		if (args.length == 0) {
			printUsage(opts);
			throw new IllegalArgumentException(
					"No args specified for application master to initialize");
		}

		if (cliParser.hasOption("help") || cliParser.hasOption("h")) {
			printUsage(opts);
			return false;
		}

		if (cliParser.hasOption("debug")) {
			dumpOutDebugInfo();
		}

		Map<String, String> envs = System.getenv();

		// TODO Anand Remove following comment
		if (!envs.containsKey(Environment.CONTAINER_ID.name())) {
			if (cliParser.hasOption("app_attempt_id")) {
				String appIdStr = cliParser.getOptionValue("app_attempt_id", "");
				appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
			} else {
				throw new IllegalArgumentException(
						"Application Attempt Id not set in the environment");
			}
		} else {
			ContainerId containerId = ConverterUtils.toContainerId(envs
					.get(Environment.CONTAINER_ID.name()));
			appAttemptID = containerId.getApplicationAttemptId();
		}

		if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
			throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
					+ " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_HOST.name())) {
			throw new RuntimeException(Environment.NM_HOST.name()
					+ " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
			throw new RuntimeException(Environment.NM_HTTP_PORT
					+ " not set in the environment");
		}
		if (!envs.containsKey(Environment.NM_PORT.name())) {
			throw new RuntimeException(Environment.NM_PORT.name()
					+ " not set in the environment");
		}

		LOG.info("Application master for app" + ", appId="
				+ appAttemptID.getApplicationId().getId() + ", clustertimestamp="
				+ appAttemptID.getApplicationId().getClusterTimestamp()
				+ ", attemptId=" + appAttemptID.getAttemptId());

		/* if (!cliParser.hasOption("query")) {
      throw new IllegalArgumentException(
          "No query specified to be executed by application master");
    }

    query = cliParser.getOptionValue("query");
    LOG.info("Received Query " + query);*/

		// NEW: get client host name and client port number

		if (!cliParser.hasOption(DDBConstants.CLIENT_HOST_NAME)) {
			throw new IllegalArgumentException(
					"No client host name is provided");
		}

		clientHostName = cliParser.getOptionValue(DDBConstants.CLIENT_HOST_NAME);
		LOG.info("Received Client Host Name" + clientHostName);

		if (!cliParser.hasOption(DDBConstants.CLIENT_PORT_NO)) {
			throw new IllegalArgumentException(
					"No client port number is provided");
		}

		clientPortNo = Integer.parseInt(cliParser.getOptionValue(DDBConstants.CLIENT_PORT_NO));
		LOG.info("Received Client port number" + clientPortNo);
		
		if (!cliParser.hasOption("nodes")) {
        throw new IllegalArgumentException(
            "No node is specified where we have to launch containers");
      }

      nodeList = cliParser.getOptionValue("nodes");
      LOG.info("Anand: Received node " + nodeList);

		//For the DB script
		if (envs.containsKey(DDBConstants.DDB_DB_LOCATION)) {
			shellDbScriptPath = envs.get(DDBConstants.DDB_DB_LOCATION);

			if (envs.containsKey(DDBConstants.DDB_DB_TIMESTAMP)) {
				shellDbScriptPathTimestamp = Long.valueOf(envs
						.get(DDBConstants.DDB_DB_TIMESTAMP));
			}
			if (envs.containsKey(DDBConstants.DDB_DB_LEN)) {
				shellDbScriptPathLen = Long.valueOf(envs
						.get(DDBConstants.DDB_DB_LEN));
			}

			if (!shellDbScriptPath.isEmpty()
					&& (shellDbScriptPathTimestamp <= 0 || shellDbScriptPathLen <= 0)) {
				LOG.error("Illegal values in env for shell script path" + ", path="
						+ shellDbScriptPath + ", len=" + shellDbScriptPathLen + ", timestamp="
						+ shellDbScriptPathTimestamp);
				throw new IllegalArgumentException(
						"Illegal values in env for shell script path");
			}
		}

		//For wrap script
		if (envs.containsKey(DDBConstants.DDB_WRAP_LOCATION)) {
			shellWrapScriptPath = envs.get(DDBConstants.DDB_WRAP_LOCATION);

			if (envs.containsKey(DDBConstants.DDB_WRAP_TIMESTAMP)) {
				shellWrapScriptPathTimestamp = Long.valueOf(envs
						.get(DDBConstants.DDB_WRAP_TIMESTAMP));
			}
			if (envs.containsKey(DDBConstants.DDB_WRAP_LEN)) {
				shellWrapScriptPathLen = Long.valueOf(envs
						.get(DDBConstants.DDB_WRAP_LEN));
			}

			if (!shellWrapScriptPath.isEmpty()
					&& (shellWrapScriptPathTimestamp <= 0 || shellWrapScriptPathLen <= 0)) {
				LOG.error("Illegal values in env for shell script path" + ", path="
						+ shellWrapScriptPath + ", len=" + shellWrapScriptPathLen + ", timestamp="
						+ shellWrapScriptPathTimestamp);
				throw new IllegalArgumentException(
						"Illegal values in env for shell script path");
			}
		}
		
		containerMemory = Integer.parseInt(cliParser.getOptionValue(
				"container_memory", "10"));
		numTotalContainers = Integer.parseInt(cliParser.getOptionValue(
				"num_containers", "1"));
		if (numTotalContainers == 0) {
			throw new IllegalArgumentException(
					"Cannot run distributed shell with no containers");
		}
		requestPriority = Integer.parseInt(cliParser
				.getOptionValue("priority", "0"));

		return true;
	}

	/**
	 * Helper function to print usage
	 *
	 * @param opts Parsed command line options
	 */
	private void printUsage(Options opts) {
		new HelpFormatter().printHelp("ApplicationMaster", opts);
	}

	private void initializeAppMaster() {
		startResourceManager();
		try {
			registerAppMaster();
		} catch (YarnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/*
	 * NEW: Function to start resource manager
	 */
	private void startResourceManager() {
		AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
		resourceManager =
				AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
		resourceManager.init(conf);
		resourceManager.start();
		
	}
	
	private void registerAppMaster() throws YarnException, IOException {
		RegisterApplicationMasterResponse response = resourceManager
				.registerApplicationMaster(appMasterHostname, appMasterRpcPort,
						appMasterTrackingUrl);
	}
	
	private void exitAppMaster() {
		
		// unregister application master
		try {
			resourceManager.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, null);
			LOG.info("Anand: Unregister Appmaster is done");
		} catch (YarnException ex) {
			LOG.error("Failed to unregister application", ex);
		} catch (IOException e) {
			LOG.error("Failed to unregister application", e);
		}
		
		// Stop Resource Manager		
		resourceManager.stop();
	}
	
	/**
	 * Main run function for the application master
	 *
	 * @throws YarnException
	 * @throws IOException
	 */
	@SuppressWarnings({ "unchecked" })
	public boolean run(String nodeStr) throws YarnException, IOException {
		
		/*
		 * FIXME: Un-comment following code  
		 */
		/*AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
		resourceManager =
				AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
		resourceManager.init(conf);
		resourceManager.start();
*/
		done = false;
		containerListener = new NMCallbackHandler();
		nmClientAsync = new NMClientAsyncImpl(containerListener);
		nmClientAsync.init(conf);
		nmClientAsync.start();

		for (int i = 0; i < numTotalContainers; ++i) {
			ContainerRequest containerAsk = setupContainerAskForRM(nodeStr);
			resourceManager.addContainerRequest(containerAsk);
			System.out.println("Container is launched on node = " + containerAsk.getNodes());
		}
		numRequestedContainers.set(numTotalContainers);

		/*while (!done) {
			try {
				Thread.sleep(200);
			} catch (InterruptedException ex) {}
		}
		finish();
*/
		return true;
	}

	private void finish() {
		// Join all launched threads
		// needed for when we time out
		// and we need to release containers
		for (Thread launchThread : launchThreads) {
			try {
				launchThread.join(10000);
			} catch (InterruptedException e) {
				LOG.info("Exception thrown in thread join: " + e.getMessage());
				e.printStackTrace();
			}
		}

		// When the application completes, it should stop all running containers
		LOG.info("Application completed. Stopping running containers");
		nmClientAsync.stop();

		// When the application completes, it should send a finish application
		// signal to the RM
		LOG.info("Application completed. Signalling finish to RM");

		FinalApplicationStatus appStatus;
		String appMessage = null;
		success = true;
		if (numFailedContainers.get() == 0 && 
				numCompletedContainers.get() == numTotalContainers) {
			appStatus = FinalApplicationStatus.SUCCEEDED;
		} else {
			appStatus = FinalApplicationStatus.FAILED;
			appMessage = "Diagnostics." + ", total=" + numTotalContainers
					+ ", completed=" + numCompletedContainers.get() + ", allocated="
					+ numAllocatedContainers.get() + ", failed="
					+ numFailedContainers.get();
			success = false;
		}
		
		/*
		 * FIXME: Remove following Comment
		 */
		/*try {
			resourceManager.unregisterApplicationMaster(appStatus, appMessage, null);
			LOG.info("Anand: Unregister Appmaster is done");
		} catch (YarnException ex) {
			LOG.error("Failed to unregister application", ex);
		} catch (IOException e) {
			LOG.error("Failed to unregister application", e);
		}
*/
		done = true;
		
		/*
		 * FIXME: Remove following comment
		 */
		
		// resourceManager.stop();
	}

	private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
		@SuppressWarnings("unchecked")
		@Override
		public void onContainersCompleted(List<ContainerStatus> completedContainers) {
			LOG.info("Got response from RM for container ask, completedCnt="
					+ completedContainers.size());
			for (ContainerStatus containerStatus : completedContainers) {
				LOG.info("Got container status for containerID="
						+ containerStatus.getContainerId() + ", state="
						+ containerStatus.getState() + ", exitStatus="
						+ containerStatus.getExitStatus() + ", diagnostics="
						+ containerStatus.getDiagnostics());

				// non complete containers should not be here
				assert (containerStatus.getState() == ContainerState.COMPLETE);

				// increment counters for completed/failed containers
				int exitStatus = containerStatus.getExitStatus();
				if (0 != exitStatus) {
					// container failed
					if (ContainerExitStatus.ABORTED != exitStatus) {
						// shell script failed
						// counts as completed
						numCompletedContainers.incrementAndGet();
						numFailedContainers.incrementAndGet();
					} else {
						// container was killed by framework, possibly preempted
						// we should re-try as the container was lost for some reason
						numAllocatedContainers.decrementAndGet();
						numRequestedContainers.decrementAndGet();
						// we do not need to release the container as it would be done
						// by the RM
					}
				} else {
					// nothing to do
					// container completed successfully
					numCompletedContainers.incrementAndGet();
					LOG.info("Container completed successfully." + ", containerId="
							+ containerStatus.getContainerId());
				}
			}

			// ask for more containers if any failed
			int askCount = numTotalContainers - numRequestedContainers.get();
			numRequestedContainers.addAndGet(askCount);

			if (askCount > 0) {
				for (int i = 0; i < askCount; ++i) {
					System.out.println("I came into askCount");
					//FIXME: Change this hard coded value 
					ContainerRequest containerAsk = setupContainerAskForRM("master");
					resourceManager.addContainerRequest(containerAsk);
				}
			}

			if (numCompletedContainers.get() == numTotalContainers) {
				done = true;
			}
		}

		@Override
		public void onContainersAllocated(List<Container> allocatedContainers) {
			LOG.info("Got response from RM for container ask, allocatedCnt="
					+ allocatedContainers.size());
			numAllocatedContainers.addAndGet(allocatedContainers.size());
			for (Container allocatedContainer : allocatedContainers) {
				LOG.info("Launching shell command on a new container."
						+ ", containerId=" + allocatedContainer.getId()
						+ ", containerNode=" + allocatedContainer.getNodeId().getHost()
						+ ":" + allocatedContainer.getNodeId().getPort()
						+ ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
						+ ", containerResourceMemory"
						+ allocatedContainer.getResource().getMemory());
				// + ", containerToken"
				// +allocatedContainer.getContainerToken().getIdentifier().toString());

				LaunchContainerRunnable runnableLaunchContainer =
						new LaunchContainerRunnable(allocatedContainer, containerListener);
				Thread launchThread = new Thread(runnableLaunchContainer);

				// launch and start the container on a separate thread to keep
				// the main thread unblocked
				// as all containers may not be allocated at one go.
				launchThreads.add(launchThread);
				launchThread.start();
			}
		}

		@Override
		public void onShutdownRequest() {
			done = true;
		}

		@Override
		public void onNodesUpdated(List<NodeReport> updatedNodes) {}

		@Override
		public float getProgress() {
			// set progress to deliver to RM on next heartbeat
			float progress = (float) numCompletedContainers.get()
					/ numTotalContainers;
			return progress;
		}

		@Override
		public void onError(Throwable e) {
			done = true;
			resourceManager.stop();
		}
	}

	private class NMCallbackHandler implements NMClientAsync.CallbackHandler {

		private ConcurrentMap<ContainerId, Container> containers =
				new ConcurrentHashMap<ContainerId, Container>();

		public void addContainer(ContainerId containerId, Container container) {
			containers.putIfAbsent(containerId, container);
		}

		@Override
		public void onContainerStopped(ContainerId containerId) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Succeeded to stop Container " + containerId);
			}
			containers.remove(containerId);
		}

		@Override
		public void onContainerStatusReceived(ContainerId containerId,
				ContainerStatus containerStatus) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Container Status: id=" + containerId + ", status=" +
						containerStatus);
			}
		}

		@Override
		public void onContainerStarted(ContainerId containerId,
				Map<String, ByteBuffer> allServiceResponse) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Succeeded to start Container " + containerId);
			}
			Container container = containers.get(containerId);
			if (container != null) {
				nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
			}
		}

		@Override
		public void onStartContainerError(ContainerId containerId, Throwable t) {
			LOG.error("Failed to start Container " + containerId);
			containers.remove(containerId);
		}

		@Override
		public void onGetContainerStatusError(
				ContainerId containerId, Throwable t) {
			LOG.error("Failed to query the status of Container " + containerId);
		}

		@Override
		public void onStopContainerError(ContainerId containerId, Throwable t) {
			LOG.error("Failed to stop Container " + containerId);
			containers.remove(containerId);
		}
	}

	/**
	 * Thread to connect to the {@link ContainerManagementProtocol} and launch the container
	 * that will execute the shell command.
	 */
	private class LaunchContainerRunnable implements Runnable {

		// Allocated container
		Container container;

		NMCallbackHandler containerListener;

		/**
		 * @param lcontainer Allocated container
		 * @param containerListener Callback handler of the container
		 */
		public LaunchContainerRunnable(
				Container lcontainer, NMCallbackHandler containerListener) {
			this.container = lcontainer;
			this.containerListener = containerListener;
		}

		@Override
		/**
		 * Connects to CM, sets up container launch context 
		 * for shell command and eventually dispatches the container 
		 * start request to the CM. 
		 */
		public void run() {
			
			/*System.out.println(Thread.currentThread().getStackTrace());
			System.out.println("Anand: \n");*/
			LOG.info("Setting up container launch container for containerid="
					+ container.getId());
			ContainerLaunchContext ctx = Records
					.newRecord(ContainerLaunchContext.class);

			// Set the local resources
			Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

			// The container for the eventual shell commands needs its own local
			// resources too.
			// copied and made available to the container.
			if (!shellDbScriptPath.isEmpty()) {
				LocalResource shellDbRsrc = Records.newRecord(LocalResource.class);
				shellDbRsrc.setType(LocalResourceType.FILE);
				shellDbRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
				try {
					shellDbRsrc.setResource(ConverterUtils.getYarnUrlFromURI(new URI(
							shellDbScriptPath)));
				} catch (URISyntaxException e) {
					LOG.error("Error when trying to use shell script path specified"
							+ " in env, path=" + shellDbScriptPath);
					e.printStackTrace();

					// A failure scenario on bad input such as invalid shell script path
					// We know we cannot continue launching the container
					// so we should release it.
					// TODO
					numCompletedContainers.incrementAndGet();
					numFailedContainers.incrementAndGet();
					return;
				}
				shellDbRsrc.setTimestamp(shellDbScriptPathTimestamp);
				shellDbRsrc.setSize(shellDbScriptPathLen);
				localResources.put(ExecDbShellStringPath, shellDbRsrc);
				System.out.println("Anand : resource URL " +shellDbRsrc.getResource());
			}

			if (!shellWrapScriptPath.isEmpty()) {
				LocalResource shellWrapRsrc = Records.newRecord(LocalResource.class);
				shellWrapRsrc.setType(LocalResourceType.FILE);
				shellWrapRsrc.setVisibility(LocalResourceVisibility.APPLICATION);
				try {
					shellWrapRsrc.setResource(ConverterUtils.getYarnUrlFromURI(new URI(
							shellWrapScriptPath)));
				} catch (URISyntaxException e) {
					LOG.error("Error when trying to use shell script path specified"
							+ " in env, path=" + shellWrapScriptPath);
					e.printStackTrace();

					// A failure scenario on bad input such as invalid shell script path
					// We know we cannot continue launching the container
					// so we should release it.
					// TODO
					numCompletedContainers.incrementAndGet();
					numFailedContainers.incrementAndGet();
					return;
				}
				shellWrapRsrc.setTimestamp(shellWrapScriptPathTimestamp);
				shellWrapRsrc.setSize(shellWrapScriptPathLen);
				localResources.put(ExecWrapShellStringPath, shellWrapRsrc);
				System.out.println("Anand : resource URL " +shellWrapRsrc.getResource());
			}
		
			ctx.setLocalResources(localResources);

			// Set the necessary command to execute on the allocated container
			//First command: python
			Vector<CharSequence> vargs = new Vector<CharSequence>(5);

			// Set executable command
			vargs.add("python");
			// Set shell script path
			if (!shellDbScriptPath.isEmpty()) {
				vargs.add(ExecWrapShellStringPath);
				System.out.println("Anand: Added Shell Script");
			}
			
			// NEW1:
			vargs.add(clientHostName);
			vargs.add(""+ clientPortNo);
			vargs.add(String.valueOf(appAttemptID.getApplicationId().getId()));
			
			// Add log redirect params
			vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
			vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

			// Get DB commmand
			StringBuilder dbCommand = new StringBuilder();
			for (CharSequence str : vargs) {
				dbCommand.append(str).append(" ");
			}
			LOG.info("DFW: Final DB Command: " + dbCommand.toString());

			List<String> commands = new ArrayList<String>();
			commands.add(dbCommand.toString());

			ctx.setCommands(commands);

			containerListener.addContainer(container.getId(), container);
			nmClientAsync.startContainerAsync(container, ctx);
		}
	}

	/**
	 * Setup the request that will be sent to the RM for the container ask.
	 *
	 * @param numContainers Containers to ask for from RM
	 * @return the setup ResourceRequest to be sent to RM
	 */
	/*
	 * TODO Anand May be you need to specify node in new container request 
	 * so pass that as argument to function
	 */
	private ContainerRequest setupContainerAskForRM(String node) {
		// setup requirements for hosts
		// using * as any host will do for the distributed shell app
		// set the priority for the request
		Priority pri = Records.newRecord(Priority.class);
		// TODO - what is the range for priority? how to decide?
		pri.setPriority(requestPriority);

		//** Anand start 
		String [] nodes = new String[1];
		nodes[0] = new String(node.toString()); 
		//** End

		// Set up resource type requirements
		// For now, only memory is supported so we set memory requirements
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(containerMemory);

		ContainerRequest request = new ContainerRequest(capability, nodes, null,
				pri, false);
		LOG.info("DFW: Requested container ask: " + request.toString());
		return request;
	}
}

