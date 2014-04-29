package distributeddb;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;


public class TCPServer {
	  private final int port;
	  private final String host;
	  private Log LOG;
	  private Queue<List<Object>> queryQueue;
	  private ChannelPipeline myPipeline;
	  private Map<String, ChannelHandlerContext> hostMap;
	  private ChannelHandlerContext appMasterCh;

	  public TCPServer(int port, Log l) {
		  this.port = port;
		  this.host = "localhost";
		  this.LOG = l;
		  this.queryQueue = new ConcurrentLinkedQueue<List<Object>>();
		  this.hostMap = new HashMap<String, ChannelHandlerContext>();
		  this.appMasterCh = null;
	  }
	  
	  public TCPServer(String host, int port, Log l) {
		  this.port = port;
		  this.host = host;
		  this.LOG = l;
		  this.queryQueue = new ConcurrentLinkedQueue<List<Object>>();
		  this.hostMap = new HashMap<String, ChannelHandlerContext>();
		  this.appMasterCh = null;
	  }
	  /**
	   * Takes a query off the queryList and returns it.
	   * @return String which is a query, or null if empty
	   */
	  public String getNextQuery() {
		  List<Object> tmp = queryQueue.poll();
        if(tmp == null) {
            return null;
        } else {
            //List object should have 2 parts, the Channel and the Query
            if(tmp.size() != 2){
                LOG.warn("List returned from channel should be size 2");
                return null;
            }
            //Otherwise get components
            ChannelHandlerContext ctx = (ChannelHandlerContext)tmp.get(0);
            LOG.info("++++ CAN HANDLE STUFF: " + ctx.canHandleDownstream() + " AND " + ctx.canHandleUpstream());
            Channel ch = ctx.getChannel();
            String q = (String)tmp.get(1);
            LOG.info("From: " + ch.toString() + " query: " + q);
            return q;
        }
	  }
	  
	  /**
	   * Takes a host name like 'wah' and registers the channel so we can send
	   * it data directly using the channel.
	   * @param host string of host name
	   * @param ch Channel object to hold on to
	   */
	  public void registerHost(String host, ChannelHandlerContext ch) {
		  hostMap.put(host,  ch);
	  }

	  /**
	   * Register this channel as how to communicate with the appmaster
	   * @param ch
	   */
	  public void registerAppMaster(ChannelHandlerContext ch) {
		  appMasterCh = ch;
	  }
	  
	  public List<Object> getNextMessage() {
		while (true) {
			List<Object> tmp = queryQueue.poll();
			if(tmp == null) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					LOG.warn("Thread interrupted from sleep?" + e.getLocalizedMessage());
				}
				continue;
			} else {
				//List object should have 2 parts, the Channel and the Query
				if(tmp.size() != 2){
					LOG.warn("List returned from channel should be size 2");
					continue;
				}
				//Otherwise get components
				//Channel ch = (Channel)tmp.get(0);
				//String q = (String)tmp.get(1);
				//LOG.info("From: " + ch.toString() + " query: " + q);
				return tmp;
			}
		}
	  }
	  /**
	   * Sends the results back to the client
	   * @param ans - the string of the answer
	   * @return true if success, false if fail
	   */
	  public boolean sendResult(String ans) {
		  try {
			  CharSequence cs = (CharSequence)ans;
			  ChannelBuffer buf = ChannelBuffers.copiedBuffer(cs,  Charset.defaultCharset());
			  myPipeline.getChannel().write(buf);
		  } catch (Exception e) {
			  LOG.error("!! Failure to send results!");
			  return false;
		  }	  
		  return true;
	  }

	  /**
	   * Send a message string to a specific host using the channel
	   * @param host
	   * @param msg
	   * @return
	   */
	  public boolean sendHostMessage(String host, String msg) {
		  try {
			  //Find channel from hostMap
			  ChannelHandlerContext ctx = hostMap.get(host);
			  if(ctx == null) {
				  LOG.info("[TCPSERVER] Host doesn't exist for '" + host + "'");
				  return false;
			  }
			  
			  // Now format the string
			  CharSequence cs = (CharSequence)msg;
			  ChannelBuffer buf = ChannelBuffers.copiedBuffer(cs,  Charset.defaultCharset());
			  
			  // Send using the proper channel
			  LOG.info("[TCPSERVER.sendHostMessage] Sending to '" + host + "' msg: '" + msg + "'");
			  ctx.getChannel().write(buf);
		  } catch (Exception e) {
			  LOG.error("[TCPSERVER] Failure to send results!");
			  return false;
		  }	  
		  return true;
	  }
	  
	  public boolean sendCtxMessage(ChannelHandlerContext ctx, String msg) {
		  try {
			  
			  // Now format the string
			  CharSequence cs = (CharSequence)msg;
			  ChannelBuffer buf = ChannelBuffers.copiedBuffer(cs,  Charset.defaultCharset());
			  
			  // Send using the proper channel
			  LOG.info("[TCPSERVER.sendCtxMessage] Sending msg: '" + msg + "'");
			  ctx.getChannel().write(buf);
		  } catch (Exception e) {
			  LOG.error("[TCPSERVER] Failure to send message!");
			  return false;
		  }	  
		  return true;
		  
	  }
	  
	  public void run() {
		  ServerBootstrap bootstrap = new ServerBootstrap (
				  new NioServerSocketChannelFactory(
						  Executors.newCachedThreadPool(),
						  Executors.newCachedThreadPool()
						  )
				  );

		  bootstrap.setOption("reuseAddress", true);
		  bootstrap.setOption("child.keepAlive", true);
		  
		  Map<String, Object> opts = bootstrap.getOptions();
		  Iterator<Map.Entry<String, Object>> it = opts.entrySet().iterator();
		  
		  LOG.info("[TCPSERVER] Options:");
		  while(it.hasNext()) {
			  Map.Entry<String, Object> p = (Map.Entry<String, Object>)it.next();
			  LOG.info("[TCPSERVER] Key: " + p.getKey() + ": " + p.getValue());
		  }

		  
		  bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			  public ChannelPipeline getPipeline() throws Exception {
				  myPipeline = Channels.pipeline(new TCPServerHandler(queryQueue, LOG));
				  return myPipeline;
			  }
		  });
		  
		  bootstrap.bind(new InetSocketAddress(host, port));
	  }
	  
	  public void close() {
		  myPipeline.getChannel().close();
	  } 
  }