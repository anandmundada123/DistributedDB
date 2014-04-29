package distributeddb;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.net.SocketAddress;

import org.apache.commons.logging.Log;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

public class TCPServerHandler extends SimpleChannelUpstreamHandler {
	  private Queue<List<Object>> queryQueue;
	  private Log LOG;
	  
	  public TCPServerHandler(Queue<List<Object>> ql, Log l) {
		  this.queryQueue = ql;
		  this.LOG = l;
	  }
	  
	  @Override
	  public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		  ChannelBuffer buf = (ChannelBuffer)e.getMessage();
		  String q = buf.toString(Charset.defaultCharset());
		  //Channel ch = e.getChannel();
		  //SocketAddress src = ch.getRemoteAddress();
		  //LOG.info("From '" + src.toString() + "', message '" + q + "'");
		  List<Object> tmp = new ArrayList<Object>();
		  tmp.add(ctx);
		  tmp.add(q);
		  queryQueue.add(tmp);
	  }

	  @Override
	  public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		  LOG.warn("[TCPSERVERHANDLER] Caught exception for channel: " + e.getChannel().toString());
		  e.getChannel().close();
	  }
	  
	  @Override
	  public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		  Channel ch = e.getChannel();
		  SocketAddress src = ch.getRemoteAddress();
		  LOG.info("[TCPSERVERHANDLER] New connection from " + src.toString());
	  }
	  
  }