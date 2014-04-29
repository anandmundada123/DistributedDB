package distributeddb;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;


public class TCPClient extends SimpleChannelUpstreamHandler {

//private static final Logger logger = Logger.getLogger(TCPClient.class.getName());

public static Channel channel;
//private Queue<String> queryQueue = new ConcurrentLinkedQueue<String>();
private final String host;
private final int port;
public static  String response;

public TCPClient(int port) {
	this.host = "localhost";
	this.port = port;
	//queryQueue = new ConcurrentLinkedQueue<String>();
	response = "";
}

public TCPClient(String host, int port) {
	this.host = host;
	this.port = port;
	//queryQueue = new ConcurrentLinkedQueue<String>();
	response = "";
}

public boolean init() {
    // Configure the client.
    ClientBootstrap bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool()));
    // Set up the pipeline factory.
    bootstrap.setPipelineFactory(new TCPClientPipelineFactory());

    // Start the connection attempt.
    ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));

    // Wait until the connection is closed or the connection attempt fails.
    channel = future.awaitUninterruptibly().getChannel();
    
    // This is where the test write is <<------
    //ChannelFuture test = channel.write("test");
    if (!future.isSuccess()) {
        future.getCause().printStackTrace();
        bootstrap.releaseExternalResources();
        return false;
    }
    return true;
}

public String getNextMessage() {
	if(response.equals(""))
		return null;
	else {
		String tmp = response;
		response = "";
		return tmp;
	}
	 // return queryQueue.poll();
}

public void closeConnection() {
	channel.close();
}

@Override
public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
   // System.out.println("Message: " + e.getMessage());
  //  ChannelBuffer buf = (ChannelBuffer)e.getMessage();
   
	//String q = buf.toString(Charset.defaultCharset());
	String q = e.getMessage().toString();
    //queryQueue.add(q);
    response = q;
}

public void sendMsg(String str) {
	channel.write(str);
}
@Override
public void channelBound(ChannelHandlerContext ctx, ChannelStateEvent e) {
    System.out.println("Bound: " + e.getChannel().isBound());
}

@Override
public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
    System.out.println("Connected: " + e.getChannel().isConnected());
    System.out.println("Connected: " + e.getChannel().getRemoteAddress());
}

@Override
public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) {
    System.out.println("Closed: " + e.getChannel());
}

@Override
public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
    System.out.println("Disconnected: " + e.getChannel());
}

@Override
public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) {
    System.out.println("Open: " + e.getChannel().isOpen());
}

@Override
public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
    System.out.println("Error: " + e.getCause());
}


}