package distributeddb;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

public class DDBUtil {
	static String getHostName() throws UnknownHostException {
		return InetAddress.getLocalHost().getHostName();
	}

	public static int findFreePort() throws IOException {
		ServerSocket server = new ServerSocket(0);
		int port = server.getLocalPort();
		server.close();
		return port;
	}
}
