package hr.dvasadva.media.nodes;

import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * Test connectivity towards <i>host:port</i>.
 * This is only for TCP protocol.
 */
public class TcpPortTest {

	/**
	 * Host IP address to try TCP connection.
	 */
	private final String host;
	
	/**
	 * TCP port to test.
	 */
	private final int port;
	
	private final int maxRetry, pause;
	
	/**
	 * Create new tester instance.
	 * 
	 * 
	 * @param host	host DNS or IP address
	 * @param port	TCP port to test
	 * @param maxRetry	maximum number of retries, until giving up
	 * @param pause	pause between retries, in millisec., if 0 the socket timeout is disabled
	 */
	public TcpPortTest(String host, int port,
			int maxRetry, int pause) {
	
		this.host = host;
		this.port = port;
		
		this.maxRetry = maxRetry;
		this.pause = pause;
	}
	
	public String getHost() {
		
		return host;
	}
	
	public int getPort() {
		
		return port;
	}
	
	/**
	 * Start the TCP connection test.
	 * 
	 * @return	true if the host is responding, or false is connectivity problem occurs
	 */
	public boolean test() {

		int retry = this.maxRetry;
		
		boolean result = false;
		
		while (retry > 0) {
		
			try {
			
				final Socket socket = new Socket();

				socket.connect(new InetSocketAddress(host, port), pause);
				socket.close();
		
				result = true;
				
				retry = 0;
			}
			catch (final Exception ex) {
				
				retry--;
			}			
		}
		
		return result;
	}
	
}
