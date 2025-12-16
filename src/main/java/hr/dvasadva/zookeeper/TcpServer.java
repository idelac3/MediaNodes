package hr.dvasadva.zookeeper;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hr.dvasadva.zookeeper.MediaNodes.Keys;

/**
 * Instance of TCP listener,
 * that will spawn new client thread, per connection.
 *
 */
public class TcpServer implements Runnable {
	
	private static final Logger log = LoggerFactory.getLogger(TcpServer.class);
	
	private boolean running;
	
	private final int port;
	
	private final ServerSocket serverSocket;
	
	private List<TcpClient> tcpClients;
	
	private final ZkClient zkClient;
	
	/**
	 * New server.
	 * 
	 * @param prop	program configuration
	 * 
	 * 
	 * @throws IOException	if port is used, or binding fails for any other reason
	 */
	public TcpServer(final Properties prop, final ZkClient zkClient) throws IOException {
	
		this.port = Integer.parseInt(prop.getProperty(Keys.TCP_LISTENING_PORT.toString()));
		
		this.tcpClients = new ArrayList<>();
		
		this.serverSocket = new ServerSocket(port);
		
		running = false;
		
		this.zkClient = zkClient;
	}
	
	/**
	 * Start TCP listener / server.
	 */
	public void start() {
		
		final Thread serverThread = new Thread(this,
				String.format(":%d", this.port));
		serverThread.start();
	}
	
	/**
	 * Terminate TCP server.
	 * 
	 * @throws IOException	if IO error occurs
	 */
	public void terminate() throws IOException {
		
		log.info("Terminating TCP server ...");
		
		this.running = false;
		this.serverSocket.close();
		
		for (final TcpClient tcpClient : this.tcpClients) {
		
			tcpClient.terminate();
		}
	}

	@Override
	public void run() {
		
		running = true;				
		
		while (running == true) {
		
			try {
			
				final Socket clientSocket = serverSocket.accept();
				
				final TcpClient tcpClient = new TcpClient(clientSocket, zkClient);
				
				final Thread clientThread = new Thread(tcpClient,
						String.format("%s:%d", clientSocket.getInetAddress(), clientSocket.getPort()));
				clientThread.start();
		
				this.tcpClients.removeIf(TcpClient::isTerminated);
				
				this.tcpClients.add(tcpClient);
				
				log.info(String.format("Got connection from '%s:%d'.", clientSocket.getInetAddress(), clientSocket.getPort()));
			}
			catch (final IOException ioException) {
				
				this.running = false;
			}
		}
	}
}
