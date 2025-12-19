package hr.dvasadva.media.nodes;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpClient implements Runnable {
	
	private static final Logger log = LoggerFactory.getLogger(TcpClient.class);
	
	private final Socket clientSocket;
	private final ZkClient zkClient;
	
	public TcpClient(final Socket clientSocket, final ZkClient zkClient) {
	
		this.clientSocket = clientSocket;
		this.zkClient = zkClient;
	}
	
	public void terminate() throws IOException {
		
		this.clientSocket.close();
	}
	
	public boolean isTerminated() {
	
		return this.clientSocket != null && this.clientSocket.isClosed();				
	}
	
	@Override
	public void run() {
		
		int EOL = '\n';
		
		try (
				final OutputStream out = clientSocket.getOutputStream(); ) {
			
			for (final byte[] entry : zkClient.getResult()) {
			
				out.write(entry);
				out.write(EOL);
			}
		
			this.clientSocket.close();
		}
		catch (final IOException ioEx) {
			
			log.error(String.format("Can't communicate with the '%s:%d' client: %s", 
					this.clientSocket.getInetAddress(), this.clientSocket.getPort()),
					ioEx.getMessage());
		}
	}

}
