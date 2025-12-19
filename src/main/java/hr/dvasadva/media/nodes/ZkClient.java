package hr.dvasadva.media.nodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hr.dvasadva.media.nodes.MediaNodes.Keys;

/**
 * A Zookeeper client that will update list of nodes,
 * once the root znode, e.g. '/media-recorder' changes.
 */
public class ZkClient implements Runnable, Watcher {

	private static final Logger log = LoggerFactory.getLogger(ZkClient.class);
	
	private final Properties prop;
	
	private final List<byte[]> result;
	
	private ZookeeperConnection zkConn;
	
	private boolean watchInstalled;
	
	public ZkClient(final Properties prop) {
	
		this.prop = prop;
		
		this.result = new ArrayList<>();
		
		this.zkConn = null;
		this.watchInstalled = false;
	}
	
	/**
	 * Start the Zookeeper client instance.
	 */
	public void start() {
		
		final String connString = prop.getProperty(Keys.ZOOKEEPER_SERVER.toString());
		
		int pos = connString.indexOf(':');
		
		final String host;
		final int port;
		if (pos == -1) {
			
			host = connString;
			port = 2181;
		}
		else {
			
			host = connString.substring(0, pos);
			port = Integer.parseInt(connString.substring(pos + 1));
		}
		
		final TcpPortTest tester = new TcpPortTest(host, port, 10, 100);
		final boolean testResult = tester.test();
		if (testResult == false) {
			
			log.warn(String.format("Can't establish connection to '%s'.", connString));
		}
		
		final Thread zkClientThread = new Thread(this,
				String.format("ZkClient-[%s]", connString));
		zkClientThread.start();
	}
	
	/**
	 * Terminate the connection towards the Zookeeper.
	 * 
	 * @throws InterruptedException
	 */
	public void terminate() throws InterruptedException {
		
		if (this.zkConn != null) {
			
			log.info("Terminating ZkClient ...");
			this.zkConn.close();
		}
	}
	
	/**
	 * Clear the result list.
	 */
	public synchronized void clearResult() {
		
		this.result.clear();
	}
	
	/**
	 * Install new result list.
	 * 
	 * @param	newResult 	a list of media recorder nodes, registered to the Zookeeper
	 */
	private synchronized void setResult(final List<byte[]> newResult) {
		
		this.result.clear();
		this.result.addAll(newResult);
	}
	
	/**
	 * Fetch the result.
	 * 
	 * @return	list of media recorder nodes, registered to the Zookeeper
	 */
	public synchronized List<byte[]> getResult() {
		
		return new ArrayList<>(this.result);
	}
	
	@Override
	public void run() {
		
		try {
		
			final String connString = prop.getProperty(Keys.ZOOKEEPER_SERVER.toString());
			
			log.info(String.format("Connecting to the Zookeeper at %s ...", connString));
			
			this.zkConn = new ZookeeperConnection(connString, 1000 * ZookeeperDefault.DEFAULT_SESSION_TIMEOUT, this);

		}
		catch (final IOException ioException) {
			
			log.error(String.format("IOError: %s", ioException.getMessage()));
		}
	}

	private void getMediaRecorderNodeList(final String mediaRecorderZnode) {

		try {
			
			final List<String> zNodes = zkConn.getNodes(mediaRecorderZnode);
			
			final List<byte[]> result = new ArrayList<>();
			
			for (final String zNode : zNodes) {
				
				final byte[] data = zkConn.getData(
						String.format("%s/%s", mediaRecorderZnode, zNode));
				
				result.add(data);
			}
			
			clearResult();
			setResult(result);
			
			if (result.size() > 0) {
				
				final String list = result.stream()
						.map(String::new)
						.collect(Collectors.joining(", "));
				
				log.info(
						String.format("Media Recorder List: %s", list));
			}
		}
		catch (final KeeperException keeperException) {
			
			log.error("Can't read the znode list.", keeperException);
		} catch (final InterruptedException interruptedException) {
			
			log.error("Interrupted.", interruptedException);
		}
	}

	@Override
	public void process(WatchedEvent event) {
		
		//
		// If znode path is null, and evet type is None,
		// then consider such event as NULL event.
		//
		
		final boolean nullEvent = 
				Objects.equals(event.getType(), Watcher.Event.EventType.None)
					&& event.getPath() == null;
		
		
		// Log received event. Only skip NULL events.
		if (nullEvent == false) {
		
			log.info(String.format("Got Zookeeper event: '%s' path: '%s'.", event.getType(), event.getPath()));
		}
		
		// Reconnect, on session expired event.
		if (Objects.equals(KeeperState.Expired, event.getState()) == true) {
		
			if (this.zkConn != null) {
				
				try {
					
					this.zkConn.close();
					this.start();
					
					return;
				}
				catch (final Exception ex) {
					
					log.error(String.format("Can't terminate broken connection: %s", ex.getMessage()));
				}
				
			}
		}
		
		final String mediaRecorderZnode = prop.getProperty(Keys.MEDIA_RECORDER_ZNODE.toString());

		try {
			
			if (zkConn.exists(mediaRecorderZnode) == false) {
				
				zkConn.createNode(mediaRecorderZnode, (new Date()).toString().getBytes());
			}
			else {
				
				getMediaRecorderNodeList(mediaRecorderZnode);
			}
			
			if (this.watchInstalled == false) {
			
				zkConn.addWatcher(mediaRecorderZnode, (event0) -> getMediaRecorderNodeList(mediaRecorderZnode) );
				this.watchInstalled = true; // Don't install watch more than once.
				
				log.info(String.format("Watching '%s' znode.", mediaRecorderZnode));
			}
			else {
				
				log.warn(String.format("Already watching '%s' znode." , mediaRecorderZnode));
			}
		}		 
		catch (final KeeperException keeperException) {

			log.error("Can't establish connection.", keeperException);
		} catch (final InterruptedException interruptedException) {

			log.error("Interrupted.", interruptedException);
		}
	}
}
