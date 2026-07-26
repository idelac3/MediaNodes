package hr.dvasadva.media.nodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper.States;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
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
	
	private Map<String, Watcher> watchInstalled;
	
	public ZkClient(final Properties prop) {
	
		this.prop = prop;
		
		this.result = new ArrayList<>();
		
		this.zkConn = null;
		this.watchInstalled = new HashMap<>();
	}
	
	/**
	 * Start the Zookeeper client instance.
	 */
	public void start() {
		
		final String connString = prop.getProperty(Keys.ZOOKEEPER_SERVER.toString());
		
		final TcpPortTest tester = getTcpPortTester();
		
		final boolean testResult = tester.test();
		if (testResult == false) {
			
			log.warn(String.format("Can't establish connection to '%s'.", connString));
		}
		
		final Thread zkClientThread = new Thread(this,
				String.format("ZkClient-[%s]", connString));
		zkClientThread.start();
	}
	
	private TcpPortTest getTcpPortTester() {

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
		
		return tester;
	}
	
	/**
	 * Terminate the connection towards the Zookeeper.
	 * 
	 * @throws InterruptedException
	 */
	public void terminate() throws InterruptedException {
		
		if (this.zkConn != null && this.zkConn.isClosed() == false) {
			
			log.info("Terminating ZkClient ...");
			
			clearResult();
			
			clearInstalledWatches();
			
			this.zkConn.close();
		}
	}
	
	/**
	 * Check the Zookeeper client connection state,
	 * e.g. {@link States#CONNECTED} or {@link States#CLOSED}.
	 * 
	 * @return {@link ZookeeperConnection} state
	 */
	public States getState() {
		
		return this.zkConn.getState();
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
		
		//
		// Open the Zookeeper connection,
		// and pass the control to the:
		//
		//  public void process(WatchedEvent)
		//
		// method below.
		//
		
		try {
		
			final String connString = prop.getProperty(Keys.ZOOKEEPER_SERVER.toString());
			
			log.info(String.format("Connecting to the Zookeeper at %s ...", connString));
			
			this.zkConn = new ZookeeperConnection(connString, 1000 * ZookeeperDefault.DEFAULT_SESSION_TIMEOUT, this);

		}
		catch (final IOException ioException) {
			
			log.error(String.format("IO Error: %s", ioException.getMessage()));
		}
		
		//
		// Install the ZkBaseAppender, an special logging appender, that acts like a 
		//  callback function that will re-initiate a Zookeeper connection.
		//
		
		final BlockingQueue<ILoggingEvent> queue = new ArrayBlockingQueue<>(64);
		
		installZkAppender(queue);

		//
		// Main loop:
		//  - verify that the zk SendThread and EventThread threads are present
		//  - if present, then simply wait on the ZkAppender instance for new log event
		//  - if zk threads are gone, then check the connectivity and (re)start the Zk connection
		//
		
		String lastLogMsg = null;
		
		boolean running = true;
		while (running == true) {

			//
			// Dump the list of active threads.
			//
			
			final int activeThreads = Thread.activeCount();
			final Thread[] tArray = new Thread[activeThreads];
			Thread.enumerate(tArray);
			
			//
			// Look for something like 'ZkClient-[10.11.12.1]-SendThread(...)'
			//
			
			boolean foundZkClientSendThread = false, foundZkClientEventThread = false;
			
			for (final Thread thread : tArray) {
				
				if (thread == null) {
					
					continue;
				}
				
				final String threadName = thread.getName();
				
				if (threadName.startsWith("ZkClient") == true
						&& threadName.contains("SendThread") == true
						&& thread.isDaemon() == true) {
					
					foundZkClientSendThread = true;							
				}
				
				if (threadName.startsWith("ZkClient") == true
						&& threadName.contains("EventThread") == true
						&& thread.isDaemon() == true) {
					
					foundZkClientEventThread = true;							
				}
			}
			
			try {
				
				if (foundZkClientEventThread == false || foundZkClientSendThread == false) {
					
					//
					// If the Zk threads are gone, 
					//  then check the connectivity and (re)start the ZkClient instance.
					//
					
					TimeUnit.SECONDS.sleep(1);
					
					terminate(); // This invocation is idempotent, doesn't matter if called multiple times.
				
					//
					// Check the connectivity and start again ZkClient instance.
					//
					
					final TcpPortTest tester = getTcpPortTester();
					final boolean testResult = tester.test();
					if (testResult == true) {

						// Start new ZkClient thread.
						start();
						
						// Leave this thread, no need to keep it running anymore.
						break;
					}
					else {
						
						// Repeat here the main while(...) loop,
						// in hope that the connectivity will get back.
						continue;
					}
				}
				
				//
				// Wait for the log events, e.g. from the 'org.apache.zookeeper.ClientCnxn'.
				// Such events occur when the zk threads have been gone.
				//
					
				final ILoggingEvent event = queue.poll(1L, TimeUnit.SECONDS);
			
				if (event != null &&
						log instanceof ch.qos.logback.classic.Logger) {
				
					final ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger) log;
					
					final String logMsg = event.getFormattedMessage();
					
					if (SimilarString.compare(lastLogMsg, logMsg) == false) {
					
						lastLogMsg = logMsg;
						
						logger.callAppenders(event);
					}
				}
			}
			catch (final InterruptedException interruptedEx) {
			
				running = false;
			}
		}
	}

	private void clearInstalledWatches() {
			
		for (final Entry<String, Watcher> entry : this.watchInstalled.entrySet()) {
			
			final String path = entry.getKey();
			final Watcher watcher = entry.getValue();
			
			try {
			
				zkConn.removeWatcher(path, watcher);
			}
			catch (final KeeperException keeperEx) {
				
				log.error(String.format("Unable to remove installed watcher for '%s'.", path), keeperEx);
			}
			catch (final InterruptedException interruptedEx) {
				
				log.error(String.format("Interrupted while removing installed watcher for '%s'.", path));
			}
		}
		
		this.watchInstalled.clear();
		
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
		catch (final SessionExpiredException sessionExpiredException) {
			
			log.warn(sessionExpiredException.getMessage());
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
		
		//
		// Ignore events like session expired / closed / disconnected.
		//
		
		final List<KeeperState> closedStates = List.of(KeeperState.Expired, KeeperState.Disconnected, KeeperState.Closed);
		
		if (closedStates.contains(event.getState()) == true) {
		
			if (this.zkConn != null) {

				// We don't want to execute code below, that will fetch the node list
				// from the Zookeeper, in case the TCP connection is lost.
				
				// Upon re-establishment of the connection, 
				// the node list will refresh, anyway.
				return;
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
			
			if (Objects.equals(KeeperState.SyncConnected, event.getState()) == true) {
			
				clearInstalledWatches();
				
				final Watcher watcher = (event0) -> getMediaRecorderNodeList(mediaRecorderZnode);
				
				zkConn.addWatcher(mediaRecorderZnode, watcher);
				
				this.watchInstalled.put(mediaRecorderZnode, watcher);
				
				log.info(String.format("Watching '%s' znode.", mediaRecorderZnode));
			}
			else {
				
				log.warn(String.format("Got event '%s' for '%s' znode.", event.getState(), mediaRecorderZnode));
			}
		}		 
		catch (final KeeperException keeperException) {

			log.error("Can't establish connection.", keeperException);
		} catch (final InterruptedException interruptedException) {

			log.error("Interrupted.", interruptedException);
		}
	}
	
	public static void installZkAppender(final BlockingQueue<ILoggingEvent> queue) {
	
		//
		// Adjust the logging for the org.apache.zookeeper.* class(es).
		//
		
		ILoggerFactory factory = LoggerFactory.getILoggerFactory();
	
		if (factory instanceof LoggerContext) {
			
			final LoggerContext context = (LoggerContext) factory;

			final ZkAppenderBase zkAppender = new ZkAppenderBase(queue);
			zkAppender.setContext(context);
			zkAppender.start();
			
			final List<ch.qos.logback.classic.Logger> orgApacheLoggers = context.getLoggerList().stream()
				.filter(Objects::nonNull)
				.filter( (appender) -> appender.getName().startsWith("org.apache") == true )
				.toList();
			
			for (final ch.qos.logback.classic.Logger logger : orgApacheLoggers) {
				
				final Iterator<Appender<ILoggingEvent>> it = logger.iteratorForAppenders();
				
				boolean foundZkAppender = false;
				
				while (it.hasNext()) {
					
					final Appender<ILoggingEvent> appender = it.next();
				
					if (appender instanceof ZkAppenderBase) {
						
						foundZkAppender = true;
					}
					else {
					
						//appender.stop();
					
						logger.detachAppender(appender);					
					}
				}
				
				if (foundZkAppender == false) {
					
					logger.addAppender(zkAppender);
				}
			}
		}
	}
}
