package hr.dvasadva.media.nodes;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main program.
 * 
 * Starts the TCP listener, Zookeeper client,
 * and install the shutdown callback.
 */
public class MediaNodes {

	/**
	 * Configuration keys, e.g. ZOOKEEPER_SERVER, TCP_LISTENING_PORT, etc. 
	 *
	 */
	public static enum Keys {
		ZOOKEEPER_SERVER("ZOOKEEPER_SERVER"),
		TCP_LISTENING_PORT("TCP_LISTENING_PORT"),
		MEDIA_RECORDER_ZNODE("MEDIA_RECORDER_ZNODE")
		;

		Keys(String string) {

		}
	};

	/**
	 * Access to program properties, like configuration variables.
	 */
	public static Properties prop = new Properties();
		
	private static final Logger log = LoggerFactory.getLogger(MediaNodes.class);
	
	public static void main(final String[] args) throws IOException {

		//
		// First configure the default values.
		//
		
		defaultConfiguration();		
		
		//
		// Read env. variables, to configure program parameters.
		//
		
		envConfiguration();
		
		//
		// Finally, use program arguments, to configure program parameters.
		//

		argConfiguration(args);
		
		//
		// Instantiate Zookeeper Client instance here.
		// It will connect to the Zookeeper, and have up-to-date list of registered media recorders.
		//
		
		final ZkClient zkClient = new ZkClient(prop);
		zkClient.start();
		
		//
		// Start here the TCP listener. It provides a list of registered media recorders via TCP socket.
		//
		
		final TcpServer tcpServer = new TcpServer(prop, zkClient);		
		tcpServer.start();
		
		//
		// Install SIGTERM, aka Ctrl+C hook to terminate properly.
		//
		
		Runtime.getRuntime().addShutdownHook(
				getShutdownThread(zkClient, tcpServer));
	}

	/**
	 * Configure the {@link Properties} instance with the default values.
	 */
	private static void defaultConfiguration() {

		prop.setProperty(Keys.ZOOKEEPER_SERVER.toString(), 
				ZookeeperDefault.DEFAULT_CONNECTION_STRING);
		
		prop.setProperty(Keys.TCP_LISTENING_PORT.toString(), "5001");
		
		prop.setProperty(Keys.MEDIA_RECORDER_ZNODE.toString(), "/media-recorder");
		
		final String defValues = prop.entrySet().stream()
				.map( (entry) -> String.format("%s=%s", entry.getKey(), entry.getValue()) )				
				.collect(Collectors.joining(", "));
		
		log.info(String.format("Using default values: %s", defValues));	
	}
	
	/**
	 * Configure the {@link Properties} instance from the system env. variables,
	 * if any configured.
	 */
	private static void envConfiguration() {
		
		for (final Keys key : Keys.values()) {
			
			final String envVariable = System.getenv(key.toString());
			if (envVariable != null) {
				
				prop.setProperty(key.toString(), envVariable);
				
				log.info(String.format("Using variable: %s=%s", key.toString(), envVariable));
			}
		}
	}
	
	/**
	 * Configure the {@link Properties} instance from the command line arguments.
	 * 
	 * @param args	list of command / program arguments
	 */
	private static void argConfiguration(final String[] args) {

		if (args.length > 0) {
			
			Keys currKey = null;
			
			for (final String arg : args) {
				
				if (arg.equals("-h") || arg.equals("--help")) {
					
					usage();
					
					return;
				}			
				else if (arg.equals("-z") || arg.equals("--zookeeper-server")) {
					
					currKey = Keys.ZOOKEEPER_SERVER;
				}
				else if (arg.equals("-t") || arg.equals("--tcp-listening-port")) {
					
					currKey = Keys.TCP_LISTENING_PORT;
				}
				else if (arg.equals("-m") || arg.equals("--media-recorder-znode")) {
					
					currKey = Keys.MEDIA_RECORDER_ZNODE;
				}
				else if (currKey != null) {
					
					if (arg.startsWith("-")) {
						
						log.warn(String.format("Detected invalid argument value: %s=%s", currKey, arg));
						
						currKey = null;
						
						continue;
					}
					
					prop.setProperty(currKey.toString(), arg);
					
					log.info(String.format("Using argument: %s=%s", currKey.toString(), arg));
					
					currKey = null;
				}
				else {
					
					log.error(
							String.format("Input argument '%s' is invalid.", arg));
					
					return;
				}
			}
		}		
	}
	
	/**
	 * Just build a thread to initiate a shutdown of a zookeeper client instance, and the TCP server
	 * instance.
	 * 
	 * @param zkClient	instance of zookeeper client
	 * @param tcpServer	instance of TCP server
	 * 
	 * @return	a {@link Thread} that will invoke shutdown
	 */
	private static Thread getShutdownThread(final ZkClient zkClient, final TcpServer tcpServer) {
		
		return new Thread( () -> {
			
			try {
			
				zkClient.terminate();
				tcpServer.terminate();
			}
			catch (final IOException | InterruptedException ex) {
				
				log.error("Can't shutdown properly.", ex);
			}
		});
	}
	
	private static void usage() {
		
		final PrintStream pr = System.out;
		
		final String usage = """
Program arguments:
				
				--zookeeper-server, -z		configure IP address and port of the Zookeeper service to connect to
				--tcp-listening-port, -t	bind and listen on TCP port
				--media-recorder-znode, -m	path to the znode on the Zookeeper where Media Recorders are registered
				
E.g.				  

				java -jar media-nodes.jar -z localhost:2181 -t 5001 -m /media-recorder
				
Environment variables:
				
				ZOOKEEPER_SERVER=localhost:2181 TCP_LISTENING_PORT=5001 MEDIA_RECORDER_ZNODE=/media-recorder java -jar media-nodes.jar
								 
Choose either option 1) or 2) to start program.				   
				""";
		
		pr.println(usage);		
	}
}
