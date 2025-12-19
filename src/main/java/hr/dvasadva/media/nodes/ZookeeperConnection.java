package hr.dvasadva.media.nodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.ClientInfo;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

/**
 * Zookeeper connection is wrapper around {@link ZooKeeper} client instance.
 * Here are some of the commonly used methods, provided by {@link ZooKeeper} library.
 * 
 * This class refers to zNode as plain node ({@link String}).
 */
public class ZookeeperConnection implements AutoCloseable {

	private final ZooKeeper zookeeper; 
	
	private final String connectionString;
	
	/**
	 * New Zookeeper connection instance.
	 * For more information, see {@link ZooKeeper#ZooKeeper(String, int, Watcher)}.
	 * 
	 * @param connectionString		-	connection string, host:port, eg. localhost:2181
	 * @param sessionTimeout		-	session timeout in millisec.
	 * @param watcher				-	{@link Watcher} implementation,
	 * 									a watcher object which will be notified of state changes, 
	 * 									may also be notified for node events
	 * 
	 * @throws IOException	if connection is not possible due to network issues like broken connection 
	 * 		or firewall blocking access to ZooKeeper, etc.
	 */
	public ZookeeperConnection(final String connectionString, final int sessionTimeout
			, final Watcher watcher) throws IOException {
	
		this.connectionString = connectionString;
		
		this.zookeeper = new ZooKeeper(connectionString
				, sessionTimeout, watcher);		
	}
	
	/**
	 * List of nodes attached to the root element.
	 * 
	 * @return	list of nodes
	 * 
	 * @throws KeeperException 
	 * @throws InterruptedException
	 */
	public List<String> getNodes() throws KeeperException, InterruptedException {
		
		boolean watch = false;
		final List<String> nodes = zookeeper.getChildren("/", watch);
		
		return nodes;
	}
	
	/**
	 * List of nodes attached to the selected path.
	 * Note that paths are separated by slash '/' char.
	 * 
	 * @param	path		-	starting path
	 * 
	 * @return	list of nodes
	 * 
	 * @throws KeeperException 
	 * @throws InterruptedException
	 */
	public List<String> getNodes(final String path) throws KeeperException, InterruptedException {
		
		boolean watch = false;
		final List<String> nodes = zookeeper.getChildren(path, watch);
		
		return nodes;
	}

	/**
	 * Read data from the selected node.
	 * 
	 * @param path		-	path to the node
	 * 
	 * @return	data, as raw bytes, might be null or empty byte array
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public byte[] getData(final String path) throws KeeperException, InterruptedException {
		
		final Stat stat = new Stat();
		final byte[] data = 
				this.zookeeper.getData(path, false, stat);
		
		return data;
	}

	/**
	 * Update zNode data.
	 * 
	 * @param path	-	zNode path to update
	 * @param data	-	new data
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void setData(final String path, byte[] data) throws KeeperException, InterruptedException {
		
		int version = -1; // match any node version.
		
		this.zookeeper.setData(path, data, version);
	}

	/**
	 * Search for the given zNode
	 * 
	 * @param startingZnodePath			-	path on which to start visiting zNode tree
	 * @param visitedZnodePathCallback	-	callback function that will handle each zNode path that exists in current zNode tree
	 * 
	 * @throws KeeperException	If the server signals an error with a non-zero error code.
	 * @throws InterruptedException If the server transaction is interrupted.
	 */
	public void lookup(final String startingZnodePath
			, final AsyncCallback.StringCallback visitedZnodePathCallback) throws KeeperException, InterruptedException {
		
		boolean watch = false;
				
		ZKUtil.visitSubTreeDFS(this.zookeeper, startingZnodePath, watch, visitedZnodePathCallback);				
	}

	/**
	 * Check that zNode exists.
	 * 
	 * @param path			-	path of the zNode
	 * 
	 * @return	{@link Stat} instance, with zNode details
	 * 
	 * @throws KeeperException	If the server signals an error with a non-zero error code.
	 * @throws InterruptedException If the server transaction is interrupted.
	 */
	public boolean exists(final String path) throws KeeperException, InterruptedException {
		
		return this.getStat(path) != null;		
	}
	
	/**
	 * 
	 * Calculate the number of child zNodes.
	 * 
	 * @param path		-	zNode path
	 * 
	 * @return	how many child zNodes exist under the given zNode path
	 * 
	 * @throws KeeperException	If the server signals an error with a non-zero error code.
	 * @throws InterruptedException If the server transaction is interrupted.
	 */
	public int getAllChildrenCount(final String path) throws KeeperException, InterruptedException {
		
		return this.zookeeper.getAllChildrenNumber(path);
	}
	
	/**
	 * Add a watch to the given zNode using the given mode. 
	 * Note: not all watch types can be set with this method.
	 * Only the modes available in {@link AddWatchMode} can be set with this method.
	 *
	 * @param	path 		-	the path that the watcher applies to
	 * @param	watcher 	-	the watcher
	 * 
	 * @throws KeeperException	If the server signals an error with a non-zero error code.
	 * @throws InterruptedException If the server transaction is interrupted.
	 */
	public void addWatcher(final String path, final Watcher watcher) throws KeeperException, InterruptedException {
	
		this.zookeeper.addWatch(path, watcher, AddWatchMode.PERSISTENT);
	}
	
	/**
	 * For the given zNode path, removes the specified watcher of any given type, eg. {@link WatcherType#Any}.
	 * Watcher shouldn't be null. A successful call guarantees that, the removed watcher won't be triggered.
	 * 
	 * @param	path 	- the path of the zNode
	 * @param	watcher - a concrete watcher
	 * 
	 * @throws KeeperException	If the server signals an error with a non-zero error code.
	 * @throws InterruptedException If the server transaction is interrupted.
	 */
	public void removeWatcher(final String path, final Watcher watcher) throws InterruptedException, KeeperException {
		
		boolean local = true; // No need for real connection to the server.
		
		this.zookeeper.removeWatches(path, watcher, WatcherType.Any, local);
	}
	/**
	 * Create new zNode, eg. persistent, ephemeral or container.
	 * For more info about the zNode type, refer to the {@link CreateMode}.
	 * 
	 * @param 	path	-	path to the node, note that parent zNodes should exist
	 * @param	data	-	assign data to the zNode, note that limit is 1 MB
	 * @param	createMode		-	one of {@link CreateMode} values
	 * 
	 * @throws KeeperException	If the server signals an error with a non-zero error code.
	 * @throws InterruptedException If the server transaction is interrupted.
	 */
	public void createNode(final String path
			, final byte[] data
			, final CreateMode createMode) throws KeeperException, InterruptedException {

		final List<ACL> acl = new ArrayList<>();
		acl.add(ZookeeperConnection.anyoneACL());
		
		this.zookeeper.create(path, data, acl, createMode);
	}	
	
	/**
	 * Create new persistent zNode.
	 * 
	 * @param 	path	-	path to the node, note that parent zNodes should exist
	 * @param	data	-	assign data to the zNode, note that limit is 1 MB
	 * 
	 * @throws KeeperException	If the server signals an error with a non-zero error code.
	 * @throws InterruptedException If the server transaction is interrupted.
	 */
	public void createNode(final String path
			, final byte[] data) throws KeeperException, InterruptedException {
	
		createNode(path, data, CreateMode.PERSISTENT);
	}

	/**
	 * Delete zNode.
	 * 
	 * @param path		-	path to the zNode
	 * 
	 * @throws KeeperException	If the server signals an error with a non-zero error code.
	 * @throws InterruptedException If the server transaction is interrupted.
	 */
	public void deleteNode(final String path) throws InterruptedException, KeeperException {
		
		int version = -1;
		
		this.zookeeper.delete(path, version);
	}

	/**
	 * Check that zNode exists and fetch {@link Stat} information.
	 * 
	 * @param path		-	path to the zNode
	 * 
	 * @return	{@link Stat} object
	 * 
	 * @throws KeeperException	If the server signals an error with a non-zero error code.
	 * @throws InterruptedException If the server transaction is interrupted.
	 */
	
	public Stat getStat(final String path) throws KeeperException, InterruptedException {
	
		boolean watch = false;
				
		return this.zookeeper.exists(path, watch);
	}
	
	/**
	 * List all authentication information.
	 * 
	 * @return	list of client auth. information
	 * 
	 * @throws InterruptedException If the server transaction is interrupted.
	 */
	public List<ClientInfo> whoAmI() throws InterruptedException {
		
		return this.zookeeper.whoAmI();
	}
	
	/**
	 * Return client configuration.
	 * 
	 * @return {@link ZKClientConfig}
	 */
	public ZKClientConfig getClientConfig() {
		
		return this.zookeeper.getClientConfig();
	}
	
	/**
	 * Add authentication information, like user:password for the current session.
	 * 
	 * @param scheme	-	eg. <i>digest</i> or <i>ip</i>
	 * @param auth		-	raw bytes for authentication, eg. for digest scheme use user:password concatenated string, in clear-text
	 */
	public void addAuth(final String scheme, final byte[] auth) {
	
		this.zookeeper.addAuthInfo(scheme, auth);
	}
	
	/**
	 * Return the ACL of the node of the given path.
	 * 
	 * @param path		-	zNode path
	 * 
	 * @return	list of {@link ACL} permissions
	 * 
	 * @throws KeeperException	If the server signals an error with a non-zero error code.
	 * @throws InterruptedException If the server transaction is interrupted.
	 */
	public List<ACL> getPermissions(final String path) throws KeeperException, InterruptedException {
		
		return this.zookeeper.getACL(path, null);
	}
	
	/**
	 * Apply the list of ACL permissions to the zNode.
	 * 
	 * @param path					-	path to the zNode
	 * @param permissions			-	list of {@link ACL} permission objects
	 * 
	 * @throws KeeperException	If the server signals an error with a non-zero error code.
	 * @throws InterruptedException If the server transaction is interrupted.
	 */
	public void setPermissions(final String path, final List<ACL> permissions) throws KeeperException, InterruptedException {
		
		int version = -1; // match any node version.
		
		this.zookeeper.setACL(path, permissions, version);
	}
	
	/**
	 * Create world-wide ACL, that permits everyone to access and modify zNode.
	 * 
	 * @return	{@link ACL} that gives all permissions to everyone
	 */
	public static ACL anyoneACL() {
		
		final int perms = ZooDefs.Perms.ALL;
		
		final Id id = ZooDefs.Ids.ANYONE_ID_UNSAFE;
		
		return new ACL(perms, id);
	}
	
	/**
	 * Closes the Zookeeper connection.
	 */
	@Override
	public void close() throws InterruptedException {

		this.zookeeper.close();
	}
	/**
	 * Returns session identifier.
	 * 
	 * @return	session identifier.
	 */
	public long getSessionId() {

		return this.zookeeper.getSessionId();
	}

	/**
	 * Returns session password, if any.
	 * 
	 * @return	session password, as raw data
	 */
	public byte[] getSessionPassword() {

		return this.zookeeper.getSessionPasswd();
	}

	/**
	 * Returns session timeout, as configured by the user of this instance.
	 * 
	 * @return	session timeout, in milliseconds
	 */
	public int getSessionTimeout() {

		return this.zookeeper.getSessionTimeout();
	}
	
	/**
	 * Connection string used to connect to the ZooKeeper.
	 */
	@Override
	public String toString() {
		
		return this.connectionString;
	}

	/**
	 * Calculate the parent zNode.
	 * 
	 * @param path		-	input path, eg. /users/alice
	 * 
	 * @return	parent path, eg. /users for the input path /users/alice
	 */
	public static String getParent(final String path) {
		
		if (path == null 
				|| path.equals("/")) {
			
			return path;
		}
		
		if (path.length() == 0) {
		
			return "/";
		}
		
		int lastPos = path.lastIndexOf('/');
		
		if (lastPos == -1) {
			
			return path;
		}
		
		if (lastPos == 0) {
			
			return "/";
		}
		
		return path.substring(0, lastPos);
	}
}
