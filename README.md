# Media Nodes

This is simple project to build a Java service that will provide a list of connected Media Recorder nodes.

The Media Recorder instances register on the Zookeeper server instance. So does **Media Nodes** service. Once connected, other utilities might benefit by simply opening the TCP connection towards the **Media Nodes** service. The response will always be a plain-text list of registered instances, e.g. found in the **/media-recorder** znode.

## Build

Maven and the OpenJDK are required to build.

```bash
mvn package
```

The executable is in the **target/** folder.

## Usage

To start the application, using default settings, use:

```bash
java -jar target/media-nodes.jar
```

assuming that the executable JAR is left in the **target/** folder.

## Configuration arguments

To customize settings, use either env.variables, or program arguments.

Env. variables are: `ZOOKEEPER_SERVER` `TCP_LISTENING_PORT` and `MEDIA_RECORDER_ZNODE`.

To start program properly, you need to specify correct Zookeeper `ZOOKEEPER_SERVER` connection string, e.g. 

```bash
ZOOKEEPER_SERVER="192.168.0.1:2181" java -jar target/media-nodes.jar
```

This will tell **media-nodes** to establish connection to the running Zookeeper at host **192.168.0.1** and port **2181**.


To modify the listening TCP port, use `TCP_LISTENING_PORT` variable.

```bash
TCP_LISTENING_PORT="5111" java -jar target/media-nodes.jar
```

Program will bind to **5111** and server requests.

The `MEDIA_RECORDER_ZNODE` by default is **/media-recorder**, and you probably don't need to modify it.

Alternative is to use program arguments: `--zookeeper-server` `--tcp-listening-port` and `--media-recorder-znode`.

Program arguments override the env. variables, if specified.

| Env. variable          | Program argument         | Default value    |
| ---------------------- | ------------------------ | ---------------- |
| `ZOOKEEPER_SERVER`     | `--zookeeper-server`     |  localhost:2181  |
| `TCP_LISTENING_PORT`   | `--tcp-listening-port`   |  5001            |
| `MEDIA_RECORDER_ZNODE` | `--media-recorder-znode` |  /media-recorder |

The table shows default values, and corresponding program arguments.

## Make a simple test 

Assuming your running Zookeeper on local system, open the CLI tool.

```bash
bin/zkCli.sh
```

or use `-server` argument to tell CLI tool where to connect to.

Create node entry, like:

```
[zk: localhost:2181(CONNECTED) 0] ls /
[zookeeper]
[zk: localhost:2181(CONNECTED) 1] create /media-recorder
Created /media-recorder
[zk: localhost:2181(CONNECTED) 2] create /media-recorder/node1 1.1.1.1
Created /media-recorder/node1
[zk: localhost:2181(CONNECTED) 3] create /media-recorder/node2 2.2.2.2
Created /media-recorder/node2
[zk: localhost:2181(CONNECTED) 4] 
[zk: localhost:2181(CONNECTED) 4] quit
```

This example creates two subnodes in the **/media-recorder** znode.

Start the **media-nodes** service.

```bash
java -jar target/media-nodes.jar
```

and finally verify you get some data back:

```bash
nc localhost 5001

1.1.1.1
2.2.2.2
```

Press **Enter** key to get back to the prompt.

In short, we have create two nodes on the Zookeeper instance, with dummy IP addresses, e.g. *1.1.1.1* and *2.2.2.2*, then we verified that we got back them as a list using **media-nodes** service.



