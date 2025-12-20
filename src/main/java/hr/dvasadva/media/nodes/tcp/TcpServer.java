package hr.dvasadva.media.nodes.tcp;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hr.dvasadva.media.nodes.MediaNodes.Keys;
import hr.dvasadva.media.nodes.ZkClient;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class TcpServer implements Runnable {
	
    private final int port;
    private static Logger log = LoggerFactory.getLogger(TcpServer.class);

    private EventLoopGroup bossGroup, workerGroup;
    private ChannelFuture channelFuture;
    
    private final ZkClient zkClient;
    
	public TcpServer(final Properties prop, final ZkClient zkClient) {

		this.zkClient = zkClient;
		
		this.port = Integer.parseInt(prop.getProperty(Keys.TCP_LISTENING_PORT.toString()));;
	
		this.channelFuture = null;
		this.bossGroup = null;
		this.workerGroup = null;
	}

	/**
	 * Start the TCP server interface.
	 */
	public void start() {
		
		if (port > 0) {
		
			final Thread thread = new Thread(this, String.format("TcpServer-[:%d]", port));
			thread.start();
		}
	}
	
	/**
	 * Terminate the TCP server instance.
	 * 
	 * @throws InterruptedException
	 */
	public void terminate() throws InterruptedException {
		
		if (this.channelFuture != null) {
			
			log.info("Terminating TCP server ...");
				
			this.channelFuture.channel().close().sync();
		}
		
		if (this.bossGroup != null && this.workerGroup != null) {
			
			this.bossGroup.shutdownGracefully();
			this.workerGroup.shutdownGracefully();
		}
	}
	
	@Override
    public void run() {
        
		this.bossGroup = new NioEventLoopGroup(1);
		this.workerGroup = new NioEventLoopGroup(4);
        
		try {
		
        	final ServerBootstrap serverBootstrap = new ServerBootstrap();
		
        	serverBootstrap.group(bossGroup, workerGroup)
	          .channel(NioServerSocketChannel.class)
	          .handler(new LoggingHandler(LogLevel.INFO))
	          .childHandler(new ChannelInitializer<Channel>() {
		        	  
		            @Override
		            protected void initChannel(final Channel ch) throws Exception {
		                
		            	final ChannelPipeline pipeline = ch.pipeline();
		                
		            	pipeline.addLast(new StringDecoder());
		                pipeline.addLast(new StringEncoder());
		                pipeline.addLast(new CustomTcpServerHandler(zkClient));
		            }
	          })
	          .option(ChannelOption.SO_BACKLOG, 128)
              .childOption(ChannelOption.SO_KEEPALIVE, true);
        	
            this.channelFuture = serverBootstrap.bind(port).sync();
        }
        catch (final Exception e) {
        	
			log.error("Can't start TCP server.", e);
		}
    }

}
