package hr.dvasadva.media.nodes.tcp;

import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hr.dvasadva.media.nodes.ZkClient;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class CustomTcpServerHandler extends SimpleChannelInboundHandler<String> {
	
	private static Logger log = LoggerFactory.getLogger(CustomTcpServerHandler.class);
	
	private final ZkClient zkClient;
	
	public CustomTcpServerHandler(final ZkClient zkClient) {
	
		this.zkClient = zkClient;
	}
	
	@Override
	public void channelReadComplete(final ChannelHandlerContext ctx) {
		
		ctx.flush();
	}

	@Override
	public void channelActive(final ChannelHandlerContext ctx) {

		final String responseData =
				this.zkClient.getResult().stream()
					.map(String::new)
					.collect(Collectors.joining("\n"));

		ctx.writeAndFlush(responseData).addListener(ChannelFutureListener.CLOSE);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		
		log.error(cause.getMessage());
		
		ctx.close();
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {

	}
}