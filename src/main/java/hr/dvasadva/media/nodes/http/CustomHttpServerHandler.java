package hr.dvasadva.media.nodes.http;

import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hr.dvasadva.media.nodes.ZkClient;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMessage;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;

public class CustomHttpServerHandler extends SimpleChannelInboundHandler<Object> {
	
	private static Logger log = LoggerFactory.getLogger(CustomHttpServerHandler.class);
	
	private HttpRequest request;

	private final ZkClient zkClient;
	
	public CustomHttpServerHandler(final ZkClient zkClient) {
	
		this.zkClient = zkClient;
		
		this.request = null;
	}
	
	@Override
	public void channelReadComplete(final ChannelHandlerContext ctx) {
		
		ctx.flush();
	}

	@Override
	protected void channelRead0(final ChannelHandlerContext ctx, Object msg) {

		if (msg instanceof HttpRequest) {
			
			final HttpRequest request = this.request = (HttpRequest) msg;

			if (HttpUtil.is100ContinueExpected((HttpMessage) request)) {
				
				writeResponse(ctx);
			}

		}

		if (msg instanceof HttpContent) {
	
			final StringBuilder responseData = new StringBuilder();
			responseData.append(
					this.zkClient.getResult().stream()
						.map(String::new)
						.collect(Collectors.joining("\n"))
					);
			
			if (msg instanceof LastHttpContent) {

				final LastHttpContent trailer = (LastHttpContent) msg;

				writeResponse(ctx, trailer, responseData);
			}
		}

	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		
		log.error(cause.getMessage());
		
		ctx.close();
	}

	private void writeResponse(ChannelHandlerContext ctx) {
		
		FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.CONTINUE,
				Unpooled.EMPTY_BUFFER);
		
		ctx.write(response);
	}

	private void writeResponse(ChannelHandlerContext ctx, LastHttpContent trailer, StringBuilder responseData) {
		
		boolean keepAlive = HttpUtil.isKeepAlive((HttpMessage) request);
		FullHttpResponse httpResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
				((HttpObject) trailer).decoderResult().isSuccess() ? HttpResponseStatus.OK : HttpResponseStatus.BAD_REQUEST,
				Unpooled.copiedBuffer(responseData.toString(), CharsetUtil.UTF_8));

		httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

		if (keepAlive) {
			httpResponse.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, httpResponse.content().readableBytes());
			httpResponse.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
		}
		
		ctx.write(httpResponse);

		if (!keepAlive) {
			
			ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
		}
	}
}