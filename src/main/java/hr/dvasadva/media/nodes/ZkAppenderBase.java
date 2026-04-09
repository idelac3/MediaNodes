package hr.dvasadva.media.nodes;

import java.util.concurrent.BlockingQueue;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.status.WarnStatus;

/**
 * This is simple {@link Appender} that will store log event to the queue, e.g. {@link BlockingQueue}.
 *
 */
class ZkAppenderBase extends AppenderBase<ILoggingEvent> {

	private BlockingQueue<ILoggingEvent> queue;
	
	public ZkAppenderBase(final BlockingQueue<ILoggingEvent> queue) {
		
		this.queue = queue;
	}
	
	
	@Override
	protected void append(ILoggingEvent eventObject) {
	
		if (this.queue != null && eventObject != null) {
	
			try {
	
				queue.put(eventObject);
			} 
			catch (final InterruptedException interruptedException) {
				
				final String msg = "Interrupted while adding new logging event to the queue";
				
				addStatus(new WarnStatus(msg, eventObject, interruptedException));
			}
		}
	}

}
