package java.wody.net;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;


public class NetUtils {

	/**
	 * 对于{@link Socket#connect(SocketAddress, int)}的一种替换
	 * 若是普通的socket，则直接调用socket.connect
	 * 若getChannel不是null，connect的实现使用了 Hadoop's selectors
	 * 
	 * 这么做的原因主要是为了避免sun的connect实现通过创建thread-local selectors
	 * 因为这样当这些selectors被关闭的时候hadoop就不能控制这些，从而可以
	 * 获取所有的可行的file descriptor而告终
	 * 
	 * This is a drop-in replacement for
	 * {@link Socket#connect(SocketAddress, int)}. In the case of normal sockets
	 * that don't have associated channels, this just invokes
	 * <code>socket.connect(endpoint, timeout)</code>. If
	 * <code>socket.getChannel()</code> returns a non-null channel, connect is
	 * implemented using Hadoop's selectors. This is done mainly to avoid Sun's
	 * connect implementation from creating thread-local selectors, since Hadoop
	 * does not have control on when these are closed and could end up taking
	 * all the available file descriptors.
	 * 
	 * @see java.net.Socket#connect(java.net.SocketAddress, int)
	 * 
	 */
	public static void connect(Socket socket, InetSocketAddress address, 
			int timeout) throws IOException {

		if (socket == null || address == null || timeout < 0) {
		      throw new IllegalArgumentException("Illegal argument for connect()");
		    }
		
		SocketChannel ch = socket.getChannel();
		if (ch == null) {
			// let the default implementation handle it.
			socket.connect(address, timeout);
		} else {
			//FIXME  没有去实现SocketIOWithTimeout.connect
			socket.connect(address, timeout);
			//SocketIOWithTimeout.connect(ch, address, timeout);
		}
		
	}

	public static OutputStream getOutputStream(Socket socket)
			throws IOException {
		return getOutputStream(socket, 0L);
	}

	/**
	 * 获得socket对应的输出流
	 * 
	 * 如果socket包含有一个SocketChannel,则返回SocketOutputStream 否则返回
	 * socket.getOutputStream,如果是这种情况，timeout被 忽略，写操作会wait知道数据available If the
	 * socket has an associated
	 * 
	 * @param socket
	 * @param timeout
	 * @return
	 * @throws IOException
	 */
	public static OutputStream getOutputStream(Socket socket, long timeout)
			throws IOException {
		return (socket.getChannel() == null) ? socket.getOutputStream()
				: new SocketOutputStream(socket, timeout);
	}

	public static InputStream getInputStream(Socket socket) throws IOException {
		return getInputStream(socket, 0L);
	}

	/**
	 * 获得socket对应的输入流
	 * 
	 * 如果socket包含有一个SocketChannel,则返回SocketInputStream 否则返回
	 * socket.getInputStream,如果是这种情况，timeout被 忽略，写操作会wait知道数据available If the
	 * socket has an associated
	 * 
	 * @param socket
	 * @param timeout
	 * @return
	 * @throws IOException
	 */
	public static InputStream getInputStream(Socket socket, long timeout)
			throws IOException {
		return (socket.getChannel() == null) ? socket.getInputStream()
				: new SocketInputStream(socket, timeout);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
