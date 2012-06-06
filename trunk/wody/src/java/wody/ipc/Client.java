package java.wody.ipc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.wody.LogUtils;
import java.wody.conf.Configuration;
import java.wody.io.DataOutputBuffer;
import java.wody.io.Writable;
import java.wody.net.NetUtils;

import javax.net.SocketFactory;

public class Client {

	private int counter = 0;                            // counter for call ids
	private Hashtable<ConnectionID, Connection> connections = new Hashtable<ConnectionID, Connection>();
	
	private final int maxIdleTime; //connections will be culled if it was idle for 
    //maxIdleTime msecs
	private final int maxRetries; //the max. no. of retries for socket connections
	private boolean tcpNoDelay; // if T then disable Nagle's Algorithm
	private int pingInterval; // how often sends ping to the server in msecs

	final private static String PING_INTERVAL_NAME = "ipc.ping.interval";
	final static int DEFAULT_PING_INTERVAL = 60000; // 1 min
	final static int PING_CALL_ID = -1;

	private Class<? extends Writable> valueClass;   // class of call values
	  
	/**
	 * 主要作为一个存储输入参数和返回结果的数据结构
	 * 
	 * @author Administrator
	 */
	private class Call{
		
		int id;
		Writable param;
		Writable value;
		IOException error;
		boolean done;
		
		public Call(Writable param){
			this.param = param;
			synchronized (Client.this) {
				this.id = counter++;
			}
		}

		public void setValue(Writable value) {
			this.value = value;
			callComplete();
		}

		public void setException(IOException error) {
			this.error = error;
			callComplete();
		}

		public void callComplete() {
			this.done = true;
			notify();
		}

	}
	
	
	/**
	 * 标记一个connection，一个client中包含有多个connection
	 * 
	 * @author Administrator
	 */
	public static class ConnectionID{
		
		InetSocketAddress address;
		Class<?> protocol;
		
		private static final int PRIME = 16777619;
		
		public ConnectionID(InetSocketAddress address, Class<?> protocol){
			this.address = address;
			this.protocol = protocol;
		}

		public InetSocketAddress getAddress() {
			return address;
		}

		public Class<?> getProtocol() {
			return protocol;
		}

		//因为是要做key，所以需要重载equals 和 hashcode两个函数
		@Override
		public int hashCode() {
			return address.hashCode() + PRIME * System.identityHashCode(protocol);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			
			ConnectionID other = (ConnectionID) obj;
			return address.equals(other.address) && protocol == other.protocol;
		}
		
	}
	
	/**
	 * 
	 * 
	 * @author Administrator
	 */
	public class Connection extends Thread{

		private ConnectionID remoteId;
		private InetSocketAddress server;
		private Socket socket = null;
		private ConnectionHeader header;
		
		private Hashtable<Integer, Call> calls = 
			new Hashtable<Integer, Call>();
		
		private DataInputStream dis;
		private DataOutputStream dos;
		
		// 最后一次 I/O 活动的时间
		private AtomicLong lastActivity = new AtomicLong();
		// 只是connection是否closed
	    private AtomicBoolean shouldCloseConnection = new AtomicBoolean();  // indicate if the connection is closed
	    private IOException closeException; // close reason
		
		public Connection(ConnectionID remoteId) throws IOException {
			
			this.remoteId = remoteId;
			this.server = remoteId.getAddress();
			
			if(server.isUnresolved()){
				throw new UnknownHostException("unknown host: " + 
                        remoteId.getAddress().getHostName());
			}
			
			//这是干什么的？
			Class<?> protocol = remoteId.getProtocol();
			header = new ConnectionHeader(protocol == null ? null : protocol.getName());
		      
			
			this.setName("IPC Client (" + sf.hashCode() +") connection to " +
			          remoteId.getAddress().toString());
			this.setDaemon(true);
			
		}
		
		public void sendParam(Call call) {
			
		}


		public synchronized boolean addCall(Call call){
			calls.put(call.id, call);
			notify();
			return true;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			super.run();
		}
		
		/** This class sends a ping to the remote side when timeout on
	     * reading. If no failure is detected, it retries until at least
	     * a byte is read.
	     */
		private class PingInputStream extends FilterInputStream {
			/* constructor */
			protected PingInputStream(InputStream in) {
				super(in);
			}

			/*
			 * Process timeout exception if the connection is not going to be
			 * closed, send a ping. otherwise, throw the timeout exception.
			 */
			private void handleTimeout(SocketTimeoutException e)
					throws IOException {
				if (shouldCloseConnection.get() || !running.get()) {
					throw e;
				} else {
					sendPing();
				}
			}

			/**
			 * Read a byte from the stream. Send a ping if timeout on read.
			 * Retries if no failure is detected until a byte is read.
			 * 
			 * @throws IOException
			 *             for any IO problem other than socket timeout
			 */
			public int read() throws IOException {
				do {
					try {
						return super.read();
					} catch (SocketTimeoutException e) {
						handleTimeout(e);
					}
				} while (true);
			}

			/**
			 * Read bytes into a buffer starting from offset <code>off</code>
			 * Send a ping if timeout on read. Retries if no failure is detected
			 * until a byte is read.
			 * 
			 * @return the total number of bytes read; -1 if the connection is
			 *         closed.
			 */
			public int read(byte[] buf, int off, int len) throws IOException {
				do {
					try {
						return super.read(buf, off, len);
					} catch (SocketTimeoutException e) {
						handleTimeout(e);
					}
				} while (true);
			}
		}

		public void setupIOStream() {

			if (socket != null) {
				return;
			}

			short ioFailures = 0;
		    short timeoutFailures = 0;
		      
			try {
				while (true) {
					try {
						socket = sf.createSocket();
						this.socket.setTcpNoDelay(tcpNoDelay);
						NetUtils.connect(this.socket, remoteId.getAddress(),
								20000);
						this.socket.setSoTimeout(pingInterval);
						break;
					} catch (SocketTimeoutException toe) {
						/*
						 * The max number of retries is 45, which amounts to
						 * 20s*45 = 15 minutes retries.
						 */
						handleConnectionFailure(timeoutFailures++, 45, toe);
					} catch (IOException ie) {
						// maxRetries是一个默认值0，因此当timeoutFailures
						// 失败45次后，到这里就直接抛异常，终止
						handleConnectionFailure(ioFailures++, maxRetries, ie);
					}
				}

				this.dis = new DataInputStream(new BufferedInputStream(
						new PingInputStream(NetUtils.getInputStream(socket))));
				this.dos = new DataOutputStream(new BufferedOutputStream(
						NetUtils.getOutputStream(socket)));

				writeHeader();

				// update last activity time
				touch();

				// start the receiver thread after the socket connection has
				// been set up
				start();
			} catch (IOException e) {
				markClosed(e);
				close();
			}

		}
		
		/* Write the header for each connection
	     * Out is not synchronized because only the first thread does this.
	     */
		private void writeHeader() throws IOException {
			// Write out the header and version
			dos.write(Server.HEADER.array());
			dos.write(Server.CURRENT_VERSION);

			// Write out the ConnectionHeader
			DataOutputBuffer buf = new DataOutputBuffer();
			header.write(buf);

			// Write out the payload length
			int bufLen = buf.getLength();
			dos.writeInt(bufLen);
			dos.write(buf.getData(), 0, bufLen);
		}
	    
		/** Update lastActivity with the current time. */
		private void touch() {
			lastActivity.set(System.currentTimeMillis());
		}
		
		/** Handle connection failures
	     *
	     * If the current number of retries is equal to the max number of retries,
	     * stop retrying and throw the exception; Otherwise backoff 1 second and
	     * try connecting again.
	     *
	     * This Method is only called from inside setupIOstreams(), which is
	     * synchronized. Hence the sleep is synchronized; 
	     * the locks will be retained.
	     *
	     * @param curRetries current number of retries
	     * @param maxRetries max number of retries allowed
	     * @param ioe failure reason
	     * @throws IOException if max number of retries is reached
	     */
	    private void handleConnectionFailure(int curRetries, int maxRetries,
				IOException ioe) throws IOException {
			// close the current connection
			try {
				socket.close();
			} catch (IOException e) {
				LogUtils.log(e);
			}
			// set socket to null so that the next call to setupIOstreams
			// can start the process of connect all over again.
			socket = null;

			// throw the exception if the maximum number of retries is reached
			if (curRetries >= maxRetries) {
				throw ioe;
			}

			// otherwise back off and retry
			try {
				Thread.sleep(1000);
			} catch (InterruptedException ignored) {
			}

			LogUtils.log("Retrying connect to server: " + server
					+ ". Already tried " + curRetries + " time(s).");
		}
	    
		private synchronized void markClosed(IOException e) {
			if (shouldCloseConnection.compareAndSet(false, true)) {
				closeException = e;
				notifyAll();
			}
		}
	      
	      /** Close the connection. */
		private synchronized void close() {
			if (!shouldCloseConnection.get()) {
				LogUtils.log("The connection is not in the closed state");
				return;
			}

			// release the resources
			// first thing to do;take the connection out of the connection list
			synchronized (connections) {
				if (connections.get(remoteId) == this) {
					connections.remove(remoteId);
				}
			}

			// close the streams and therefore the socket
			try {
				dos.close();
				dis.close();
			} catch (IOException e) {
				LogUtils.log(e);
			}
			

			// clean up all calls
			if (closeException == null) {
				if (!calls.isEmpty()) {
					LogUtils.log("A connection is closed for "
							+ "no cause and calls are not empty");

					// clean up calls anyway
					closeException = new IOException(
							"Unexpected closed connection");
					cleanupCalls();
				}
			} else {
				// cleanup calls
				cleanupCalls();
			}
		}
		
	    /* Cleanup all calls and mark them as done */
		private void cleanupCalls() {
			Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator();
			while (itor.hasNext()) {
				Call c = itor.next().getValue();
				c.setException(closeException); // local exception
				itor.remove();
			}
		}
		
	}
	
	
	private SocketFactory sf;
	private Configuration conf;
	
	public Client(SocketFactory sf,
			Configuration conf) {

	    this.valueClass = valueClass;
	    this.maxIdleTime = 
	      conf.getInt("ipc.client.connection.maxidletime", 10000); //10s
	    this.maxRetries = conf.getInt("ipc.client.connect.max.retries", 10);
	    this.tcpNoDelay = conf.getBoolean("ipc.client.tcpnodelay", false);
	    this.pingInterval = conf.getInt(PING_INTERVAL_NAME, DEFAULT_PING_INTERVAL);
		
		this.sf = sf;
		this.conf = conf;
		
	}

	
	public void incCount() {
		// TODO Auto-generated method stub
		
	}

	public Writable call(Writable invocation, InetSocketAddress address,
			Class<?> protocol) throws InterruptedException, IOException{
	
		//构造call，发送请求
		Call call = new Call(invocation);
		Connection connection = getConnection(address, protocol, call);
		connection.sendParam(call);
		
		//轮询等待结果
		boolean interrupted = false;
		synchronized (call) {
			while(!call.done){
				try{
					call.wait();
				}catch(InterruptedException ie){
					interrupted = true;
				}
			}
			
			if(interrupted){
				Thread.currentThread().interrupt();
			}
			
			//获取结果返回,处理进行返回
			if(call.error != null){
				if(call.error instanceof RemoteException){
					call.error.fillInStackTrace();
					throw call.error;
				} else {
					throw wrapException(address, call.error);
				}
			}else{
				return call.value;
			}
		}
	}

	/**
	   * Take an IOException and the address we were trying to connect to
	   * and return an IOException with the input exception as the cause.
	   * The new exception provides the stack trace of the place where 
	   * the exception is thrown and some extra diagnostics information.
	   * If the exception is ConnectException or SocketTimeoutException, 
	   * return a new one of the same type; Otherwise return an IOException.
	   * 
	   * @param addr target address
	   * @param exception the relevant exception
	   * @return an exception to throw
	   */
	  private IOException wrapException(InetSocketAddress addr,
	                                         IOException exception) {
	    if (exception instanceof ConnectException) {
	      //connection refused; include the host:port in the error
	      return (ConnectException)new ConnectException(
	           "Call to " + addr + " failed on connection exception: " + exception)
	                    .initCause(exception);
	    } else if (exception instanceof SocketTimeoutException) {
	      return (SocketTimeoutException)new SocketTimeoutException(
	           "Call to " + addr + " failed on socket timeout exception: "
	                      + exception).initCause(exception);
	    } else {
	      return (IOException)new IOException(
	           "Call to " + addr + " failed on local exception: " + exception)
	                                 .initCause(exception);

	    }
	  }

	private Connection getConnection(InetSocketAddress address, 
			Class<?> protocol, Call call) throws IOException{
		
		Connection connection = null;
		ConnectionID cID = new ConnectionID(address, protocol);
		
		do{
			synchronized (connections) {
				connection = connections.get(cID);
				if(connection == null){
					connection = new Connection(cID);
					connections.put(cID, connection);
				}
			}
			
			//什么情况下，需要不断反复的进行调用
		} while (connection.addCall(call));
		
		connection.setupIOStream();
		return connection;
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}


}
