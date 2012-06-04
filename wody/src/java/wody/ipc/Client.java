package java.wody.ipc;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.rmi.RemoteException;
import java.util.Hashtable;
import java.wody.conf.Configuration;
import java.wody.io.Writable;

import javax.net.SocketFactory;


public class Client {

	private int counter = 0;                            // counter for call ids
	private Hashtable<ConnectionID, Connection> connections = new Hashtable<ConnectionID, Connection>();
	
	
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
	public static class Connection{

		public void sendParam(Call call) {
			// TODO Auto-generated method stub
			
		}
		
		
		
	}
	
	
	private SocketFactory sf;
	private Configuration conf;
	
	public Client(SocketFactory sf,
			Configuration conf) {

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
			Class<?> protocol, Call call) {
		
		Connection connection = null;
		ConnectionID cID = new ConnectionID(address, protocol);
		
//		do{
//			synchronized (connections) {
//				connection = connections.get(cID);
//				if(connection == null){
//					connection = new Connection();
//					connections.put(cID, connection);
//				}
//			}
//		} while (connection.addCall(call));
		
//		connection.setupIOStream();
		return connection;
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}


}
