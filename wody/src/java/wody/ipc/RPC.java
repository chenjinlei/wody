package java.wody.ipc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.wody.conf.Configurable;
import java.wody.conf.Configuration;
import java.wody.io.ObjectWritable;
import java.wody.io.UTF8;
import java.wody.io.Writable;

import javax.net.SocketFactory;

public class RPC {

	private static class Invocation implements Writable, Configurable {

		private String methodName;
		private Class[] paramClasses;
		private Object[] parameters;
		private Configuration config;

		public Invocation() {
		}

		public Invocation(Method method, Object[] args) {
			this.methodName = method.getName();
			this.paramClasses = method.getParameterTypes();
			this.parameters = args;
		}

		public String getMethodName() {
			return methodName;
		}

		public Class[] getParamClasses() {
			return paramClasses;
		}

		public Object[] getParameters() {
			return parameters;
		}

		@Override
		public void write(DataOutput out) throws IOException {

			// 先写method，然后是param数量 最后是param
			UTF8.writeString(out, methodName);
			out.writeInt(parameters.length);
			for (int i = 0; i < parameters.length; i++) {
				ObjectWritable.writeObject(out, parameters[i],
						paramClasses[i], config);
			}
		}

		@Override
		public void readFields(DataInput in) throws IOException {

			// 先读method，然后是param数量，最后具体param
			methodName = UTF8.readString(in);
		    parameters = new Object[in.readInt()];
		    paramClasses = new Class[parameters.length];
		    ObjectWritable objectWritable = new ObjectWritable();
		    for (int i = 0; i < parameters.length; i++) {
		    	parameters[i] = ObjectWritable.readObject(in, objectWritable, this.config);
		    	paramClasses[i] = objectWritable.getDeclaredClass();
		    }
		}

		public static Invocation read(DataInput in) throws IOException {
			Invocation invocation = new Invocation();
			invocation.readFields(in);
			return invocation;
		}

		public String toString() {
			StringBuffer buffer = new StringBuffer();
			buffer.append(methodName);
			buffer.append("(");
			for (int i = 0; i < parameters.length; i++) {
				if (i != 0)
					buffer.append(", ");
				buffer.append(parameters[i]);
			}
			buffer.append(")");
			return buffer.toString();
		}

		public Configuration getConf() {
			return config;
		}

		public void setConf(Configuration config) {
			this.config = config;
		}

	}
	
	private static class ClientCache {
		
		private Map<SocketFactory, Client> clients = 
			new HashMap<SocketFactory, Client>();
		
		//FIXME to find out why InetAddress is not used 
		//and what is socketfactory
		public synchronized Client getClient(SocketFactory sf, 
				Configuration conf){
			
			Client client = clients.get(sf);
			if(client == null){
				client = new Client(sf, conf);
				clients.put(sf, client);
			} else {
				client.incCount();
			}
			
			return client;
		}
		
		public synchronized Client getClient(Configuration conf){
			return getClient(SocketFactory.getDefault(), conf);
		}
	}
	
	private static ClientCache CLIENTS = new ClientCache();

	private static class Invoker implements InvocationHandler {

		private InetSocketAddress address;
		private Configuration config;
		private SocketFactory factory;

		public Invoker(InetSocketAddress address, Configuration config,
				SocketFactory factory) {
			this.address = address;
			this.config = config;
			this.factory = factory;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args)
				throws Throwable {
			
			long start = System.currentTimeMillis();
			
			// 把 method 与 args 封装成输入后，通过client进行发送
			Invocation invocation = new Invocation(method, args);
			Client client = CLIENTS.getClient(factory, config);
			ObjectWritable result = (ObjectWritable) client.call(invocation, 
					address, method.getDeclaringClass());
			
			long end = System.currentTimeMillis();
			System.out.println("{Invoker.invoke} method " + method.getName()
					+ " takes " + (end - start) + " ms");
			return result.get();
		}
	}

	/**
	 * 客户端获取远程代理
	 * 
	 */
	public static VersionedProtocol getProxy(Class<?> protocol,
			InetSocketAddress address, Configuration config,
			SocketFactory factory, long clientVersion) throws IOException {

		Invoker invoker = new Invoker(address, config, factory);

		VersionedProtocol vp = (VersionedProtocol) Proxy.newProxyInstance(
				protocol.getClassLoader(), new Class[] { protocol }, invoker);

		long serverVersion = vp.getProtocolVersion(protocol.getName(),
				clientVersion);
		if (serverVersion == clientVersion) {
			return vp;
		} else {
			throw new VersionMismatch(protocol.getName(), clientVersion,
					serverVersion);
		}
	}

	// FIXME
	public static Server getServer(Object object) {
		return null;
	}
	
	
	/**
	   * A version mismatch for the RPC protocol.
	   */
	  public static class VersionMismatch extends IOException {
	    private String interfaceName;
	    private long clientVersion;
	    private long serverVersion;
	    
	    /**
	     * Create a version mismatch exception
	     * @param interfaceName the name of the protocol mismatch
	     * @param clientVersion the client's version of the protocol
	     * @param serverVersion the server's version of the protocol
	     */
	    public VersionMismatch(String interfaceName, long clientVersion,
	                           long serverVersion) {
	      super("Protocol " + interfaceName + " version mismatch. (client = " +
	            clientVersion + ", server = " + serverVersion + ")");
	      this.interfaceName = interfaceName;
	      this.clientVersion = clientVersion;
	      this.serverVersion = serverVersion;
	    }
	    
	    /**
	     * Get the interface name
	     * @return the java class name 
	     *          (eg. org.apache.hadoop.mapred.InterTrackerProtocol)
	     */
	    public String getInterfaceName() {
	      return interfaceName;
	    }
	    
	    /**
	     * Get the client's preferred version
	     */
	    public long getClientVersion() {
	      return clientVersion;
	    }
	    
	    /**
	     * Get the server's agreed to version.
	     */
	    public long getServerVersion() {
	      return serverVersion;
	    }
	  }

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
