package java.wody.ipc;

import java.nio.ByteBuffer;
import java.wody.io.Writable;

public abstract class Server {

	/**
	 * The first four bytes of Hadoop RPC connections
	 */
	public static final ByteBuffer HEADER = ByteBuffer.wrap("hrpc".getBytes());

	// 1 : Introduce ping and server does not throw away RPCs
	// 3 : Introduce the protocol into the RPC connection header
	public static final byte CURRENT_VERSION = 3;

	protected abstract Writable call();

}
