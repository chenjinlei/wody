package java.wody.net;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class SocketOutputStream extends OutputStream implements
		WritableByteChannel {

	public SocketOutputStream(Socket socket, long timeout) {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void write(int b) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public int write(ByteBuffer src) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isOpen() {
		// TODO Auto-generated method stub
		return false;
	}

}
