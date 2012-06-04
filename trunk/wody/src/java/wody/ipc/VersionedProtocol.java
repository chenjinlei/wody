package java.wody.ipc;

import java.io.IOException;

public interface VersionedProtocol {

	public long getProtocolVersion(String protocol, 
            long clientVersion) throws IOException;
}
