package java.wody.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.wody.LogUtils;

public class UTF8 {

	private static final DataOutputBuffer OBUF = new DataOutputBuffer();
	
	/**
	 * Write a UTF-8 encoded string.
	 * 
	 * @see DataOutput#writeUTF(String)
	 */
	public static int writeString(DataOutput out, String s) throws IOException {
		if (s.length() > 0xffff / 3) { // maybe too long
			LogUtils.log("truncating long string: " + s.length()
					+ " chars, starting with " + s.substring(0, 20));
			s = s.substring(0, 0xffff / 3);
		}

		int len = utf8Length(s);
		if (len > 0xffff) // double-check length
			throw new IOException("string too long!");

		out.writeShort(len);
		writeChars(out, s, 0, s.length());
		return len;
	}

	/** Returns the number of bytes required to write this. */
	private static int utf8Length(String string) {
		int stringLength = string.length();
		int utf8Length = 0;
		for (int i = 0; i < stringLength; i++) {
			int c = string.charAt(i);
			if ((c >= 0x0001) && (c <= 0x007F)) {
				utf8Length++;
			} else if (c > 0x07FF) {
				utf8Length += 3;
			} else {
				utf8Length += 2;
			}
		}
		return utf8Length;
	}

	private static void writeChars(DataOutput out, String s, int start,
			int length) throws IOException {
		final int end = start + length;
		for (int i = start; i < end; i++) {
			int code = s.charAt(i);
			if (code >= 0x01 && code <= 0x7F) {
				out.writeByte((byte) code);
			} else if (code <= 0x07FF) {
				out.writeByte((byte) (0xC0 | ((code >> 6) & 0x1F)));
				out.writeByte((byte) (0x80 | code & 0x3F));
			} else {
				out.writeByte((byte) (0xE0 | ((code >> 12) & 0X0F)));
				out.writeByte((byte) (0x80 | ((code >> 6) & 0x3F)));
				out.writeByte((byte) (0x80 | (code & 0x3F)));
			}
		}
	}
	
	/** Read a UTF-8 encoded string.
	   *
	   * @see DataInput#readUTF()
	   */
	  public static String readString(DataInput in) throws IOException {
	    int bytes = in.readUnsignedShort();
	    StringBuffer buffer = new StringBuffer(bytes);
	    readChars(in, buffer, bytes);
	    return buffer.toString();
	  }

	  private static void readChars(DataInput in, StringBuffer buffer, int nBytes)
	    throws IOException {
	    synchronized (OBUF) {
	      OBUF.reset();
	      OBUF.write(in, nBytes);
	      byte[] bytes = OBUF.getData();
	      int i = 0;
	      while (i < nBytes) {
	        byte b = bytes[i++];
	        if ((b & 0x80) == 0) {
	          buffer.append((char)(b & 0x7F));
	        } else if ((b & 0xE0) != 0xE0) {
	          buffer.append((char)(((b & 0x1F) << 6)
	                               | (bytes[i++] & 0x3F)));
	        } else {
	          buffer.append((char)(((b & 0x0F) << 12)
	                               | ((bytes[i++] & 0x3F) << 6)
	                               |  (bytes[i++] & 0x3F)));
	        }
	      }
	    }
	  }
}
