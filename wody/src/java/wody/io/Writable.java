package java.wody.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;



/**
 * 可序列化对象需要实现的接口
 * 
 * 实现类一般同时实现一个静态的  <code>read(DataInput)</code>
 * 方法，通过调用readField构造一个该对象的实例
 * 
 * <p>Example:</p>
 * <p><blockquote><pre>
 *     public class MyWritable implements Writable {
 *       // Some data     
 *       private int counter;
 *       private long timestamp;
 *       
 *       public void write(DataOutput out) throws IOException {
 *         out.writeInt(counter);
 *         out.writeLong(timestamp);
 *       }
 *       
 *       public void readFields(DataInput in) throws IOException {
 *         counter = in.readInt();
 *         timestamp = in.readLong();
 *       }
 *       
 *       public static MyWritable read(DataInput in) throws IOException {
 *         MyWritable w = new MyWritable();
 *         w.readFields(in);
 *         return w;
 *       }
 *     }
 *     
 * @author dongyu
 *
 */
public interface Writable {
	
	/**
	 * 通过输入流读取一个对象的数据信息
	 * @param in
	 * @throws IOException
	 */
	public void readFields(DataInput in) throws IOException;
	
	/**
	 * 序列化一个对象到输出流
	 * @param out
	 * @throws IOException
	 */
	public void write(DataOutput out) throws IOException;
	
}
