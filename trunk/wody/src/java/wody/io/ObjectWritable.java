package java.wody.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;
import java.wody.conf.Configurable;
import java.wody.conf.Configuration;

/** 一个多态的writable，会把实例和类名一起写出、读取
 *  处理数组、String、原始类型这几种没有Writable包装的类
 */

public class ObjectWritable implements Writable, Configurable {

	private Configuration conf;
	private Object instance;
	private Class declaredClass;
	
	public ObjectWritable(){}
	
	public ObjectWritable(Object instance){
		set(instance);
	}
	
	public ObjectWritable(Class declaredClass, Object instance){
		this.declaredClass = declaredClass;
		this.instance = instance;
	}
	
	public void set(Object instance) {
		this.instance = instance;
		this.declaredClass = instance.getClass();
	}
	
	public Object get(){
		return instance;
	}
	
	public String toString() {
	    return "OW[class=" + declaredClass + ",value=" + instance + "]";
	  }

	  
	  public void readFields(DataInput in) throws IOException {
	    readObject(in, this, this.conf);
	  }
	  
	  public void write(DataOutput out) throws IOException {
	    writeObject(out, instance, declaredClass, conf);
	  }

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;

	}

	private static final Map<String, Class<?>> PRIMITIVE_NAMES = new HashMap<String, Class<?>>();
	static {
		PRIMITIVE_NAMES.put("boolean", Boolean.TYPE);
		PRIMITIVE_NAMES.put("byte", Byte.TYPE);
		PRIMITIVE_NAMES.put("char", Character.TYPE);
		PRIMITIVE_NAMES.put("short", Short.TYPE);
		PRIMITIVE_NAMES.put("int", Integer.TYPE);
		PRIMITIVE_NAMES.put("long", Long.TYPE);
		PRIMITIVE_NAMES.put("float", Float.TYPE);
		PRIMITIVE_NAMES.put("double", Double.TYPE);
		PRIMITIVE_NAMES.put("void", Void.TYPE);
	}

	/**
	 * 将数据写到输出流中
	 * 
	 * @param out
	 * @param paramCls
	 * @param param
	 * @throws IOException
	 */
	
	public static void writeObject(DataOutput out, Object instance,
            Class declaredClass, Configuration conf) throws IOException {

		// FIXME how if param is null

		// 先写出类名
		UTF8.writeString(out, declaredClass.getName());

		// 判断是否为数组，若是，则递归调用本函数
		if (declaredClass.isArray()) {
			int length = Array.getLength(instance);
			out.writeInt(length);
			for (int i = 0; i < length; i++) {
				writeObject(out, Array.get(instance, i),
						declaredClass.getComponentType(), conf);
			}
		} else if (declaredClass == String.class) {
			UTF8.writeString(out, (String) instance);
		} else if (declaredClass.isPrimitive()) {

			if (declaredClass == Double.TYPE) {
				out.writeDouble(((Double) instance).doubleValue());
			} else if (declaredClass == Float.TYPE) {
				out.writeFloat(((Float) instance).floatValue());
			} else if (declaredClass == Integer.TYPE) {
				out.writeInt(((Integer) instance).intValue());
			} else if (declaredClass == Long.TYPE) {
				out.writeLong(((Long) instance).longValue());
			} else if (declaredClass == Short.TYPE) {
				out.writeShort(((Short) instance).shortValue());
			} else if (declaredClass == Byte.TYPE) {
				out.writeByte(((Byte) instance).byteValue());
			} else if (declaredClass == Boolean.TYPE) {
				out.writeBoolean(((Boolean) instance).booleanValue());
			} else if (declaredClass == Character.TYPE) {
				out.writeChar(((Character) instance).charValue());
			} else if (declaredClass == Void.TYPE) {

			} else {
				throw new IllegalArgumentException("Not a primitive: "
						+ declaredClass);
			}
		} else if (declaredClass.isEnum()) {
			UTF8.writeString(out, ((Enum) instance).name());
		} else if (Writable.class.isAssignableFrom(declaredClass)) {
			((Writable) instance).write(out);
		} else {
			throw new IOException("Can't write: " + instance + " as " + declaredClass);
		}
	}

	/**
	 * 
	 * 将数据从输入流中获取得到，这里一个需要解析出对象的类型 另一个需要解析出该对象实例的值
	 * 
	 * @param in
	 * @param conf
	 * @throws IOException
	 */
	public static Object readObject(DataInput in, ObjectWritable objectWritable, 
			Configuration conf) throws IOException {
	
		// 获取类名，通过反射等途径得到其对应的Class
		String clsName = UTF8.readString(in);

		Class<?> declaredClass = PRIMITIVE_NAMES.get(clsName);
		if (declaredClass == null) {
			// FIXME getClassByName is not implemented yet
			try {
				declaredClass = conf.getClassByName(clsName);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("readObject can't find class " + clsName, e);
			}
		}
		
		// 判断是否为基本类型
		Object instance;
		if(declaredClass.isPrimitive()){
			if(declaredClass == Integer.TYPE){
				instance = Integer.valueOf(in.readInt());
			} else if (declaredClass == Long.TYPE) {
				instance = Long.valueOf(in.readLong());
			} else if (declaredClass == Double.TYPE) {
				instance = Double.valueOf(in.readDouble());
			}else if (declaredClass == Float.TYPE) {
				instance = Float.valueOf(in.readFloat());
			}else if (declaredClass == Short.TYPE) {
				instance = Short.valueOf(in.readShort());
			}else if (declaredClass == Byte.TYPE) {
				instance = Byte.valueOf(in.readByte());
			}else if (declaredClass == Boolean.TYPE) {
				instance = Boolean.valueOf(in.readBoolean());
			}else if (declaredClass == Character.TYPE) {
				instance = Character.valueOf(in.readChar());
			}else if (declaredClass == Void.TYPE) {
				instance = null;
			}else {
		        throw new IllegalArgumentException("Not a primitive: "+declaredClass);
		    }
		} else if (declaredClass.isArray()){
			//读取数组长度  ，然后获取其中各个元素
			int len = in.readInt();
			instance = Array.newInstance(declaredClass.getComponentType(), len);
			for(int i = 0; i < len; i++){
				Array.set(instance, i, readObject(in, null, conf));
			}
		} else if (declaredClass == String.class){
			instance = UTF8.readString(in);
		} else if (declaredClass.isEnum()) {         // enum
			instance = Enum.valueOf((Class<? extends Enum>) declaredClass, 
					UTF8.readString(in));
		} else {

			// 由于在write中，对于Writable类型先写了instance.getClass.getName
			// 因此，先读取这个classname，然后通过反射获得对应的类
			String str = null;
			Class instanceClass;
			try {
				str = UTF8.readString(in);
				instanceClass = conf.getClassByName(str);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(
						"readObject can't find class " + str, e);
			}
			
			Writable writable = WritableFactories.newInstance(instanceClass, conf);
			writable.readFields(in);
			instance = writable;
		} 

		if (objectWritable != null) {                 // store values
		      objectWritable.declaredClass = declaredClass;
		      objectWritable.instance = instance;
		}
		return instance;
	}

	public Class getDeclaredClass() {
		return declaredClass;
	}
}
