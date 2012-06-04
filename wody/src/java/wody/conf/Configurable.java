package java.wody.conf;



/**
 * 使用configuration来对自身进行配置的类需要继承此接口
 * 
 * @author dongyu
 *
 */
public interface Configurable {

	
	/** 获取configuration 对象	 */
	public Configuration getConf();
	
	/** 设置configuration 对象	 */
	public void setConf(Configuration conf);
	
}
