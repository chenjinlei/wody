package java.wody;


/**
 * 
 * 日志相关工具类
 * @author Administrator
 *
 */
public class LogUtils {

	//private static Logger log = Logger.getLogger(RPC.class.getName());

    /**
     * 记录异常日志信息
     * 
     * @param e
     */
	//FIXME log4j
    public static void log(Throwable e) {
        e.printStackTrace();
        //log.debug(e.getMessage(), e);
    }

    /**
     * 输出日志
     * 
     * @param object
     */
    public static void log(Object object) {
        System.out.println(object);
        //log.debug(object);
    }
	
}
