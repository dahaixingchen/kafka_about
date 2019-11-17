import org.apache.log4j.Logger;
import org.junit.Test;

/**
 * @ClassName: TestLog4j
 * @Description: TODO
 * @Author chengfei
 * @Date 2019/11/17 12:16
 * @Version 1.0
 **/
public class TestLog4j {
    static Logger logger = Logger.getLogger(TestLog4j.class);
    public static void main(String[] args) {
        logger.warn("this is debug message");
        logger.error("dafs");
    }
    @Test
    public void test(){
        logger.warn("this is warn ");
    }
}
