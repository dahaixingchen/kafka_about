import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.protocol.types.Field;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Map;

/**
 * @ClassName: TestJson
 * @Author chengfei
 * @Date 2020/11/26 20:24
 * @Description: TODO
 **/
public class TestJson {
    public static void main(String[] args) {
//        LocalDate now = LocalDate.now();
        System.out.println(System.currentTimeMillis());
//        System.out.println(LocalDateTime.now().toString().replace(".","-")
//                .replace(":","-"));
        String str = "{\"DESC\":\"asfdsdsf\",\"ID\":\"122\",\"IPHONE\":\"155858\",\"name\":\"cheng\"}";
//        Map<String,Object> parse = (Map) JSON.parse(str);
//        for (Map.Entry map :parse.entrySet()){
//            System.out.println(map.getKey());
//            System.out.println(map.getValue());
//        }
        use use = JSON.parseObject(str, use.class);
        System.out.println(use.toString());

    }

    static class use{
        private int id;
        private String name;
        private long iphone;
        private String desc;

        public use() {
        }

        @Override
        public String toString() {
            return "use{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", iphone=" + iphone +
                    ", desc='" + desc + '\'' +
                    '}';
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getIphone() {
            return iphone;
        }

        public void setIphone(long iphone) {
            this.iphone = iphone;
        }

        public String getDesc() {
            return desc;
        }

        public void setDesc(String desc) {
            this.desc = desc;
        }
    }

}
