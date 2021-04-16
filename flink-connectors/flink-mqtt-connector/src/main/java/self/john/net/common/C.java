package self.john.net.common;

import java.util.Properties;

public class C {
    /**
     * 检车属性是否存在
     * @param p properties
     * @param key key
     */
    public static void checkProperty(Properties p, String key) {
        if (!p.containsKey(key)) {
            throw new IllegalArgumentException("Required property '" + key + "' not set.");
        }
    }
}
