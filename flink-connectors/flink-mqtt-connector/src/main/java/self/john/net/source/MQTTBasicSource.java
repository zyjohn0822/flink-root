package self.john.net.source;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import static self.john.net.common.C.checkProperty;
import static self.john.net.Configuration.*;

/**
 * @author zhangyong
 */
public class MQTTBasicSource extends RichSourceFunction<String> {


    private final Properties properties;
    //Runtime fields
    private transient MqttClient client;
    //控制while循环标志
    private transient volatile boolean running;
    //控制停在run方法中的锁对象
    private final static Object waitLock = new Object();

    public MQTTBasicSource(Properties properties) {
        checkProperty(properties, URL);
        checkProperty(properties, CLIENT_ID);
        checkProperty(properties, TOPIC);

        this.properties = properties;
    }


    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(true);
        if (properties.containsKey(USERNAME)) {
            connectOptions.setUserName(properties.getProperty(USERNAME));
        }
        if (properties.containsKey(PASSWORD)) {
            connectOptions.setPassword(properties.getProperty(PASSWORD).toCharArray());
        }
        connectOptions.setAutomaticReconnect(true);
        connectOptions.setConnectionTimeout(5);
        client = new MqttClient(properties.getProperty(URL), properties.getProperty(CLIENT_ID));
        client.connect(connectOptions);
        //订阅主题
        client.subscribe(properties.getProperty(TOPIC), (topic, message) -> {
            String msg = new String(message.getPayload(), StandardCharsets.UTF_8);
            ctx.collect(msg);
        });
        //控制语句
        running = true;
        while (running) {
            synchronized (waitLock) {
                waitLock.wait(100L);
            }

        }
    }

    @Override
    public void cancel() {
        try {
            if (client != null) {
                client.disconnect();
            }
        } catch (MqttException exception) {

        } finally {
            this.running = false;
        }
        // leave main method
        synchronized (waitLock) {
            waitLock.notify();
        }

    }

}
