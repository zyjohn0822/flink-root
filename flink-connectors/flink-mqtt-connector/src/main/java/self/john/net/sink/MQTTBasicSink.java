package self.john.net.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;


import java.util.Properties;

import static self.john.net.Configuration.*;
import static self.john.net.common.C.checkProperty;

/**
 * @author zy
 */
public class MQTTBasicSink extends AbstractMQTTBasicSInk<String> {
    private Properties properties;
    private transient MqttClient mqttClient;

    public MQTTBasicSink(Properties properties) {
        checkProperty(properties, URL);
        checkProperty(properties, CLIENT_ID);
        checkProperty(properties, TOPIC);

        this.properties = properties;
    }
    @Override
    public void open() throws MqttException {
        mqttClient = new MqttClient(
                properties.getProperty(URL),
                properties.getProperty(CLIENT_ID),
                new MemoryPersistence());
        //设置配置选项
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(false);
        connOpts.setAutomaticReconnect(true);
        connOpts.setMaxReconnectDelay(1000);
        connOpts.setUserName(properties.getProperty(USERNAME));
        connOpts.setPassword(properties.getProperty(PASSWORD).toCharArray());
        mqttClient.connect(connOpts);
    }

    @Override
    public void execute(String value) throws MqttException {
        mqttClient.publish(properties.getProperty(TOPIC), value.getBytes(), 0, false);
    }

    @Override
    public void cancel() throws MqttException {
        mqttClient.disconnect();
    }
}
