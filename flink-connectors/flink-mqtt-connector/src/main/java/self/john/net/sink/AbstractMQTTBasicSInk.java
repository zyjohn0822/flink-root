package self.john.net.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import self.john.net.api.AbstractBasicSink;

import java.util.Properties;

import static self.john.net.Configuration.*;
import static self.john.net.common.C.checkProperty;

public abstract class AbstractMQTTBasicSInk<T> extends AbstractBasicSink<T> {
    private Properties properties;
    private transient MqttClient mqttClient;

    public AbstractMQTTBasicSInk(Properties properties) {
        checkProperty(properties, URL);
        checkProperty(properties, CLIENT_ID);
        checkProperty(properties, TOPIC);

        this.properties = properties;
    }
    public AbstractMQTTBasicSInk() {
        super();
    }

}
