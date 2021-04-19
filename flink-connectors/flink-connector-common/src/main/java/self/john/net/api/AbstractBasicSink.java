package self.john.net.api;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author zhang yong
 * @version 1.0
 * @description 封装通用的sink适配类
 * @date 2021/4/19 14:18
 */
public abstract class AbstractBasicSink<T> extends RichSinkFunction<T> {
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        open();
    }

    @Override
    public void close() throws Exception {
        super.close();
        cancel();
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        execute(value);
    }

    /**
     * @description
     *
     * @author zhang yong
     * @date 2021/4/19 13:57
     * @return void
     */
    public abstract void open() throws Exception;

    /**
     * @description
     *
     * @param value
     * @author zhang yong
     * @date 2021/4/19 13:56
     * @return void
     */
    public abstract void execute(T value) throws Exception;
    /**
     * @description
     *
     * @author Hisense
     * @date 2021/4/19 14:20
     * @return void
     */
    public abstract void cancel() throws Exception;

}
