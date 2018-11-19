package everest;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.camel.Exchange;
import org.apache.camel.spi.DataFormat;
import org.apache.camel.spi.DataFormatName;
import org.apache.camel.support.ServiceSupport;

/**
 * A <a href="http://camel.apache.org/data-format.html">data format</a> ({@link DataFormat})
 * for everest data.
 */
public class everestDataFormat extends ServiceSupport implements DataFormat, DataFormatName {

    public String getDataFormatName() {
        return "scheme1";
    }

    public void marshal(Exchange exchange, Object graph, OutputStream stream) throws Exception {
        // marshal from the Java object (graph) to the scheme1 type
        byte[] bytes = exchange.getContext().getTypeConverter().mandatoryConvertTo(byte[].class, graph);
        stream.write(bytes);
    }

    public Object unmarshal(Exchange exchange, InputStream stream) throws Exception {
        // unmarshal from the input stream of scheme1 to Java object (graph)
        byte[] bytes = exchange.getContext().getTypeConverter().mandatoryConvertTo(byte[].class, stream);
        return bytes;
    }

    @Override
    protected void doStart() throws Exception {
        // init logic here
    }

    @Override
    protected void doStop() throws Exception {
        // cleanup logic here
    }

}
