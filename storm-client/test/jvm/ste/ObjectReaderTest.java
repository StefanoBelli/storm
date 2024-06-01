package ste;

import org.apache.storm.utils.ObjectReader;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
public class ObjectReaderTest {
    @Test
    public void dummy() {
        ObjectReader.getBoolean(Boolean.valueOf("true"), false);
        assertEquals(1,1);
    }
}
