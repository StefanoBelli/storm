package org.apache.storm.validation;

import org.junit.Test;

import java.util.List;

public class ConfigValidationTest {
    @Test
    public void dummy(){
        List<Class<?>> l = ConfigValidation.getConfigClasses();
        for(final Class<?> e : l) {
            System.out.println("got config class");
        }
    }
}
