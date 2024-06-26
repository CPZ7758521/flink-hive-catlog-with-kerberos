package com.pandora.www.config;

import java.io.IOException;
import java.util.Properties;

public class Config {
    public static String env;

    static {
        env = System.getProperty("env");

        Properties properties = new Properties();
        try {
            switch (env) {
                case "test":
                    properties.load(Config.class.getClassLoader().getResourceAsStream(env + "/config.properties"));
                case "pord":
                    properties.load(Config.class.getClassLoader().getResourceAsStream(env + "config.properties"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
