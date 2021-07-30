package tech.dsoc.labs.config;

import java.util.Properties;

/**
 * Basic KStreams properties
 * @author sih
 */
public final class KStreamsConfig {

  public static Properties properties() {
    Properties props = new Properties();
    props.setProperty("application.id", "kstreams-cookie");
    props.setProperty("bootstrap.servers", "dummy:12345");
    props.setProperty("auto.offset.reset","latest");
    props.setProperty("state.dir", "/tmp/kstreams");
    return props;
  }

}
