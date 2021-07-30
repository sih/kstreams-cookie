package tech.dsoc.labs.kstreams.cookie.headers;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import tech.dsoc.labs.config.KStreamsConfig;

/**
 * This stream job accepts a temperature reading from a source (in the key) and uses a data quality
 * indicator in the header to determine whether to republish or not.
 * @author sih
 */
@Slf4j
public class TemperatureEmitter {

    static final String INPUT_READINGS = "T-TEMPERATURE-READING";
    static final String VERIFIED_OUTPUT = "T-TEMPERATURE-VERIFIED";

  public static void main(String[] args) {
    StreamsBuilder builder = createStreamBuilder();
    KafkaStreams temperatureEmitter = new KafkaStreams(builder.build(), KStreamsConfig.properties());
    log.info("Starting temperature emitter");
    temperatureEmitter.start();
    Runtime.getRuntime().addShutdownHook(new Thread(temperatureEmitter::close));
  }

    static StreamsBuilder createStreamBuilder() {
        StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(INPUT_READINGS, Consumed.with(Serdes.String(), Serdes.Double()))
            .transformValues(DataQualityChecker::new)
            .filter((k,v) -> !v.isNaN())
            .to(VERIFIED_OUTPUT, Produced.with(Serdes.String(), Serdes.Double()));
       return builder;
    }


}
