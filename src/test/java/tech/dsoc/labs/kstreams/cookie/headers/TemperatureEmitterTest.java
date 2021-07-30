package tech.dsoc.labs.kstreams.cookie.headers;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tech.dsoc.labs.config.KStreamsConfig;

/**
 * @author sih
 */
class TemperatureEmitterTest {

  private TopologyTestDriver testDriver;
  private TemperatureEmitter temperatureEmitter;
  private TestInputTopic<String,Double> inputReading;
  private TestOutputTopic<String,Double> outputTemperatures;
  private TestRecord<String,Double> lowQualityReading;
  private TestRecord<String,Double> noQualityReading;
  private TestRecord<String,Double> highQualityReading;

  private static final byte[] HIGH_QUALITY_HEADER =
      DataQualityChecker.HIGH_QUALITY.getBytes(StandardCharsets.UTF_8);
  private static final byte[] LOW_QUALITY_HEADER =
      DataQualityChecker.LOW_QUALITY.getBytes(StandardCharsets.UTF_8);

  @BeforeEach
  void setUp() {
    StreamsBuilder builder = TemperatureEmitter.createStreamBuilder();
    testDriver = new TopologyTestDriver(builder.build(KStreamsConfig.properties()));
    inputReading = testDriver.createInputTopic(
        TemperatureEmitter.INPUT_READINGS,
        Serdes.String().serializer(),
        Serdes.Double().serializer());
    outputTemperatures = testDriver.createOutputTopic(
        TemperatureEmitter.VERIFIED_OUTPUT,
        Serdes.String().deserializer(),
        Serdes.Double().deserializer());
    lowQualityReading = new TestRecord<String, Double>("lowFoo", 23.4D);
    lowQualityReading.headers().add(DataQualityChecker.DQ_HEADER, LOW_QUALITY_HEADER);
    highQualityReading = new TestRecord<String, Double>("highFoo", 21.9D);
    highQualityReading.headers().add(DataQualityChecker.DQ_HEADER, HIGH_QUALITY_HEADER);
    noQualityReading = new TestRecord<String, Double>("noFoo", 28.5D);
  }

  @Test
  @DisplayName("Check that the DQ Should ignore low quality readings or those with no DQ")
  void testDataQualityIsEnforced() {
    inputReading.pipeInput(lowQualityReading);
    inputReading.pipeInput(highQualityReading);
    inputReading.pipeInput(noQualityReading);
    List<TestRecord<String,Double>> verifiedReadings = outputTemperatures.readRecordsToList();
    assertThat(verifiedReadings.size())
        .as("Highlander: there can be only one")
        .isEqualTo(1);
    TestRecord<String,Double> reading = verifiedReadings.iterator().next();
    assertThat(reading.key()).isEqualTo("highFoo");
    assertThat(reading.value()).isEqualTo(21.9D);
  }

}