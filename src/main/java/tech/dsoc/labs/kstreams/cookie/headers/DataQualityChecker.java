package tech.dsoc.labs.kstreams.cookie.headers;


import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * This processor will read the DQ item from th eheader and either transform the value to null ot
 * be filtered out by the emitter or leave it as-is
 * @author sih
 */
@Slf4j
public class DataQualityChecker implements ValueTransformer<Double,Double> {

  private ProcessorContext context;
  static final String DQ_HEADER = "DQValue";
  static final String HIGH_QUALITY = "H";
  static final String LOW_QUALITY = "L";

  @Override
  public void init(ProcessorContext processorContext) {
    // Can only access the headers as part of the actual stream job so all we do in init is save the
    // context to be used in the transform method
    context = processorContext;
  }

  @Override
  public Double transform(Double temperatureReading) {
    Double verifiedReading = Double.NaN;
    Iterator<Header> headerMatches = context.headers().headers(DQ_HEADER).iterator();
    if (headerMatches.hasNext()) {
      Header dqHeader = headerMatches.next();
      String dqValue = new String(dqHeader.value(), StandardCharsets.UTF_8);
      if (HIGH_QUALITY.equalsIgnoreCase(dqValue)) {
        verifiedReading = temperatureReading;
      }
    }
    return verifiedReading;
  }

  @Override
  public void close() {
    // no action needed here
  }
}
