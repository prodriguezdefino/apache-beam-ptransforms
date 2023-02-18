package com.google.cloud.pso.beam.common;

import com.google.cloud.pso.beam.common.formats.AvroUtils;
import com.google.cloud.pso.beam.common.formats.ThriftUtils;
import com.google.cloud.pso.beam.common.formats.TransportFormats;
import com.google.cloud.pso.beam.common.formats.transforms.TransformTransportToFormat;
import com.google.cloud.pso.beam.common.transport.CommonTransport;
import com.google.cloud.pso.beam.generator.thrift.Message;
import com.google.cloud.pso.beam.generator.thrift.Topic;
import com.google.cloud.pso.beam.generator.thrift.User;
import java.util.HashMap;
import java.util.Random;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 */
public class TransformTransportToFormatTest {

  public TransformTransportToFormatTest() {
  }

  /**
   * Test of retrieveRowSchema method, of class PrepareBQIngestion.
   */
  @Test
  public void testRetrieveRowSchema() {
    String className = "com.google.cloud.pso.beam.generator.thrift.Message";
    Schema result = TransformTransportToFormat.retrieveRowSchema(className);
    assertNotNull(result);
  }

  /**
   * Test of retrieveAvroSchema method, of class PrepareBQIngestion.
   */
  @Test
  public void testRetrieveAvroSchema() throws Exception {
    var className = "com.google.cloud.pso.beam.generator.thrift.Message";
    var result = AvroUtils.retrieveAvroSchemaFromThriftClassName(className);
    assertNotNull(result);
  }

  /**
   * Test of retrieveThriftClass method, of class PrepareBQIngestion.
   */
  @Test
  public void testRetrieveThriftClass() throws Exception {
    var className = "com.google.cloud.pso.beam.generator.thrift.Message";
    var result = ThriftUtils.retrieveThriftClass(className);
    assertNotNull(result);
  }

  @Test
  public void testTransformThriftToRow() throws Exception {
    var className = "com.google.cloud.pso.beam.generator.thrift.Message";
    var avroSchema = AvroUtils.retrieveAvroSchemaFromThriftClassName(className);
    var beamSchema = TransformTransportToFormat.retrieveRowSchema(className);
    var thriftClass = ThriftUtils.retrieveThriftClass(className);
    var random = new Random();

    var message = new Message();
    message.setMessage("some message");

    var user = new User();
    user.setDescription("some description");
    user.setLocation("some location");
    user.setUuid("some id");
    user.setStartup(random.nextLong());

    message.setUser(user);

    var topic = new Topic();
    topic.setId("some other id");
    topic.setName("topic name");
    topic.setValue(random.nextLong());
    
    message.setTopic(topic);

    var thriftData = getBytesFromThriftObject(message);
    var transport = new CommonTransport("someid", new HashMap<>(), thriftData);

    var row = TransformTransportToFormat.retrieveRowFromTransport(
            transport, TransportFormats.Format.THRIFT, thriftClass, beamSchema, avroSchema);

    assertNotNull(row);
  }

  static byte[] getBytesFromThriftObject(TBase<?, ?> instance) {
    try {
      TSerializer serializer = null;
      try {
        serializer = new TSerializer(new TBinaryProtocol.Factory());
      } catch (Exception e) {
        throw new RuntimeException("Error while creating a TSerializer.", e);
      }
      return serializer.serialize(instance);
    } catch (TException ex) {
      throw new RuntimeException("Can't serialize instance.", ex);
    }
  }

}
