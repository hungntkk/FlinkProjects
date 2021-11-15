package KafkaConnectorFlink;

import java.util.Properties;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.*;
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import Helper.JsonToAvro;


public class KafkaConnectorFlink {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "localhost:9092");

        DataStream < String > kafkaData = env.addSource(new FlinkKafkaConsumer < String > ("test3",
                new SimpleStringSchema(),
                p));

        // Tách 1 message thành nhiều message: dữ liệu từ Kafka VD: 1 message Thien Hung sẽ tách thành 2 message Thien va Hung
        DataStreamSink kafkaData2 = kafkaData.flatMap(new FlatMapFunction < String, Tuple2 < String, Integer >> () {
            public void flatMap(String value, Collector < Tuple2 < String, Integer >> out) {
                String[] words = value.split(" ");
                for (String word: words)
                    out.collect(new Tuple2 < String, Integer > (word, 1));
            }
        })

                .keyBy(t -> t.f0)
                .sum(1)
                .print().setParallelism(1);
//                .addSink(StreamingFileSink
//                        .forRowFormat(new Path("/home/jivesh/kafka.txt"),
//                                new SimpleStringEncoder < Tuple2 < String, Integer >> ("UTF-8"))
//                        .withRollingPolicy(DefaultRollingPolicy.builder().build())
//                        .build());
        String schemastr ="{ " +
                "\"type\" : \"record\", " +
                "\"name\" : \"twitter_schema\", " +
                "\"namespace\" : \"com.miguno.avro\", " +
                "\"fields\" : [ { " +
                "\"name\" : \"username\", " +
                "\"type\" : \"string\", " +
                "\"doc\"  : \"Name of the user account on Twitter.com\" " +
                "}, { " +
                "\"name\" : \"tweet\", " +
                "\"type\" : \"string\", " +
                "\"doc\"  : \"The content of the user's Twitter message\" " +
                "}, { " +
                "\"name\" : \"timestamp\", " +
                "\"type\" : \"long\", " +
                "\"doc\"  : \"Unix epoch time in seconds\" " +
                "} ], " +
                "\"doc:\" : \"A basic schema for storing Twitter messages\" }";

//        Schema schema = Schema.parse(schemastr);
//        final StreamingFileSink<GenericRecord> sink = StreamingFileSink
//                .forBulkFormat(new Path("/home/prophet/Flink/data/parquet"), ParquetAvroWriters.forGenericRecord(schema))
//                .build();

//        JsonToAvro.fromJasonToAvro(kafkaData, schemastr).addSink(sink);
        env.execute("Kafka Example");
    }

}