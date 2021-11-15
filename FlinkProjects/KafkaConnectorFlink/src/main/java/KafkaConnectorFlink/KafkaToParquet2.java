package KafkaConnectorFlink;

import Helper.JsonToAvro;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import java.util.Properties;


public class KafkaToParquet2 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // schema dùng để convert avro format
        String schemastr ="{ " +
                "\"type\" : \"record\", " +
                "\"name\" : \"table_operation\", " +
                "\"namespace\" : \"com.operation.table\", " +
                "\"fields\" : [ { " +
                "\"name\" : \"event_time\", " +
                "\"type\" : \"long\", " +
                "\"doc\"  : \"event time sended with msg\" " +
                "}, { " +
                "\"name\" : \"table_name\", " +
                "\"type\" : \"string\", " +
                "\"doc\"  : \"table name\" " +
                "}, { " +
                "\"name\" : \"operation\", " +
                "\"type\" : \"string\", " +
                "\"doc\"  : \"operation\" " +
                "} ], " +
                "\"doc:\" : \"A basic schema for storing tables operations\" }";
        Schema schema = Schema.parse(schemastr);

        // properties của kafka connector
        Properties p = new Properties();
        p.setProperty("bootstrap.servers", "localhost:9092");
        // Get data từ kafka dưới dạng string json
        DataStream< String > kafkaData = env.addSource(new FlinkKafkaConsumer< String >("test3",
                new SimpleStringSchema(),
                p));

        DataStream<GenericRecord> records = kafkaData
                // convert string json to  genericRecord:
                //.flatMap(new FlatMapFunction < String, GenericRecord> () {
                //    public void flatMap(String value, Collector < GenericRecord > out) throws Exception{
                //        JSONObject jsonObj = new JSONObject(value);
                //
                //        // Hàm này e copy ở chỗ khác đưa vô, thử chạy java đơn giản thì convert được.
                //        out.collect(JsonToAvro.fromJasonToAvro(jsonObj.toString(), schemastr));
                //    }
                //})
                // Hàm này cũng convert json string thành GenericRecord nhưng e lấy ở Stackoverflow thấy ng ta dùng để convert trong Flink
                .map(inputStr -> {
                    // convert string json to json object
                    JSONObject jsonObjValue = new JSONObject(inputStr);
                    // convert json object to POJO type. Ở đây là class TableOperation bên dưới
                    TableOperation pojosValue = new TableOperation(jsonObjValue.getLong("event_time"), jsonObjValue.getString("table_name"), jsonObjValue.getString("operation"));
                    GenericData.Record rec = new GenericData.Record(schema);
                    rec.put(0, pojosValue);
                    return rec;
                });
//                .map(new MapFunction<String, GenericRecord>() {
//                    @Override
//                    public GenericRecord map(String value) throws Exception {
//                        // convert string json to json object
//                        JSONObject jsonObjValue = new JSONObject(value);
//                        // convert json object to POJO type. Ở đây là class TableOperation bên dưới
//                        TableOperation pojosValue = new TableOperation(jsonObjValue.getLong("event_time"), jsonObjValue.getString("table_name"), jsonObjValue.getString("operation"));
//
//                        GenericData.Record rec = new GenericData.Record(schema);
//                        rec.put(0, pojosValue);
//                        return rec;
//                    }
//                })
        // .keyBy(t -> t.f0)
        // Window + trigger : chia Window theo row (mỗi Window 5 row)

        records.windowAll(GlobalWindows.create())
                .trigger(CountTrigger.of(5))
                // Sau khi Window bắt buộc reduce, aggregate hoặc apply window function: Hàm này e viết đơn giản là trả về row gần nhất
                .reduce(new Reduce2())
                .print().setParallelism(1);

//


////
//
//        final StreamingFileSink<GenericRecord> sink = StreamingFileSink
//                .forBulkFormat(new Path("/home/prophet/Flink/data/parquet"), ParquetAvroWriters.forGenericRecord(schema))
//                .build();
//
//        JsonToAvro.fromJasonToAvro(kafkaData, schemastr).addSink(sink);
        env.execute("Kafka Example");
    }

    public static class TableOperation {

        public String table_name;
        public String operation;
        public Long event_time;

        public TableOperation() {}

        public TableOperation(Long event_time, String table_name, String operation) {
            this.event_time = event_time;
            this.table_name = table_name;
            this.operation = operation;
        }
    }
    public static class Reduce1 implements ReduceFunction < Tuple3< Long, String, String>> {
        public Tuple3< Long, String, String> reduce(Tuple3 < Long, String, String > current,
                                                    Tuple3< Long, String, String > pre_result) {

            return new Tuple3< Long, String, String> (pre_result.f0, pre_result.f1, pre_result.f2);
        }
    }
    public static class Reduce2 implements ReduceFunction < GenericRecord > {
        public GenericRecord reduce(GenericRecord current, GenericRecord pre_result) {

            return pre_result;
        }
    }

    public static class Splitter implements MapFunction < String, Tuple3< Long, String, String>> {
        public Tuple3 < Long, String, String > map(String value) // 01-06-2018,June,Category5,Bat,12
        {
            JSONObject jsonObj = new JSONObject(value);
            return new Tuple3 < Long, String, String > (jsonObj.getLong("event_time"), jsonObj.getString("table_name"), jsonObj.getString("operation"));
        } //    June    Category5      Bat                      12
    }

}