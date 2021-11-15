package TableAPI;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.expressions.TimeIntervalUnit;
import org.apache.flink.table.api.*;


import static org.apache.flink.table.api.Expressions.*;

public class Table_API {

    public static Table report(Table transactions) {
        throw new UnimplementedException();
    }

    public static void main(String[] args) throws Exception {
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("CREATE TABLE operation_msg (\n" +
                "    event_time  BIGINT,\n" +
                "    table_name      VARCHAR,\n" +
                "    operation VARCHAR,\n" +
                "    proctime AS PROCTIME() \n" +
//                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'test3',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = 'testGroup1',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format'    = 'json'\n" +
                ")");

        tEnv.executeSql("CREATE TABLE par_sink  (\n" +
                "    event_time  BIGINT,\n" +
                "    window_start timestamp(3) NOT NULL,\n" +
                "    window_end timestamp(3) NOT NULL,\n" +
                "    operation VARCHAR\n" +
//                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'filesystem',\n" +
                "    'path'     = 'file:///home/prophet/Flink/data/parquet2',\n" +
                "    'format'    = 'parquet' \n" +
                ")");

//        Table transactions = tEnv.from("transactions").select("*");
//        String sql="SELECT * FROM transactions";
        String sql=
            "    INSERT INTO par_sink(event_time, window_start, window_end, operation) \n" +
            "    SELECT event_time, window_start, window_end, operation \n" +
            "    FROM TABLE ( \n" +
            "        TUMBLE(TABLE operation_msg, DESCRIPTOR(proctime), INTERVAL '10' SECONDS)) \n" +
            "    GROUP BY event_time, window_start, window_end, operation \n";

        tEnv.executeSql(sql).print();

//        report(transactions).executeInsert("spend_report");
    }
}