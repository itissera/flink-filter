package com.aureum.stream.flinkfilter.consumer;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.sql.Time;

@Component
@EnableScheduling
public class FlinkStreamConsumer {
    private static final Logger logger = LoggerFactory.getLogger(FlinkStreamConsumer.class);
    private DataStream<String> dataStream ;
    private StreamExecutionEnvironment see;
    private StreamTableEnvironment streamTableEnvironment;



    public static final MapFunction<String,Tuple2<Integer,Time>> mapFunction = new MapFunction<String, Tuple2<Integer, Time>>() {
        @Override
        public Tuple2<Integer, Time> map(String value) throws Exception {
            String p = value.trim();
            logger.info(">>> Map value : "+value );
            Time creationTime = new Time(System.currentTimeMillis());
            return new Tuple2<>(Integer.getInteger(p),creationTime);
        }
    };


    public static final AscendingTimestampExtractor extractor = new AscendingTimestampExtractor<Tuple2<Integer,Time>>() {

        @Override
        public long extractAscendingTimestamp(Tuple2<Integer, Time> element) {
            return element.f1.getTime();
        }
    };

    @PostConstruct
    public void init(){
        see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);
        streamTableEnvironment =TableEnvironment.getTableEnvironment(see);
        see.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    }


    @Scheduled (fixedDelay = 1000)
    public void consumeStream() throws Exception {
        DataStream<String> dataStream = see.socketTextStream("localhost",6777);

        DataStream<Tuple2<Integer,Time>> dataSet = dataStream.map(mapFunction).assignTimestampsAndWatermarks(extractor);

        streamTableEnvironment.registerDataStream("random_numbers_2",dataSet);

        String sql = "SELECT * FROM  random_numbers";
        Table table = streamTableEnvironment.sqlQuery(sql);
        streamTableEnvironment.toAppendStream(table,Row.class).print();
        //dataSet.print();
        see.execute("Read from socket random numbers");
    }




}
