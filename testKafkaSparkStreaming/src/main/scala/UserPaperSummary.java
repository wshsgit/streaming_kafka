import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class UserPaperSummary {

    public static void main(String[] args) throws IOException {
//        Kafka_hbase.insertData("t_answerrecord");
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "master01:9092,slave02:9092,slave03:9092");
        // 制定consumer group
        props.put("group.id", "user_paper_summary");
        // 是否自动确认offset
        props.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        // key的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 定义consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 指定要消费的topic, 可同时处理多个
        consumer.subscribe(Arrays.asList("tzuser_paper_summary"));

        while (true) {
            // 读取数据，读取超时时间为100ms
            ConsumerRecords<String, String> records = consumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                String[] array = record.value().toString().split(",");

                String id = array[0].replace("\"", "");
                System.out.println(id);
                int userid;
                int isright;
                int pid_value;
                int pid_0_value;
                int qid_value;
                int qid_0_value;
                int h_q_r_count_value = 0;
                int h_p_r_count_value = 0;
                System.out.println(array[1].toString()=="  ");
                System.out.println(array[1].toString()==null);
                System.out.println(array[1].toString().length());
                System.out.println("  ".equals(array[1].toString()));
                if ("".equals(array[1].replace("\"", "").toString())) {
                    userid = 000000;
                }else {
                    userid = Integer.parseInt(array[1].replace("\"", ""));
                }
                int testpaperid = Integer.parseInt(array[2].replace("\"", "").toString());
                int questionid = Integer.parseInt(array[3].replace("\"", "").toString());
                ResultScanner results = Kafka_hbase.QueryByCondition1("t_question_point", questionid+"_" );

//                System.out.println(results);
                for (Result result :results) {
                    System.out.println("获得到rowkey:" + new String(result.getRow()));
                    String question_point_rowkey = new String(result.getRow());
                    String[] strings = question_point_rowkey.split("_");
//                    String pointid = strings[1];
                    int pointid = Integer.parseInt(strings[1].toString());
                    String qid = questionid + "";
                    String qid_0 = questionid + "";
                    String pid = pointid + "";
                    String pid_0 = pointid + "_0";
                    // Rowkey: userid + testpaperid ;
                    String rowkey = userid + "_" + testpaperid;
                    System.out.println(rowkey);

                    if ("".equals(array[5].replace("\"", "").toString())) {
                        isright = 0;
                    } else {
                        isright = Integer.parseInt(array[5].replace("\"", "").toString());
                    }
                    int h_pid_0_value = Kafka_hbase.QueryByRowkey("user_paper_summary", rowkey, pid_0);
                    int h_qid_0_value = Kafka_hbase.QueryByRowkey("user_paper_summary", rowkey, qid_0);
                    int h_q_count_value = Kafka_hbase.QueryByRowkey("user_paper_summary", rowkey, "q_count");
                    int h_p_count_value = Kafka_hbase.QueryByRowkey("user_paper_summary", rowkey, "p_count");


                    if (isright == 0) {
                        pid_0_value = h_pid_0_value + 1;
                        qid_0_value = h_qid_0_value + 1;
                    } else {
                        pid_0_value = h_pid_0_value;
                        qid_0_value = h_qid_0_value;
                        h_q_r_count_value = Kafka_hbase.QueryByRowkey("user_paper_summary", rowkey, "q_r_count");
                        h_p_r_count_value = Kafka_hbase.QueryByRowkey("user_paper_summary", rowkey, "p_r_count");
                        h_q_r_count_value = h_q_r_count_value + 1;
                        h_p_r_count_value = h_p_r_count_value + 1;
                    }
                    int h_pid_value = Kafka_hbase.QueryByRowkey("user_paper_summary", rowkey, pid);
                    int h_qid_value = Kafka_hbase.QueryByRowkey("user_paper_summary", rowkey, qid);
                    pid_value = h_pid_value + 1;
                    qid_value = h_qid_value + 1;
                    h_q_count_value = h_q_count_value + 1;
                    h_p_count_value = h_p_count_value + 1;
                    Kafka_hbase.insertDataSum("user_paper_summary", rowkey,
                            "s", "q_count", h_q_count_value + "", "q_r_count", h_q_r_count_value + "",
                            "p_count", h_p_count_value + "", "p_r_count", h_p_r_count_value + "",
                            "q", qid_0, qid_0_value + "", qid, qid_value + "",
                            "p", pid_0, pid_0_value + "", pid, pid_value + "");
                }

                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }

}
