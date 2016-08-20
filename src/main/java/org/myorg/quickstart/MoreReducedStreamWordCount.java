package org.myorg.quickstart;

//import all the classes will be used in this program
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//Usage: MoreReducedStreamWordCount <hostname> <port>

public class MoreReducedStreamWordCount{

    public static void main(String[] args) throws Exception{

        //tips of usage
        if (args.length != 2){
            System.err.println("Usage: MoreReducedStreamWordCount <hostname> <port>");
            return;
        }

        String hostname = args[0];
        Integer port = Integer.parseInt(args[1]);

        //initialize the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //get input data
        DataStream<String> input = env.socketTextStream(hostname, port);

        DataStream<Tuple2<String, Integer>> count =
        //split up the lines and get the initial of each word in the form of (initial, 1)
            input.flatMap(new Extractor())
        //group by the initials and sum up by field 1
            .keyBy(0)
            .sum(1);

        //print out the result
        count.print();
        //execute program
        env.execute("More reduced stream word count!");
    }
}


//implement a function that can split the lines into words and extract the intial of each word.
final class Extractor implements FlatMapFunction<String, Tuple2<String, Integer>>{
    @Override
    public void flatMap(String lines, Collector<Tuple2<String, Integer>> output){
        //split up & get initials
        String[] initials = lines.toLowerCase().split("\\W+");
        for (String initial : initials){
            initial = initial.substring(0,1);
            output.collect(new Tuple2<String, Integer>(initial, 1));
        }
    }
}
