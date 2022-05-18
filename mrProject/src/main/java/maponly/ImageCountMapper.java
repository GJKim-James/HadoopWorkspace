package maponly;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 맵 역할을 수행하기 위해서는 Mapper 자바 파일을 상속받아야함
 * Mapper 파일의 앞의 2개 데이터 타입(LongWritable, Text)은 분석할 파일의 키(key)와 값(value)의 데이터 타입
 * Mapper 파일의 뒤의 2개 데이터 타입(Text, IntWritable)은 리듀스에 보낼 키(key)와 값(value)의 데이터 타입
 */
public class ImageCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> { // 리듀서를 생성하지 않기 때문에 뒤에 Text, IntWritable 데이터 타입은 뭘 써도 상관없음

    /**
     * 부모 Mapper 자바 파일에 작성된 map 함수를 덮어쓰기(Override) 수행
     * map 함수는 분석할 파일의 레코드 1줄마다 실행됨
     * 파일의 라인 수가 100개라면, map 함수는 100번 실행됨
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 분석할 파일의 한 줄 값 ex) 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
        String[] fields = value.toString().split("\""); // 큰따옴표("")를 기준으로 나눔
        // fields[0] : "96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] ", fields[1] : "GET /cat.jpg HTTP/1.1", fields[2] : " 200 12433"

        if (fields.length > 1) {
            String request = fields[1]; // GET /cat.jpg HTTP/1.1
            fields = request.split(" "); // request에 있는 값을 공백으로 다시 나눔
            // fields[0] : "GET", fields[1] : "/cat.jpg", fields[2] : "HTTP/1.1"

            if (fields.length > 1) {

                String fileName = fields[1].toLowerCase();

                if (fileName.endsWith(".jpg")) {
                    context.getCounter("imageCount", "jpg").increment(1);

                } else if (fileName.endsWith(".gif")) {
                    context.getCounter("imageCount", "gif").increment(1);

                } else {
                    context.getCounter("imageCount", "other").increment(1);

                }

            }

        }

    }

}
