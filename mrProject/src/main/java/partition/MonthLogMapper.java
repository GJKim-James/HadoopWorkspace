package partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * 맵 역할을 수행하기 위해서는 Mapper 자바 파일을 상속받아야함
 * Mapper 파일의 앞의 2개 데이터 타입(LongWritable, Text)은 분석할 파일의 키(key)와 값(value)의 데이터 타입
 * Mapper 파일의 뒤의 2개 데이터 타입(Text, Text)은 리듀스에 보낼 키(key)와 값(value)의 데이터 타입
 */
public class MonthLogMapper extends Mapper<LongWritable, Text, Text, Text> {

    // /access_log 파일로부터 추출될 월 정보가 제대로 수집되었는지 확인하기 위한 변수 생성
    List<String> months = null;

    // months 변수의 데이터 타입이 List이기 때문에 MonthLogMapper() 생성자를 통해 값을 넣어줌
    public MonthLogMapper() {

        // 이 변수는 만들지 않아도 되지만, 추출한 값이 정상적으로 들어왔는지 확인하기 위해서 만듦
        // 추출한 값이 months 변수에 존재하는 값과 일치하는지 확인
        this.months = Arrays.asList("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec");

    }

    /**
     * 부모 Mapper 자바 파일에 작성된 map 함수를 덮어쓰기(Override) 수행
     * map 함수는 분석할 파일의 레코드 1줄마다 실행됨
     * 파일의 라인 수가 100개라면, map 함수는 100번 실행됨
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 분석할 파일의 한 줄 값 ex) 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
        String[] fields = value.toString().split(" "); // 공백을 기준으로 나눔
        // fields[0] : "96.7.4.14", fields[1] : "-", fields[2] : "-", fields[3] : "[24/Apr/2011:04:20:11" 등등

        if (fields.length > 0) {

            String ip = fields[0]; // 96.7.4.14

            String[] dtFields = fields[3].split("/"); // '/'를 기준으로 나눔
            // dtFields[0] : "[24", dtFields[1] : "Apr", dtFields[2] : "2011:04:20:11" 등등

            if (dtFields.length > 1) {

                String theMonth = dtFields[1];

                if (months.contains(theMonth)) {
                    context.write(new Text(ip), new Text(theMonth)); // 전달되는 키(key) : IP, 값(value) : 월 정보(Jan~Dec)
                }

            }

        }

    }

}
