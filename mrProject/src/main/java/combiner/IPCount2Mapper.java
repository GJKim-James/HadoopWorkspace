package combiner;

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
public class IPCount2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * 부모 Mapper 자바 파일에 작성된 map 함수를 덮어쓰기(Override) 수행
     * map 함수는 분석할 파일의 레코드 1줄마다 실행됨
     * 파일의 라인 수가 100개라면, map 함수는 100번 실행됨
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 분석할 파일의 한 줄 값(로그 한 줄 저장됨)
        String line = value.toString(); // ex) 10.223.157.186 - - [15/Jul/2009:14:58:59 -0700] "GET / HTTP/1.1" 403 202

        String ip = ""; // 로그로부터 추출된 IP
        int forCnt = 0; // 반복 횟수

        // 분석할 한 줄 내용(line)을 '특수문자, 공백'을 기준으로 나눔
        for (String word : line.split("\\W+")) { // \\W+ : '알파벳, 숫자, _ 이외의 문자(특수문자, 공백)'를 의미하는 정규식
            // word에는 10, 223, 157, 186, 15, Jul, 2009, 14, 58, 59, 0700 등 이런 값들이 저장됨

            // word 변수에 값이 있다면
            if (word.length() > 0) {

                forCnt++; // 반복 횟수 증가
                ip += (word + "."); // IP값 저장

                // IP는 196.168.0.127 => 4가지 숫자로 조합되기에 반복문 1번부터 4번까지가 IP 주소임
                if (forCnt == 4) {

                    // ip 변수 값은 '196.168.0.127.'과 같이 마지막에도 .이 붙음
                    // 마지막 .을 제거하기 위해 0부터 마지막 위치에서 -1값까지 문자열을 자름
                    ip = ip.substring(0, ip.length() - 1);

                    // Shuffle and Sort로 데이터 전달하기
                    // 전달하는 값은 ip 변수 값과 빈도 수(1)를 전달
                    context.write(new Text(ip), new IntWritable(1));

                }

            }

        }

    }

}
