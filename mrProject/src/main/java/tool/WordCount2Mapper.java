package tool;

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
public class WordCount2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * 부모 Mapper 자바 파일에 작성된 map 함수를 덮어쓰기(Override) 수행
     * map 함수는 분석할 파일의 레코드 1줄마다 실행됨
     * 파일의 라인 수가 100개라면, map 함수는 100번 실행됨
     */
    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        // 분석할 파일의 한 줄 값
        String line = value.toString(); // ex) the cat sat on the mat

        // 단어 빈도 수 구현은 공백을 기준으로 나눈 단어로 구분함
        // 분석할 한 줄 내용을 공백으로 나눔
        // word 변수에는 공백을 기준으로 나눠진 단어가 저장됨
        for (String word : line.split("\\W+")) { // \\W+ : '알파벳, 숫자, _ 이외의 문자(특수문자, 공백)'를 의미하는 정규식

            // word 변수에 값이 있다면
            if (word.length() > 0) {

                // Shuffle and Sort로 데이터 전달하기
                // 전달하는 값을 단어와 빈도 수(1)를 전달함
                context.write(new Text(word), new IntWritable(1));

            }

        }

    }

}
