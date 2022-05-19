package partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 리듀스 역할을 수행하기 위해서는 Reducer 자바 파일을 상속받아야 함
 * Reducer 파일의 앞의 2개 데이터 타입(Text, Text)은 Shuffle and Sort에 보낸 데이터의 키(key)와 값(value)의 데이터 타입
 * 보통 Mapper에서 보낸 데이터 타입과 동일함
 * Reducer 파일의 뒤의 2개 데이터 타입(Text, IntWritable)은 결과 파일 생성에 사용될 키(key)와 값(value)
 */
public class TimeLogReducer extends Reducer<Text, Text, Text, IntWritable> {

    /**
     * 부모 Reducer 자바 파일에 작성된 reduce 함수를 덮어쓰기(Override) 수행
     * reduce 함수는 Shuffle and Sort로 처리된 데이터 수만큼 실행됨
     * 처리된 데이터의 수가 500개라면, reduce 함수는 500번 실행됨
     *
     * Reducer 객체는 기본값이 1개로 1개의 쓰레드로 처리함
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // IP별 빈도 수를 계산하기 위한 변수
        int ipCount = 0;

        // Shuffle and Sort로 인해 IP별로 데이터들의 값들이 List 구조로 저장됨
        // 파티셔너를 통해 같은 시간대에 해당되는 IP만 넘어옴
        // 192.168.0.127 : {'01', '01', '01', '01', '01', '01'} => 192.168.0.127, 6(Text, Text의 데이터 타입)
        // 위의 예시는 192.168.0.127 IP가 호출된 수는 6번이 된다.
        for (Text value : values) {

            ipCount++;

        }

        // 분석 결과 파일에 데이터 저장하기
        context.write(key, new IntWritable(ipCount));

    }

}
