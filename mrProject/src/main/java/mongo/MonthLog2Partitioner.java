package mongo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * Mapper에서 Shuffle and Sort로 데이터를 전달할 때 실행됨
 */
public class MonthLog2Partitioner extends Partitioner<Text, Text> {

    // Mapper 함수에서 받는 월(months)과 리듀서를 매칭하기 위한 객체
    // 1월은 0번 리듀스, 2월은 1번 리듀스 등 이런 형태로 매칭
    Map<String, Integer> months = new HashMap<>();

    // MonthLog2Partitioner() 생성자에 매칭 정보 저장
    public MonthLog2Partitioner() {
        this.months.put("Jan", 0);
        this.months.put("Feb", 1);
        this.months.put("Mar", 2);
        this.months.put("Apr", 3);
        this.months.put("May", 4);
        this.months.put("Jun", 5);
        this.months.put("Jul", 6);
        this.months.put("Aug", 7);
        this.months.put("Sep", 8);
        this.months.put("Oct", 9);
        this.months.put("Nov", 10);
        this.months.put("Dec", 11);
    }

    /**
     * Partitioner 객체에 정의된 함수를 오버라이드
     *
     * @param key Mapper에서 Shuffle and Sort로 전달한 키(key); 월(reqMonth) 값이 들어옴
     * @param value Mapper에서 Shuffle and Sort로 전달한 값(value); JSON 문자열 값이 들어옴
     * @param numReducerTasks 리듀서 객체 번호(0부터 시작; LOG_01 - 1월, LOG_02 - 2월, LOG_03 - 3월 등등)
     */
    @Override
    public int getPartition(Text key, Text value, int numReducerTasks) {

        // 실행될 리듀스 번호를 키(key) 값인 월(Jan, Feb, Mar 등)에 따라 매핑
        return months.get(key.toString());

    }

}
