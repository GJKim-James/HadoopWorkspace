package success;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
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
@Log4j
public class ResultCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // 맵리듀스 잡 이름
    // 로그 출력 시, 확인을 위해 appName 변수로 활용
    String appName = "";

    // URL 전송 성공 여부 코드 값(성공 : 200, 실패 : 200 아닌 숫자)
    // URL 요청 성공인 항목만 분석
    String resultCode = "";

    /**
     * Driver 파일(ResultCount)에서 정의한 변수 값을 가져와 map 함수에 적용하기 위해 setup 함수 구현
     */
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        super.setup(context);

        // 사용자 정의 정보 가져오기
        Configuration conf = context.getConfiguration();

        // Driver에서 정의된 맵리듀스 잡 이름 가져오기
        this.appName = conf.get("AppName");

        // Driver에서 정의된 환경 설정 값 가져오기
        // Driver에서 정의된 환경 설정 값이 없다면, 200으로 설정함
        this.resultCode = conf.get("resultCode", "200");

        log.info("[" + this.appName + "] 난 map 함수를 실행하기 전에 1번만 실행되는 setup 함수다!");

    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        super.cleanup(context);

        log.info("[" + this.appName + "] 난 에러가 나도 무조건 실행되는 cleanup 함수다!");

    }

    /**
     * 부모 Mapper 자바 파일에 작성된 map 함수를 덮어쓰기(Override) 수행
     * map 함수는 분석할 파일의 레코드 1줄마다 실행됨
     * 파일의 라인 수가 100개라면, map 함수는 100번 실행됨
     */
    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        // 분석할 파일의 한 줄 값
        String line = value.toString(); // ex) 10.223.157.186 - - [15/Jul/2009:15:50:35 -0700] "GET / HTTP/1.1" 200 9157

        // 분석할 한 줄 내용을 공백을 기준으로 한 단어씩 나눔
        String[] arr = line.split("\\W+"); // [10, 223, 157, 186, ***, 200, 9157]

        // 전송 결과 코드가 존재하는 위치
        // 로그의 뒤에서 2번째에 성공 코드(200) 값이 존재함
        // index는 0부터 시작하기 때문에 -2를 함
        int pos = arr.length - 2;

        // 전송 결과 코드
        String result = arr[pos];

        log.info("[" + this.appName + "] " + result);

        // Driver 파일에서 정의한 코드 값과 로그의 코드 값이 일치한다면
        if (resultCode.equals(result)) {

            // 200 성공 코드 값만 Shuffle and Sort로 보내기
            context.write(new Text(result), new IntWritable(1));

        }

    }

}
