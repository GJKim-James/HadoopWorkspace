package mongo;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j;
import mongo.dto.AccessLogDTO;
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
@Log4j
public class MonthLog2Mapper extends Mapper<LongWritable, Text, Text, Text> {

    // /access_log 파일로부터 추출될 월 정보가 제대로 수집되었는지 확인하기 위한 변수 생성
    List<String> months = null;

    // months 변수의 데이터 타입이 List이기 때문에 MonthLog2Mapper() 생성자를 통해 값을 넣어줌
    public MonthLog2Mapper() {

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

        String ip = fields[0]; // IP 추출; 96.7.4.14
        String reqTime = ""; // 요청 일시

        if (fields[3].length() > 2) { // 일부 데이터가 요청 일시 값이 누락된 경우가 있어 요청 일시 값이 존재하는 체크
            reqTime = fields[3].substring(1); // '[' 제외한 요청 일시 추출; 24/Apr/2011:04:20:11
        }

        String reqMethod = ""; // GET, POST 요청 방법 추출

        if (fields[5].length() > 2) { // 일부 데이터가 요청 일시 값이 누락된 경우가 있어 요청 일시 값이 존재하는 체크
            reqMethod = fields[5].substring(1); // fields[5] : "GET, '"' 제외한 요청 방법 추출; GET
        }

        String reqURI = fields[6]; // fields[6] : /cat.jpg HTTP/1.1", 요청 URI 추출; /cat.jpg HTTP/1.1

        // 파티셔너를 통해 분할 처리하기 위해 요청 일시로부터 월 값을 추출
        String reqMonth = ""; // 요청 일시에서 월 정보 추출

        String[] dtFields = fields[3].split("/"); // dtFields[0] : [24, dtFields[1] : Apr 등등

        if (dtFields.length > 1) {
            reqMonth = dtFields[1]; // dtFields[1] : Apr
        }

        log.info("ip : " + ip);
        log.info("reqTime : " + reqTime);
        log.info("reqMethod : " + reqMethod);
        log.info("reqURI : " + reqURI);
        log.info("reqMonth : " + reqMonth);

        // 추출한 정보를 저장하기 위해 pDTO 선언 후, 값 저장
        // IP는 키(key)로 사용하기 때문에 DTO에 저장하지 않음
        AccessLogDTO pDTO = new AccessLogDTO();
        pDTO.setIp(ip); // IP 주소
        pDTO.setReqTime(reqTime); // 요청 일시
        pDTO.setReqMethod(reqMethod); // 요청 방법
        pDTO.setReqURI(reqURI); // 요청 URI

        // DTO에 저장된 내용을 JSON 문자열로 변경
        // DTO -> JSON 변경
        String json = new ObjectMapper().writeValueAsString(pDTO);

        // 월 정보가 일치하는지 확인
        if (months.contains(reqMonth)) {
            // MonthLog2Partitioner로 보내서 월별 리듀스 분할하기
            context.write(new Text(reqMonth), new Text(json)); // Key : 월, Value : JSON 문자열
            // 파티셔너에서 Key(월) 값을 가지고 리듀서를 분할 처리하도록 설정할 예정
        }

    }

}
