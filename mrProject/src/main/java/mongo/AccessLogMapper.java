package mongo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.log4j.Log4j;
import mongo.conn.MongoDBConnection;
import mongo.dto.AccessLogDTO;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.Document;

import java.io.IOException;
import java.util.Map;

/**
 * 맵 역할을 수행하기 위해서는 Mapper 자바 파일을 상속받아야함
 * Mapper 파일의 앞의 2개 데이터 타입(LongWritable, Text)은 분석할 파일의 키(key)와 값(value)의 데이터 타입
 * Mapper 파일의 뒤의 2개 데이터 타입(Text, Text)은 리듀스에 보낼 키(key)와 값(value)의 데이터 타입
 */
@Log4j
public class AccessLogMapper extends Mapper<LongWritable, Text, Text, Text> {

    // MongoDB 접속 및 제어를 위한 객체
    private MongoDatabase mongodb;

    // 로그 파일의 내용을 저장할 MongoDB 컬렉션명
    private String colNm = "ACCESS_LOG";

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        // MongoDB 객체 생성을 통해 MongoDB 접속
        this.mongodb = new MongoDBConnection().getMongoDB();

        // 컬렉션을 생성할지 말지 결정할 변수(true : 생성 / false : 미생성)
        boolean create = true;

        // MongoDB에 생성할 컬렉션이 존재하는지 확인하는 로직
        // Spring-data-mongo는 컬렉션이 존재하는지 여부 확인 함수를 제공
        // 하지만, MongoDriver 라이브러리는 제공하지 않아서 컬렉션 존재 여부 확인 함수를 만들어서 사용해야함
        for (String s : this.mongodb.listCollectionNames()) {

            // 컬렉션이 존재하면 생성하지 못하도록 create 변수를 false로 변경
            if (this.colNm.equals(s)) {
                create = false;
                break;
            }

        }

        if (create) { // MongoDB에 컬렉션이 생성안되어 있으면 생성하기
            // 컬렉션 생성
            this.mongodb.createCollection(this.colNm); // colNm = "ACCESS_LOG"
        }

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

        log.info("ip : " + ip);
        log.info("reqTime : " + reqTime);
        log.info("reqMethod : " + reqMethod);
        log.info("reqURI : " + reqURI);

        // 추출한 정보를 저장하기 위해 pDTO 선언 후, 값 저장
        // IP는 키(key)로 사용하기 때문에 DTO에 저장하지 않음
        AccessLogDTO pDTO = new AccessLogDTO();
        pDTO.setIp(ip); // IP 주소
        pDTO.setReqTime(reqTime); // 요청 일시
        pDTO.setReqMethod(reqMethod); // 요청 방법
        pDTO.setReqURI(reqURI); // 요청 URI

        // 저장할 MongoDB 컬렉션 가져오기
        MongoCollection<Document> col = this.mongodb.getCollection(this.colNm);

        // MongoDB에 저장하기
        col.insertOne(new Document(new ObjectMapper().convertValue(pDTO, Map.class)));

        col = null;

    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        // MongoDB 접속 해제
        this.mongodb = null;

    }

}
