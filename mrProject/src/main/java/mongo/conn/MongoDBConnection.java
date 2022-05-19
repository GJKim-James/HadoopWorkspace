package mongo.conn;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoDatabase;
import lombok.Getter;

// MongoDB 연동 후, 연동 정보를 변경할 이유가 없기 때문에 @Setter 선언 안함
// 연동 정보 가지고 가는 것만 필요하기 때문에 @Getter 선언
@Getter
public class MongoDBConnection {

    private MongoDatabase mongoDB;
    private MongoClient mongoClient;

    // 생성자를 통해 객체 생성 시 자동으로 메모리에 올리도록 사용
    public MongoDBConnection() {

        // MongoDB 접속 정보
        String hostName = "192.168.169.129"; // MongoDB 접속할 IP 주소
        int port = 27017; // MongoDB 접속할 포트
        String userName = "myUser"; // MongoDB 아이디
        String password = "1234"; // MongoDB 비밀번호
        String db = "MapReduceDB"; // MongoDB 접속할 데이터베이스

        // MongoDB 접속
        mongoClient = new MongoClient(hostName, port);

        // MongoDB 접속 정보 설정(아이디, DB, 비밀번호) - MongoDB의 MapReduceDB 접속
        MongoCredential.createCredential(userName, db, password.toCharArray());

        // 데이터 저장 및 삭제할 DB 설정(접속한 DB 정보 가져오기)
        mongoDB = mongoClient.getDatabase(db);

    }

}
