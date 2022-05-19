package mongo.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@Setter
@Getter
public class AccessLogDTO {

    // Mapper에서 Reducer로 정보를 전달하기 위해 사용
    private String ip; // IP
    private String reqTime; // 요청 일시
    private String reqMethod; // 요청 방법
    private String reqURI; // 요청 URI

}
