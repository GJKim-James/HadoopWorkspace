package success;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 맵리듀스를 실행하기 위한 Main 함수가 존재하는 자바 파일
 * 드라이버 파일로 부름
 */
@Log4j
public class ResultCount2 extends Configuration implements Tool {

    // 맵리듀스 실행 함수
    public static void main(String[] args) throws Exception {

        // 파라미터는 분석 대상, 분석 결과가 저장될 위치, 전송 결과로 3개 받음
        // hadoop jar mr.jar success.ResultCount2 /access_log /result11 404(CLI;Xshell에서 입력하는 내용)
        // /access_log, /result11, 404 => 이 3개의 파라미터
        if (args.length != 3) {
            log.info("분석할 폴더(파일) 및 분석 결과가 저장될 폴더를 입력해야 합니다.");
            System.exit(-1);
        }

        // ToolRunner를 이용한 맵리듀스 실행
        int exitCode = ToolRunner.run(new ResultCount2(), args);

        System.exit(exitCode);

    }

    // Configuration 객체에 저장될 값을 정의하며, 저장된 값은 하둡의 Context에 저장되어 맵리듀스 전체에서 사용 가능
    // 정적인 값만 정의하며, 파라미터로 인해 동적으로 변경되는 항목은 정의하지 않음
    @Override
    public void setConf(Configuration configuration) {

        // App 이름 정의
        configuration.set("AppName", "Send Result2");

    }

    // Configuration 객체에 저장된 값 가져오기
    @Override
    public Configuration getConf() {

        // 맵리듀스 전체에 적용될 변수를 정의할 때 사용
        Configuration conf = new Configuration();

        // 변수 정의
        this.setConf(conf);

        return conf;

    }

    @Override
    public int run(String[] args) throws Exception {

        // 설정 값 가져오기
        Configuration conf = this.getConf();

        // 전송 결과 코드 값을 설정함
        // 200, 403, 404 등 바뀌는 부분을 args[2]로 표시
        conf.set("resultCode", args[2]);

        // 잡 이름 가져오기
        String appName = conf.get("AppName");
        log.info("appName : " + appName);

        // 맵리듀스 실행을 위한 잡 객체를 가져오기
        // 하둡이 실행되면, 기본적으로 잡 객체를 메모리에 올림
        Job job = Job.getInstance(conf);

        // 맵리듀스 잡이 시작되는 main 함수가 존재하는 파일 설정
        job.setJarByClass(ResultCount2.class);

        // 맵리듀스 잡 이름 설정, 리소스 매니저 등 맵리듀스 실행 결과 및 로그 확일할 때 편리함
        job.setJobName(appName);

        // 분석할 폴더(파일) -- 첫 번째 파라미터
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 분석 결과가 저장되는 폴더(파일) -- 두 번째 파라미터
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 맵리듀스의 맵 역할을 수행하는 Mapper 자바 파일 설정
        job.setMapperClass(ResultCountMapper.class);

        // 맵리듀스의 리듀스 역할을 수행하는 Reducer 자바 파일 설정
        job.setReducerClass(ResultCountReducer.class);

        // 분석 결과가 저장될 때 사용될 키(key)의 데이터 타입
        job.setOutputKeyClass(Text.class);

        // 분석 결과가 저장될 때 사용될 값(value)의 데이터 타입
        job.setOutputValueClass(IntWritable.class);

        // 맵리듀스 실행
        boolean success = job.waitForCompletion(true);

        return (success ? 0 : 1);

    }

}
