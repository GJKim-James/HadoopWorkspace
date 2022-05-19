package mongo;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public class AccessLog extends Configuration implements Tool {

    // 맵리듀스 실행 함수
    public static void main(String[] args) throws Exception {

        // 파라미터는 분석할 파일(폴더)과 분석 결과가 저장될 파일(폴더) 2개 받음
        if (args.length != 2) {
            log.info("분석할 파일(폴더)과 분석 결과가 저장될 폴더를 입력해야 합니다.");
            System.exit(-1);
        }

        // ToolRunner를 이용한 맵리듀스 실행
        int exitCode = ToolRunner.run(new AccessLog(), args);

        System.exit(exitCode);

    }

    // Configuration 객체에 저장될 값을 정의하며, 저장된 값은 하둡의 Context에 저장되어 맵리듀스 전체에서 사용 가능
    @Override
    public void setConf(Configuration configuration) {

        // App 이름 정의
        configuration.set("AppName", "AccessLog MongoDB Test");

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

        // Configuration 객체에 정의된 AppName 값 가져오기
        Configuration conf = this.getConf();
        String appName = conf.get("AppName");

        log.info("aapName : " + appName);

        // 메모리에 올린 하둡분산파일시스템에 저장된 파일명
        String cacheFile = "/access_log";

        // 맵리듀스 실행을 위한 잡 객체를 가져오기
        // 하둡이 실행되면, 기본적으로 잡 객체를 메모리에 올림
        Job job = Job.getInstance(conf);

        // 호출이 발생하면, 메모리에 저장하여 캐시 처리 수행
        // 하둡분산파일시스템에 저장된 파일만 가능함
        // 하둡분산파일시스템에 저장된 /access_log 파일을 메모리에 올리기
        // 메모리 사용량은 증가하지만, 처리 속도는 향상됨
        job.addCacheFile(new Path(cacheFile).toUri());

        // 맵리듀스 잡이 시작되는 main 함수가 존재하는 파일 설정
        job.setJarByClass(AccessLog.class);

        // 맵리듀스 잡 이름 설정, 리소스 매니저 등 맵리듀스 실행 결과 및 로그 확일할 때 편리함
        job.setJobName(appName);

        // 분석할 폴더(파일) -- 첫 번째 파라미터
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 분석 결과가 저장되는 폴더(파일) -- 두 번째 파라미터
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 맵리듀스의 맵 역할을 수행하는 Mapper 자바 파일 설정
        job.setMapperClass(AccessLogMapper.class);

        // 모든 로그 내용을 MongoDB에 저장하기에 Shuffle and Sort가 필요없음
        // 리듀서 사용 안하도록 설정함
        job.setNumReduceTasks(0);

        // 결과 파일을 생성하지 않기 때문에 setOutputKeyClass, setOutputValueClass 필요 없음

       // 맵리듀스 실행
        boolean success = job.waitForCompletion(true);

        return (success ? 0 : 1);

    }

}