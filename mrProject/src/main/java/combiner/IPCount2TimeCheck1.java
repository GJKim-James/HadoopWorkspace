package combiner;

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
 * 컴바이너(Combiner)가 적용된 맵리듀스 실행 시간 체크
 */
@Log4j
public class IPCount2TimeCheck1 extends Configuration implements Tool {

    // 맵리듀스 실행 함수
    public static void main(String[] args) throws Exception {

        // 파라미터는 분석할 파일(폴더)과 분석 결과가 저장될 파일(폴더)로 2개 받음
        if (args.length != 2) {
            System.out.printf("분석할 폴더(파일) 및 분석 결과가 저장될 폴더를 입력해야 합니다.");
            System.exit(-1);
        }

        // 컴바이너가 적용된 맵리듀스 실행하기 전 시간
        long startTime = System.nanoTime();

        // ToolRunner를 이용한 맵리듀스 실행
        int exitCode = ToolRunner.run(new IPCount2TimeCheck1(), args);

        // 컴바이너가 적용된 맵리듀스 실행 완료 시간
        long endTime = System.nanoTime();

        log.info("Time : " + (endTime - startTime) + "ns"); // 컴바이너 사용했을 경우 실행 시간(속도) 확인

        System.exit(exitCode);

    }

    // Configuration 객체에 저장될 값을 정의하며, 저장된 값은 하둡의 Context에 저장되어 맵리듀스 전체에서 사용 가능
    @Override
    public void setConf(Configuration configuration) {

        // App 이름 정의
        configuration.set("AppName", "Combiner Test");

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

        // 맵리듀스 실행을 위한 잡 객체를 가져오기
        // 하둡이 실행되면, 기본적으로 잡 객체를 메모리에 올림
        Job job = Job.getInstance(conf);

        // 맵리듀스 잡이 시작되는 main 함수가 존재하는 파일 설정
        job.setJarByClass(IPCount2.class);

        // 맵리듀스 잡 이름 설정, 리소스 매니저 등 맵리듀스 실행 결과 및 로그 확일할 때 편리함
        job.setJobName("Combiner IPCount2");

        // 분석할 폴더(파일) -- 첫 번째 파라미터
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 분석 결과가 저장되는 폴더(파일) -- 두 번째 파라미터
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 맵리듀스의 맵 역할을 수행하는 Mapper 자바 파일 설정
        job.setMapperClass(IPCount2Mapper.class);

        // 맵리듀스의 리듀스 역할을 수행하는 Reducer 자바 파일 설정
        job.setReducerClass(IPCount2Reducer.class);

        // 미니 리듀서(mini-Reducer)라 부르며, Combiner 수행 설정
        // 보통 Reducer 역할을 수행하는 객체를 바인딩 함
        // 실제 데이터가 저장된 데이터 노드에서 실행됨
        job.setCombinerClass(IPCount2Reducer.class);

        // 분석 결과가 저장될 때 사용될 키(key)의 데이터 타입
        job.setOutputKeyClass(Text.class);

        // 분석 결과가 저장될 때 사용될 값(value)의 데이터 타입
        job.setOutputValueClass(IntWritable.class);

        // 맵리듀스 실행
        boolean success = job.waitForCompletion(true);

        return (success ? 0 : 1);

    }

}
