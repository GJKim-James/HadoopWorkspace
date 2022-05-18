package seq;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 맵리듀스를 실행하기 위한 Main 함수가 존재하는 자바 파일
 * 드라이버 파일로 부름
 */
@Log4j
public class CreateCompressSequenceFile extends Configuration implements Tool {

    // 맵리듀스 실행 함수
    public static void main(String[] args) throws Exception {

        // 파라미터는 변환할 원본 파일(폴더)과 시퀀스 파일로 변환될 파일(폴더) 2개 받음
        if (args.length != 2) {
            System.out.printf("변환활 원본 파일(폴더)과 시퀀스 파일로 변환될 파일(폴더)를 입력해야 합니다.");
            System.exit(-1);
        }

        // ToolRunner를 이용한 맵리듀스 실행
        int exitCode = ToolRunner.run(new CreateCompressSequenceFile(), args);

        System.exit(exitCode);

    }

    // Configuration 객체에 저장될 값을 정의하며, 저장된 값은 하둡의 Context에 저장되어 맵리듀스 전체에서 사용 가능
    @Override
    public void setConf(Configuration configuration) {

        // App 이름 정의
        configuration.set("AppName", "Compress Sequence File Create Test");

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
        job.setJarByClass(CreateCompressSequenceFile.class);

        // 맵리듀스 잡 이름 설정, 리소스 매니저 등 맵리듀스 실행 결과 및 로그 확일할 때 편리함
        job.setJobName(appName);

        // 시퀀스 파일 구조로 생성할 폴더(파일) -- 첫 번째 파라미터
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 시퀀스 파일 구조로 생성되는 폴더(파일) -- 두 번째 파라미터
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 시퀀스 파일 구조로 변환하기
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 생성될 파일에 압축을 진행할지 설정(true : 압축 수행)
        FileOutputFormat.setCompressOutput(job, true);

        // 생성될 파일을 압축할 때 사용할 알고리즘 선택(SnappyCodec 사용)
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

        // 압축 방식 설정(BLOCK : BLOCK마다 압축, RECORD : 한 줄마다 압축)
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        // 시퀀스 파일 만들기는 리듀서 작업이 필요하지 않기 때문에 객체의 수를 0으로 정의
        job.setNumReduceTasks(0);

        // 맵리듀스 실행
        boolean success = job.waitForCompletion(true);

        return (success ? 0 : 1);

    }

}
