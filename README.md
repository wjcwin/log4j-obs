**用于FLink作业将日志落到obs对象存储上**

1.mvn package 打包项目，keytop-logappender-obs.jar 大约6Mb 放入${FLINK_HOME}/lib目录下。
2.修改日志配置文件：log4j 或者 logback，模板如下：

**log4j.properties  example:**

~~~properties
rootLogger.level = INFO
rootLogger.appenderRef.console.ref = ConsoleAppender
rootLogger.appenderRef.rolling.ref = RollingFileAppender
rootLogger.appenderRef.Cloud.ref = ObsLogAppender

appender.Cloud.name = ObsLogAppender
appender.Cloud.type = ObsLogAppender
appender.Cloud.accessKeyId = xxxxxxxxxxxxxx
appender.Cloud.secretAccessKey = xxxxxxxxxxxxxx
appender.Cloud.endpoint = obs.cn-east-3.myhuaweicloud.com
appender.Cloud.bucketName = hive-test1111
appender.Cloud.logPath = flink/logs
appender.Cloud.preFix = flink-log
appender.Cloud.maxFileSize = 20971520
appender.Cloud.maxBackupIndex = 10
appender.Cloud.layout.type = PatternLayout
appender.Cloud.layout.pattern = %d{yyyy-MM-dd HH:mm:ss.SSS} %contextName [%thread] %-5level %logger{36}:%L - %msg%n

# Log all infos to the console
appender.console.name = ConsoleAppender
appender.console.type = CONSOLE
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n

~~~

**logback.xml example :**

~~~xml
<configuration>
    <appender name="console" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{60} %X{sourceThread} - %msg%n</pattern>
        </encoder>
    </appender>

    <appender name="rolling" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <file>${log.file}</file>
        <append>false</append>

        <rollingPolicy class="ch.qos.logback.core.rolling.FixedWindowRollingPolicy">
            <fileNamePattern>${log.file}.%i</fileNamePattern>
            <minIndex>1</minIndex>
            <maxIndex>10</maxIndex>
        </rollingPolicy>

        <triggeringPolicy class="ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy">
            <maxFileSize>100MB</maxFileSize>
        </triggeringPolicy>

        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{60} %X{sourceThread} - %msg%n</pattern>
        </encoder>
    </appender>

    <appender name="ObsLogAppender" class="com.wjc.log.huaweicloud.ObsLogAppender" accessKeyId="xxxxxxxxxxxxxx"
              secretAccessKey="xxxxxxxxxxxxxx" endpoint="obs.cn-east-3.myhuaweicloud.com"
              bucketName="hive-test1111" logPath="flink/logs" preFix="flink-log" maxFileSize="20971520" maxBackupIndex="10">
        <encoder>
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{60} %X{sourceThread} - %msg%n</pattern>
        </encoder>
    </appender>

    <!-- This affects logging for both user code and Flink -->
    <root level="INFO">
        <appender-ref ref="console"/>
        <appender-ref ref="rolling"/>
        <appender-ref ref="Cloud"/>
    </root>

    <root level="WARN">
        <appender-ref ref="console"/>
        <appender-ref ref="rolling"/>
        <appender-ref ref="Cloud"/>
    </root>

    <root level="ERROR">
        <appender-ref ref="console"/>
        <appender-ref ref="rolling"/>
        <appender-ref ref="Cloud"/>
    </root>

    <logger name="akka" level="INFO"/>
    <logger name="org.apache.kafka" level="INFO"/>
    <logger name="org.apache.hadoop" level="INFO"/>
    <logger name="org.apache.zookeeper" level="INFO"/>
    <logger name="com.obs" level="ERROR"/>

    <logger name="org.jboss.netty.channel.DefaultChannelPipeline" level="ERROR"/>
</configuration>

~~~