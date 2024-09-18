package com.wjc.log.huaweicloud;

import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.AppendObjectRequest;
import com.obs.services.model.AppendObjectResult;
import com.obs.services.model.ObjectMetadata;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.status.StatusLogger;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


@Plugin(name = "ObsLogAppender", category = "Core", elementType = "appender", printObject = true)
public class ObsLogAppender extends AbstractAppender {
    protected static final org.apache.logging.log4j.Logger LOGGER = StatusLogger.getLogger();

    private final String bucketName;
    private final String logPath;
    private final Long maxFileSize;
    private final int maxBackupIndex;
    private ObsClient obsClient;

    private final String jobPath;

    private Long logFileSize;
    private volatile Long logFilePosition;
    private int nextFileIndex = 1;
    private Lock lock = new ReentrantLock();

    private InetAddress ip = InetAddress.getLocalHost();
    private String hostname = ip.getHostName();
    private String ipAddress = ip.getHostAddress();
    private String logFileName;
    private String preFix;

    private ArrayBlockingQueue<String> historyLogFileName;
    private ArrayList<String> mesBuffer;
    private Long flushTime;

    protected ObsLogAppender(
            String name,
            Layout<? extends Serializable> layout,
            String accessKeyId,
            String secretAccessKey,
            String endpoint,
            String bucketName,
            String logPath,
            String preFix,
            Long maxFileSize,
            int maxBackupIndex)
            throws UnknownHostException {
        super(name, null, layout, true);
        mesBuffer = new ArrayList<>();
        flushTime = System.currentTimeMillis();
        this.bucketName = bucketName;
        this.maxFileSize = maxFileSize;
        this.preFix = preFix;
        this.maxBackupIndex = maxBackupIndex;
        this.obsClient = new ObsClient(accessKeyId, secretAccessKey, endpoint);
        this.jobPath = System.getenv("FLINK_JOB_NAME");
        this.historyLogFileName = new ArrayBlockingQueue<String>(2 * maxBackupIndex);
        this.logPath = logPath + "/" + jobPath + "/" + hostname + "_" + ipAddress;
        logFileName =
                this.logPath
                        + "/"
                        + this.preFix
                        + "_"
                        + currentDateStr()
                        + "_"
                        + (nextFileIndex - 1)
                        + ".log";
        LOGGER.info(
                "创建华为云滚动日志Appender:maxFileSize={},maxBackupIndex={},logPath={},bucketName={}",
                maxFileSize,
                maxBackupIndex,
                logPath,
                bucketName);

        createNewLogFile();
    }

    // 获取当前日期格式化字符串 yyyy-MM-dd
    public String currentDateStr() {
        final Date date = new Date();
        final DateFormat format = DateFormat.getDateInstance();
        return format.format(date);
    }

    @Override
    public boolean stop(long timeout, TimeUnit timeUnit) {
        if (!mesBuffer.isEmpty()) {
            flushMes();
        }
        return super.stop(timeout, timeUnit);
    }

    //  @SneakyThrows
    @Override
    public void append(LogEvent event) {
        lock.lock();
        try {

            if (logFileSize >= maxFileSize) {
                rotateLogs();
            }

            String message = new String(getLayout().toByteArray(event), StandardCharsets.UTF_8);
            mesBuffer.add(message);
            // 缓存了50条数据或者 超过20s后，就向 obs写日志，否则不写
            if (mesBuffer.size() >= 100 || flushTime <= System.currentTimeMillis() - 20 * 1000) {
                flushMes();
            }
        } finally {
            lock.unlock();
        }
    }

    public void flushMes() {
        final byte[] messageBytes = messageJoin(mesBuffer, "\n").getBytes(StandardCharsets.UTF_8);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(messageBytes);
        // 通过获取对象属性接口获取下次追加上传的位置
        // 追加上传
        AppendObjectRequest appendRequest = new AppendObjectRequest();
        appendRequest.setBucketName(bucketName);
        appendRequest.setObjectKey(logFileName);
        appendRequest.setInput(inputStream);
        final boolean isSuccess = append(appendRequest, messageBytes.length, 10);
        if (isSuccess) {
            // 追加写入成功，清理缓存，刷写时间
            mesBuffer.clear();
            flushTime = System.currentTimeMillis();
        } else {
            // 写入失败，日志提醒
            LOGGER.error("缓存日志刷写obs失败，查看上面重试日志");
        }
    }

    public String messageJoin(ArrayList<String> al, String split) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < al.size(); i++) {
            sb.append(al.get(i));
        }
        return sb.toString();
    }

    // 追加写，成功 true，失败 false
    public boolean append(AppendObjectRequest appendRequest, int inputSize, int maxRetries) {
        int attempt = 0;
        while (attempt <= maxRetries) {
            attempt++;
            try {
                appendRequest.setPosition(logFilePosition);
                final AppendObjectResult result = obsClient.appendObject(appendRequest);
                logFilePosition = result.getNextPosition();
                logFileSize = logFileSize + inputSize;
                return true;
            } catch (ObsException e) {
                if (e.getXmlMessage().contains("this object is already append 10000 times")) {
                    // 次数达到限制，滚动文件
                    rotateLogs();
                    appendRequest.setObjectKey(logFileName);
                } else {
                    // 重新获取getNextPosition
                    logFilePosition = obsClient.getObjectMetadata(bucketName, logFileName).getNextPosition();
                    appendRequest.setPosition(logFilePosition);
                }

                try {
                    final AppendObjectResult result = obsClient.appendObject(appendRequest);
                    logFilePosition = result.getNextPosition();
                    logFileSize = logFileSize + inputSize;
                    return true;
                } catch (ObsException ex) {
                    LOGGER.error("第 " + attempt + " 次重试失败：" + e.getMessage());
                    ex.printStackTrace();
                }
            }
        }
        return false;
    }

    private void createNewLogFile() {
        if (!obsClient.doesObjectExist(bucketName, logFileName)) {
            logFileSize = 0L;
            logFilePosition = 0L;
        } else {
            final ObjectMetadata metadata = obsClient.getObjectMetadata(bucketName, logFileName);
            logFileSize = metadata.getContentLength();
            logFilePosition = metadata.getNextPosition();
            LOGGER.info("当前对象：" + logFileName + ", 大小：" + logFileSize);
        }
        // 将新文件名放入队列钟
        historyLogFileName.offer(logFileName);
    }

    private void rotateLogs() {
        closeLogFile();
        if (historyLogFileName.size() >= maxBackupIndex) {
            // 如果满了，将最早的日志文件删除，
            String earliestFileName = historyLogFileName.poll();
            if (obsClient.doesObjectExist(bucketName, earliestFileName)) {
                LOGGER.info("滚动日志删除：" + earliestFileName);
                obsClient.deleteObject(bucketName, earliestFileName);
            }
        }
        // 将日志文件转为 最新 rolling文件
        logFileName = logPath + "/" + preFix + "_" + currentDateStr() + "_" + nextFileIndex + ".log";
        nextFileIndex++;
        createNewLogFile();
    }

    private void closeLogFile() {
        // 没有需要关闭的资源
    }

    @PluginFactory
    public static ObsLogAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginAttribute("accessKeyId") String accessKeyId,
            @PluginAttribute("secretAccessKey") String secretAccessKey,
            @PluginAttribute("endpoint") String endpoint,
            @PluginAttribute("bucketName") String bucketName,
            @PluginAttribute("logPath") String logPath,
            @PluginAttribute("preFix") String preFix,
            @PluginAttribute("maxFileSize") Long maxFileSize,
            @PluginAttribute("maxBackupIndex") int maxBackupIndex,
            @PluginElement("Layout") Layout<? extends Serializable> layout)
            throws UnknownHostException {
        return new ObsLogAppender(
                name,
                layout,
                accessKeyId,
                secretAccessKey,
                endpoint,
                bucketName,
                logPath,
                preFix,
                maxFileSize,
                maxBackupIndex);
    }
}
