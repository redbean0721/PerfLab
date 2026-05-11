package com.redbean0721;

import picocli.CommandLine;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.locks.LockSupport;

@CommandLine.Command(name = "disk-test",
        mixinStandardHelpOptions = true,
        description = "進行磁碟寫入壓力測試")
public class DiskTest implements Runnable {
    @CommandLine.Option(names = {"-l", "--limit"}, description = "寫入速度限制 (MB/s), 預設 100 MB/s, 輸入 -1 表示不限制", defaultValue = "100")
    private int limitMB;

    @CommandLine.Option(names = {"-p", "--path"}, description = "測試檔案路徑, 預設 ./test.dat", defaultValue = "./test.dat")
    private String path;

    @CommandLine.Option(names = {"-r", "--random"}, description = "是否啟用隨機 4K 寫入測試, 預設 false", defaultValue = "false")
    private boolean randomMode;

    @CommandLine.Option(names = {"--direct"}, description = "強制寫入磁碟 (模擬繞過快取), 預設 false", defaultValue = "false")
    private boolean directMode;

    @Override
    public void run() {
//        long targetBps = limitMB * 1024L * 1024L;
//        int bufferSize = 128 * 1024;   // 128KB buffer
        int bufferSize = randomMode ? 4 * 1024 : 1024 * 1024;    // 隨機模式 4K, 持續模式 1M, 搭配 mmap 效率
        long maxFileSize = 1024 * 1024 * 1024L; // 限制測試檔為 1GB

//        byte[] data = new byte[bufferSize];
        byte[] randomData = new byte[bufferSize];
//        new Random().nextBytes(randomData);
        Random random = new Random();
        random.nextBytes(randomData);
//        ByteBuffer byteBuffer = ByteBuffer.wrap(randomData);

        File file = new File(path);
        System.out.printf("模式: %s | 限速: %s MB/s | 強制寫入: %s%n",
                randomMode ? "隨機 4K" : "順序寫入",
                limitMB == -1 ? "不限速" : limitMB,
                directMode);

        // mmap 須使用 RandomAccessFile 取得 READ_WRITE 權限
        // 使用 FileChannel 提高性能
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            randomAccessFile.setLength(maxFileSize);
            FileChannel fileChannel = randomAccessFile.getChannel();

            // 建立記憶體映射
            MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, maxFileSize);

            long startNano = System.nanoTime();
            long bytesWrittenTotal = 0;
            long lastReportTime = System.currentTimeMillis();
            long bytesSinceLastReport = 0;

            int currentPosition = 0;

            while (!Thread.currentThread().isInterrupted()) {
//                long cycleStart = System.nanoTime();

//                byteBuffer.clear();

                // 隨機 4K 定位邏輯
                if (randomMode) {
                    // 在 1GB 空間內隨機找一個 4K 對齊的起始點
//                    long randomPosition = (long) random.nextInt((int) (maxFileSize / bufferSize)) *  bufferSize;
//                    fileChannel.position(randomPosition);
                    currentPosition = random.nextInt((int) (maxFileSize - bufferSize));
                    currentPosition = (currentPosition / 4096) * 4096;  // 對齊 4K
                } else if (currentPosition + bufferSize > maxFileSize) {
                    // 順序寫入模式下, 如果檔案超過 1GB 則清空
                    currentPosition = 0;
                }

                // 執行寫入
//                fileChannel.write(byteBuffer);

                mappedByteBuffer.position(currentPosition);

                // 防止控制器壓縮
                randomData[0] = (byte) random.nextInt();
                mappedByteBuffer.put(randomData);

                // 如果開啟 directMode, 調用 force() 觸發 msync 強制將快取刷入硬體
                if (directMode) {
//                    fileChannel.force(false);   // false 表示不更新元數據，只刷資料
                    mappedByteBuffer.force();
                }

                if (!randomMode) {
                    currentPosition += bufferSize;
                }

                bytesWrittenTotal += bufferSize;
                bytesSinceLastReport += bufferSize;

                // 控速邏輯: 計算寫入資料理論該花多少時間
                // 理論時間 (ns) = (已寫入總量 / 目標速度) * 10^9
                // 若 limitMB == -1 則跳過
                if (limitMB != -1) {
                    long targetBps = (long) limitMB * 1024 * 1024;
                    long expectedElapsedNano = (bytesWrittenTotal * 1_000_000_000L) / targetBps;
                    long actualElapsedNano = System.nanoTime() - startNano;

                    if (actualElapsedNano < expectedElapsedNano) {
                        // 使用 parkNanos 突破 Thread.sleep 的 15.6ms 限制
                        LockSupport.parkNanos(expectedElapsedNano - actualElapsedNano);
                    }
                }

                // 每秒顯示一次速度
                long now = System.currentTimeMillis();
                if (now - lastReportTime >= 1000) {
                    double speed = bytesSinceLastReport / (1024.0 * 1024.0);
                    System.out.printf("\r當前 I/O 吞吐量: %.2f MB/s", speed);
                    bytesSinceLastReport = 0;
                    lastReportTime = now;
                }
            }
        } catch (Exception e) {
            System.err.println("\n錯誤: " + e.getMessage());
//            throw new RuntimeException(e);
        }
    }
}
