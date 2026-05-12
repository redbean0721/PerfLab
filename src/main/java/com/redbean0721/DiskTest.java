package com.redbean0721;

import picocli.CommandLine;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

@CommandLine.Command(name = "disk-test",
        mixinStandardHelpOptions = true,
        description = "進行磁碟寫入壓力測試")
public class DiskTest implements Runnable {
    @CommandLine.Option(names = {"-l", "--limit"}, description = "寫入速度限制 (MB/s), 預設 100 MB/s, 輸入 -1 表示不限制", defaultValue = "100")
    private int limitMB;

//    @CommandLine.Option(names = {"-p", "--path"}, description = "測試檔案路徑, 預設 ./test.dat", defaultValue = "./test.dat")
//    private String path;

    @CommandLine.Option(names = {"-r", "--random"}, description = "是否啟用隨機 4K 寫入測試, 預設 false", defaultValue = "false")
    private boolean randomMode;

    @CommandLine.Option(names = {"-t", "--threads"}, description = "執行緒數量 (併發寫入檔案數), 預設 4", defaultValue = "4")
    private int threadCount;

    @CommandLine.Option(names = {"--direct"}, description = "強制寫入磁碟 (模擬繞過快取), 預設 false", defaultValue = "false")
    private boolean directMode;

    private final LongAdder bytesWrittenTotal = new LongAdder();    // 統計全域速度
    private long totalBytesEverWritten = 0; // 統計總寫入量
    private long startTime;

    @Override
    public void run() {
        this.startTime = System.currentTimeMillis();

        // 註冊 Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            long endTime = System.currentTimeMillis();
            double durationSec = (endTime - startTime) / 1000.0;
            double totalGB = totalBytesEverWritten / (1024.0 * 1024.0 * 1024.0);
            double avgSpeed = durationSec > 0 ? (totalGB * 1024.0 / durationSec) : 0;

            System.out.println("\n\n" + "=".repeat(40));
            System.out.println("          PerfLab 測試報告");
            System.out.println("=".repeat(40));
            System.out.printf(" 測試時長     : %.2f 秒%n", durationSec);
            System.out.printf(" 累計寫入量   : %.4f GB%n", totalGB);
            System.out.printf(" 平均寫入速度 : %.2f MB/s%n", avgSpeed);
            System.out.println("=".repeat(40));
            System.out.println("測試結束, 檔案刪除中");
        }));

        System.out.println("正在清理舊的檔案...");
        File dir = new File(".");
        File[] files = dir.listFiles((d, name) -> name.startsWith("test_part_") && name.endsWith(".dat"));
        if (files != null) {
            for (File file : files) {
                if (file.delete()) {
                    System.out.println("已刪除: " + file.getName());
                } else {
                    System.err.println("無法刪除檔案(可能被其他程式佔用): " + file.getName());
                }
            }
        }

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        long perFileSize = 1800 * 1024 * 1024L; // 每個執行緒寫入 1.8GB

        System.out.printf("執行緒數: %d | 模式: %s | 限速: %s MB/s | 強制寫入: %s%n",
                threadCount,
                randomMode ? "隨機 4K" : "順序寫入",
                limitMB == -1 ? "不限速" : limitMB,
                directMode);

        // 啟動監控執行緒
        startReporter();

        for (int i = 0; i < threadCount; i++) {
            String subPath = "test_part_" + i + ".dat";
            executorService.submit(new WriteTask(subPath, perFileSize));
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void startReporter() {
        Thread reporter = new Thread(() -> {
            long lastTime = System.currentTimeMillis();
            while (!Thread.currentThread().isInterrupted()) {
                LockSupport.parkNanos(1_000_000_000L);  // 每秒報告一次

                // 每秒顯示一次速度
                long currentTime = System.currentTimeMillis();
                long bytes = bytesWrittenTotal.sumThenReset();

                // 更新累積總量
                totalBytesEverWritten += bytes;

                double speed = bytes / (1024.0 * 1024.0 * ((currentTime - lastTime) / 1000.0));
                double totalGB = totalBytesEverWritten / (1024.0 * 1024.0 * 1024.0);
                System.out.printf("\r當前總 I/O 吞吐量: %.2f MB/s | 累計寫入量: %.2f GB", speed, totalGB);
                lastTime = currentTime;
            }
        });
        reporter.setDaemon(true);
        reporter.start();
    }

    class WriteTask implements Runnable {
        private final String taskPath;
        private final long fileSize;

        public WriteTask(String taskPath, long fileSize) {
            this.taskPath = taskPath;
            this.fileSize = fileSize;
        }

        @Override
        public void run() {
//        long targetBps = limitMB * 1024L * 1024L;
//        int bufferSize = 128 * 1024;   // 128KB buffer
            int bufferSize = randomMode ? 4 * 1024 : 1024 * 1024;    // 隨機模式 4K, 持續模式 1M, 搭配 mmap 效率
//            long maxFileSize = 1024 * 1024 * 1024L; // 限制測試檔為 1GB

//        byte[] data = new byte[bufferSize];
            byte[] randomData = new byte[bufferSize];
//        new Random().nextBytes(randomData);
            Random random = new Random();
            random.nextBytes(randomData);
//        ByteBuffer byteBuffer = ByteBuffer.wrap(randomData);

            File file = new File(taskPath);
            file.deleteOnExit();    // 程式結束後刪除檔案

            // mmap 須使用 RandomAccessFile 取得 READ_WRITE 權限
            // 使用 FileChannel 提高性能
            try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
                randomAccessFile.setLength(fileSize);
                FileChannel fileChannel = randomAccessFile.getChannel();

                // 建立記憶體映射
                MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);

                long threadStartNano = System.nanoTime();
                long threadBytesWritten = 0;
//                long lastReportTime = System.currentTimeMillis();
//                long bytesSinceLastReport = 0;

                int currentPosition = 0;

                while (!Thread.currentThread().isInterrupted()) {
//                long cycleStart = System.nanoTime();

//                byteBuffer.clear();

                    // 隨機 4K 定位邏輯
                    if (randomMode) {
                        // 在 1GB 空間內隨機找一個 4K 對齊的起始點
//                    long randomPosition = (long) random.nextInt((int) (maxFileSize / bufferSize)) *  bufferSize;
//                    fileChannel.position(randomPosition);
                        currentPosition = random.nextInt((int) (fileSize - bufferSize));
                        currentPosition = (currentPosition / 4096) * 4096;  // 對齊 4K
                    } else if (currentPosition + bufferSize > fileSize) {
                        // 順序寫入模式下, 如果檔案超過 1.8GB 則清空
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

                    bytesWrittenTotal.add(bufferSize);
                    threadBytesWritten += bufferSize;

                    // 控速邏輯: 計算寫入資料理論該花多少時間
                    // 理論時間 (ns) = (已寫入總量 / 目標速度) * 10^9
                    // 若 limitMB == -1 則跳過
                    if (limitMB != -1) {
                        long targetBpsPerThread = ((long) limitMB * 1024 * 1024) / threadCount;
                        long expectedElapsedNano = (threadBytesWritten * 1_000_000_000L) / targetBpsPerThread;
                        long actualElapsedNano = System.nanoTime() - threadStartNano;

                        if (actualElapsedNano < expectedElapsedNano) {
                            // 使用 parkNanos 突破 Thread.sleep 的 15.6ms 限制
                            LockSupport.parkNanos(expectedElapsedNano - actualElapsedNano);
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("\n執行緒 [" + taskPath + "] 錯誤: " + e.getMessage());
//            throw new RuntimeException(e);
            }
        }
    }
}