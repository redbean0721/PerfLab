package com.redbean0721;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "perflab",
        mixinStandardHelpOptions = true,
        version = "1.1-SNAPSHOT-20260511-1",
        description = "System Performance & Stress Lab",
        subcommands = { DiskTest.class })
public class Main implements Runnable {
    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public void run() {
//        System.out.println("請使用子命令");
        new CommandLine(this).usage(System.out);
    }
}