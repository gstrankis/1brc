package lv.gstg.speedtest;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class ReadFileTest {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) {
        new ReadFileTest().run();
    }

    private void run() {
        measure("wrappedByteBuffer 1Mb", () -> wrappedByteBuffer(1024 * 1024));
        measure("mappedByteBuffer 1Mb", () -> mappedByteBuffer(1024 * 1024));
        // measure("mappedByteBuffer 10Mb", () -> mappedByteBuffer_on_randomAccessFile(10 * 1024 * 1024));
        measure("bufferedInputStream   1Mb", () -> bufferedInputStream(1024 * 1024));
        measure("bufferedInputStream  10Mb", () -> bufferedInputStream(10 * 1024 * 1024));
        // measure("bufferedInputStream 100Mb", () -> bufferedInputStream(100 * 1024 * 1024));
        // measure("bufferedInputStream 1Gb", () -> bufferedInputStream(1024 * 1024 * 1024));
        // measure("bufferedReader_readLine", this::bufferedReader_readLine);
        // measure("bufferedReader 1Mb", () -> bufferedReader(1024 * 1024));
        // measure("bufferedReader 1Gb", () -> bufferedReader(1024 * 1024 * 1024));
    }

    private void bufferedInputStream(final int bufferSize) {
        try (InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(FILE)), bufferSize)) {
            int b;
            byte[] buf = new byte[bufferSize];
            long bytes = 0;
            while ((b = in.read(buf)) >= 0) {
                bytes += b;
            }
            // System.out.println("BufferedInputStream(Files.newInputStream " + bytes + " bytes");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void bufferedReader(final int bufferSize) {
        try (BufferedReader in = Files.newBufferedReader(Paths.get(FILE))) {
            int b;
            char[] buf = new char[bufferSize];
            long bytes = 0;
            while ((b = in.read(buf)) >= 0) {
                bytes += b;
            }
            // System.out.println("bufferedReader " + bytes + " bytes");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void bufferedReader_readLine() {
        try (BufferedReader in = Files.newBufferedReader(Paths.get(FILE))) {
            long bytes = 0;
            String line;
            while ((line = in.readLine()) != null) {
                bytes += line.length();
            }
            // System.out.println("bufferedReader_readLine " + bytes + " bytes");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void mappedByteBuffer(final int bufferSize) {
        // https://www.geeksforgeeks.org/what-is-memory-mapped-file-in-java/
        // try (FileChannel ch = new RandomAccessFile(FILE, "r").getChannel()) {
        try (FileChannel ch = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            long offset = 0;
            byte[] buf = new byte[bufferSize];
            long bytes = 0;
            while (offset < ch.size()) {
                long length = ch.size() - offset;
                if (length > bufferSize)
                    length = bufferSize;
                MappedByteBuffer out = ch.map(FileChannel.MapMode.READ_ONLY, offset, length);
                offset += bufferSize;

                out.get(buf, 0, (int) length);
                bytes += length;
            }
            System.out.println("mappedByteBuffer " + bytes + " bytes of file size " + ch.size());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void wrappedByteBuffer(final int bufferSize) {
        try (FileChannel ch = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            byte[] buf = new byte[bufferSize];
            ByteBuffer bb = ByteBuffer.wrap(buf);
            int read;
            long bytes = 0;
            while ((read = ch.read(bb)) >= 0) {
                bytes += read;
                bb.flip();
                System.out.println("wrappedByteBuffer " + bytes + " bytes of " + ch.size());
            }
            System.out.println("wrappedByteBuffer " + bytes + " bytes of file size " + ch.size());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void measure(String name, Runnable r) {
        long started = System.currentTimeMillis();
        r.run();
        System.out.printf("%-30s: %-8dms%n", name, (System.currentTimeMillis() - started));
    }
}
