package lv.gstg.speedtest;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.*;

public class ReadFileTest {

    private static final String FILE = "./measurements.txt";

    public static void main(String[] args) throws Exception {
        new ReadFileTest().run();
    }

    private void run() throws Exception {
        readBufferTest();
        // System.exit(1);
        System.out.println("--readFile--");

        // measure("mappedByteBufferParallel 1Th ", () -> mappedByteBufferParallel(1));
        // measure("mappedByteBufferParallel 2Th", () -> mappedByteBufferParallel(2));
        measure("mappedByteBufferParallel  8Th", () -> mappedByteBufferParallel(8));
        // measure("mappedByteBufferParallel 16Th", () -> mappedByteBufferParallel(16));
        // measure("mappedByteBuffer 1Mb", () -> mappedByteBuffer(1024 * 1024));
        measure("mappedByteBuffer 10Mb", () -> mappedByteBuffer(10 * 1024 * 1024));
        // measure("mappedByteBuffer 1Gb", () -> mappedByteBuffer(1024 * 1024 * 1024));
        measure("wrappedByteBuffer 1Mb", () -> wrappedByteBuffer(1024 * 1024));
        // measure("bufferedInputStream 1Mb", () -> bufferedInputStream(1024 * 1024));
        measure("bufferedInputStream 10Mb", () -> bufferedInputStream(10 * 1024 * 1024));
        // measure("bufferedInputStream 100Mb", () -> bufferedInputStream(100 * 1024 * 1024));
        // measure("bufferedInputStream 1Gb", () -> bufferedInputStream(1024 * 1024 * 1024));
        // measure("bufferedReader_readLine", this::bufferedReader_readLine);
        // measure("bufferedReader 1Mb", () -> bufferedReader(1024 * 1024));
        // measure("bufferedReader 1Gb", () -> bufferedReader(1024 * 1024 * 1024));
    }

    private void readBufferTest() throws Exception {
        byte[] bytes = new byte[1024 * 1024 * 1024]; // 1Gb
        for (int i = 0; i < 3; i++) {
            measure("readAllBytesOneByOne", () -> readAllBytesOneByOne(ByteBuffer.wrap(bytes)));
            measure("readAllBytesUsingBuffer 1kb", () -> readAllBytesUsingBuffer(ByteBuffer.wrap(bytes), 1024));
            measure("readAllBytesUsingBuffer 1Mb", () -> readAllBytesUsingBuffer(ByteBuffer.wrap(bytes), 1024 * 1024));
        }
    }

    private long bufferedInputStream(final int bufferSize) {
        try (InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(FILE)), bufferSize)) {
            int b;
            byte[] buf = new byte[bufferSize];
            long bytes = 0;
            while ((b = in.read(buf)) >= 0) {
                bytes += readAllBytes(ByteBuffer.wrap(buf));
                // bytes += b;
            }
            return bytes;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long bufferedReader(final int bufferSize) {
        try (BufferedReader in = Files.newBufferedReader(Paths.get(FILE))) {
            int b;
            char[] buf = new char[bufferSize];
            long bytes = 0;
            while ((b = in.read(buf)) >= 0) {
                bytes += b;
            }
            return bytes;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long bufferedReader_readLine() {
        try (BufferedReader in = Files.newBufferedReader(Paths.get(FILE))) {
            long bytes = 0;
            String line;
            while ((line = in.readLine()) != null) {
                bytes += line.length();
            }
            return bytes;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long mappedByteBuffer(final int bufferSize) {
        // https://www.geeksforgeeks.org/what-is-memory-mapped-file-in-java/
        // try (FileChannel ch = new RandomAccessFile(FILE, "r").getChannel()) {
        try (FileChannel ch = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            long offset = 0;
            byte[] buf = new byte[bufferSize];
            long bytes = 0;
            while (offset < ch.size()) {
                MappedByteBuffer bb = ch.map(FileChannel.MapMode.READ_ONLY, offset, Math.min(bufferSize, ch.size() - offset));
                offset += bufferSize;

                bytes += readAllBytes(bb);

                // bb.get(buf, 0, (int) length);
                // bytes += length;
            }
            return bytes;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long mappedByteBufferParallel(int nThreads) throws IOException, ExecutionException, InterruptedException {
        var fileSize = new File(FILE).length();
        var maxSegLength = fileSize / nThreads + 1;
        long offset = 0;
        var pool = Executors.newFixedThreadPool(nThreads);
        Future<Long>[] futures = new Future[nThreads];
        FileChannel ch = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ);
        for (int i = 0; i < nThreads; i++) {
            int segLength = (int) Math.min(fileSize - offset, maxSegLength);
            long finalOffset = offset;
            futures[i] = pool.submit(() -> {
                return mapAndReadAllBytes(ch, finalOffset, segLength);
            });
            offset += maxSegLength;
        }

        long bytes = 0;
        for (int i = 0; i < nThreads; i++) {
            bytes += futures[i].get();
        }
        pool.shutdown();
        return bytes;
    }

    private long mapAndReadAllBytes(FileChannel ch, long offset, int length) {
        // https://www.geeksforgeeks.org/what-is-memory-mapped-file-in-java/
        // try (FileChannel ch = new RandomAccessFile(FILE, "r").getChannel()) {
        try {
            MappedByteBuffer bb = ch.map(FileChannel.MapMode.READ_ONLY, offset, length);
            return readAllBytes(bb);

            // byte[] buf = new byte[64 * 1024];
            // //System.out.printf("mappedByteBuffer %12d %9d: capacity: %d limit: %d%n", offset, length, bb.capacity(), bb.limit());
            // int bufOffset = 0;
            // while (bufOffset < length) {
            // bb.get(buf, 0, Math.min(buf.length, bb.remaining()));
            // bufOffset += buf.length;
            // }
            // return bb.limit();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long readAllBytes(ByteBuffer bb) {
        return readAllBytesOneByOne(bb);
        // return readAllBytesUsingBuffer(bb, 1024);
    }

    private long readAllBytesOneByOne(ByteBuffer bb) {
        long bytes = 0;
        long lines = 0;
        // read bytes one-by-one
        while (bb.hasRemaining()) {
            var b = bb.get();
            bytes++;
            if (b == '\n')
                lines += consumeByte(b);
        }
        return bytes + lines;
    }

    private long readAllBytesUsingBuffer(ByteBuffer bb, int bufferSize) {
        long bytes = 0;
        long lines = 0;

        // read by copying to buf array
        byte[] buf = new byte[bufferSize];
        while (bb.hasRemaining()) {
            var len = Math.min(buf.length, bb.remaining());
            bb.get(buf, 0, len);
            bytes += len;
            for (int i = 0; i < buf.length; i++)
                lines += consumeByte(buf[i]);
        }
        return bytes + lines;
    }

    private int consumeByte(byte b) {
        final byte newLine = '\n';
        if (b == newLine)
            return 1;
        else
            return 0;
    }

    private long wrappedByteBuffer(final int bufferSize) {
        try (FileChannel ch = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            byte[] buf = new byte[bufferSize];
            ByteBuffer bb = ByteBuffer.wrap(buf);
            int read;
            long bytes = 0;
            while ((read = ch.read(bb)) >= 0) {
                bb.rewind();
                bytes += readAllBytes(bb);
                bb.rewind();
            }
            return bytes;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void measure(String name, Callable<Long> r) throws Exception {
        long started = System.currentTimeMillis();
        long bytes = r.call();
        System.out.printf("%-30s: %6dms bytes:%d%n", name, (System.currentTimeMillis() - started), bytes);
    }
}
