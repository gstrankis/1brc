package lv.gstg.speedtest;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ReadFileTest {

    private static final String FILE = "./measurements.txt";
    private int bufferReadSize = 1;

    public static void main(String[] args) throws Exception {
        System.out.println("availableProcessors=" + Runtime.getRuntime().availableProcessors());

        System.out.println("--readBuffer--");
        //readBufferTest();

        System.out.println("--readChunksTest--");
        //readChunksTest(8);

        //System.exit(1);
        System.out.println("--readFile--");
        //new ReadFileTest(1).run();
        //new ReadFileTest(64).run();
        new ReadFileTest(1024).run();
        //new ReadFileTest(1024 * 1024).run();
    }

    ReadFileTest(int bufferReadSize) {
        this.bufferReadSize = bufferReadSize;
    }

    private void run() throws Exception {

        //measure("mappedByteBufferParallel  1Th", () -> mappedByteBufferParallel(1));
        //measure("mappedByteBufferParallel  2Th", () -> mappedByteBufferParallel(2));
        measure("mappedByteBufferParallel  8Th", () -> mappedByteBufferParallel(8));
        measure("mappedByteBufferParallel 16Th", () -> mappedByteBufferParallel(16));
        // measure("mappedByteBuffer 1Mb", () -> mappedByteBuffer(1024 * 1024));
        measure("mappedByteBuffer    10Mb", () -> mappedByteBuffer(10 * 1024 * 1024));
        // measure("mappedByteBuffer     1Gb", () -> mappedByteBuffer(1024 * 1024 * 1024));
        // measure("wrappedByteBuffer  1Mb", () -> wrappedByteBuffer(1024 * 1024));
        measure("wrappedByteBuffer   10Mb", () -> wrappedByteBuffer(10 * 1024 * 1024));
        // measure("bufferedInputStream 1Mb", () -> bufferedInputStream(1024 * 1024));
        measure("bufferedInputStream 10Mb", () -> bufferedInputStream(10 * 1024 * 1024));
        // measure("bufferedInputStream 100Mb", () -> bufferedInputStream(100 * 1024 * 1024));
        // measure("bufferedInputStream 1Gb", () -> bufferedInputStream(1024 * 1024 * 1024));
        // measure("bufferedReader_readLine", this::bufferedReader_readLine);
        // measure("bufferedReader 1Mb", () -> bufferedReader(1024 * 1024));
        // measure("bufferedReader 1Gb", () -> bufferedReader(1024 * 1024 * 1024));
    }

    private static void readBufferTest() throws Exception {
        byte[] bytes = new byte[1024 * 1024 * 1024]; // 1Gb
        var sizes = List.of(1, 64, 1024, 1024 * 1024);

        for (int i = 0; i < 3; i++) {
            sizes.forEach(size -> {
                var r = new ReadFileTest(size);
                r.measure("readHeapBuffer", () -> r.readAllBytes(ByteBuffer.wrap(bytes)));
            });
        }
    }

    private static void readChunksTest(int numberOfChunks) {
        long started = System.currentTimeMillis();

        var r = new ReadFileTest(1);
        long bytes = 0;
        var chunks = r.getChunks(numberOfChunks);
        for (var chunk : chunks) {
            System.out.println(chunk);
            bytes += chunk.length;
        }

        System.out.printf("%-30s: %6dms bytes:%d%n", "readChunksTest",
                (System.currentTimeMillis() - started), bytes);
    }

    private List<Chunk> getChunks(int numberOfChunks) {
        var result = new ArrayList<Chunk>();
        try (FileChannel ch = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            long chunkSize = Math.min(ch.size() / numberOfChunks + 1, Integer.MAX_VALUE);
            long offset = 0;
//            byte[] buf = new byte[64];
            while (offset < ch.size()) {
                long length = Math.min(offset + chunkSize, ch.size()) - offset;
                if (offset + length < ch.size()) {
                    ch.position(offset + length);
                    var bb = ByteBuffer.allocate(64);
                    ch.read(bb);
                    bb.rewind();
                    while (bb.hasRemaining() && bb.get() != '\n')
                        length++;
                    //System.out.printf("adjust offset %-8d %s%n", offset, new String(buf).replace('\n', '$'));
                }
                result.add(new Chunk(offset, length));
                offset += length;
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ReadResult bufferedInputStream(final int bufferSize) {
        try (InputStream in = new BufferedInputStream(Files.newInputStream(Paths.get(FILE)), bufferSize)) {
            int bytesRead;
            byte[] buf = new byte[bufferSize];
            ReadResult bytes = new ReadResult(0, 0);
            while ((bytesRead = in.read(buf)) >= 0) {
                bytes = bytes.add(readAllBytes(ByteBuffer.wrap(buf, 0, bytesRead)));
            }
            return bytes;
        } catch (IOException e) {
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
        } catch (IOException e) {
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ReadResult mappedByteBuffer(final int bufferSize) {
        // https://www.geeksforgeeks.org/what-is-memory-mapped-file-in-java/
        // try (FileChannel ch = new RandomAccessFile(FILE, "r").getChannel()) {
        try (FileChannel ch = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            long offset = 0;
            byte[] buf = new byte[bufferSize];
            ReadResult result = new ReadResult(0, 0);
            while (offset < ch.size()) {
                MappedByteBuffer bb = ch.map(FileChannel.MapMode.READ_ONLY, offset, Math.min(bufferSize, ch.size() - offset));
                offset += bufferSize;

                result = result.add(readAllBytes(bb));

                // bb.get(buf, 0, (int) length);
                // bytes += length;
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ReadResult mappedByteBufferParallel(int nThreads) throws IOException, ExecutionException, InterruptedException {
        var fileSize = new File(FILE).length();
        var maxSegLength = Math.min(fileSize / nThreads + 1, Integer.MAX_VALUE);
        long offset = 0;
        var pool = Executors.newFixedThreadPool(nThreads);
        List<Future<ReadResult>> futures = new ArrayList<>();
        FileChannel ch = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ);
        while (offset < fileSize) {
            int segLength = (int) Math.min(fileSize - offset, maxSegLength);
            long finalOffset = offset;
            futures.add(pool.submit(() -> {
                return mapAndReadAllBytes(ch, finalOffset, segLength);
            }));
            offset += maxSegLength;
        }

        ReadResult result = new ReadResult(0, 0);
        for (int i = 0; i < futures.size(); i++) {
            result = result.add(futures.get(i).get());
        }
        ch.close();
        pool.shutdown();
        return result;
    }

    private ReadResult mapAndReadAllBytes(FileChannel ch, long offset, int length) {
        // https://www.geeksforgeeks.org/what-is-memory-mapped-file-in-java/
        try {
            MappedByteBuffer bb = ch.map(FileChannel.MapMode.READ_ONLY, offset, length);
            return readAllBytes(bb);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    record Chunk(long offset, long length) {
    }

    private ReadResult readAllBytes(ByteBuffer bb) {
        if (this.bufferReadSize == 1)
            return readAllBytesOneByOne(bb);
        else
            return readAllBytesUsingBuffer(bb, this.bufferReadSize);
    }

    private ReadResult readAllBytesOneByOne(ByteBuffer bb) {
        //while (bb.hasRemaining()) {
//mappedByteBufferParallel  8Th  buf:1b  :   4350ms ReadResult[bytes=13795372587, lines=1000000000]
//mappedByteBuffer    10Mb       buf:1b  :  14064ms ReadResult[bytes=13795372587, lines=1000000000]
//wrappedByteBuffer   10Mb       buf:1b  :  16648ms ReadResult[bytes=13795372587, lines=1000000000]
//bufferedInputStream 10Mb       buf:1b  :  19147ms ReadResult[bytes=13795372587, lines=1000000000]
//for (int i = 0; i < limit; i++) {
//mappedByteBufferParallel  8Th  buf:1b  :   5862ms ReadResult[bytes=13795372587, lines=1000000000]
//mappedByteBuffer    10Mb       buf:1b  :  15270ms ReadResult[bytes=13795372587, lines=1000000000]
//wrappedByteBuffer   10Mb       buf:1b  :  15047ms ReadResult[bytes=13795372587, lines=1000000000]
//bufferedInputStream 10Mb       buf:1b  :  16711ms ReadResult[bytes=13795372587, lines=1000000000]

        long lines = 0;
        // read bytes one-by-one
        while (bb.hasRemaining()) {
            //final var limit = bb.limit();
            //for (int i = 0; i < limit; i++) { //no big difference/worse
            final var b = bb.get();
            lines += consumeByte(b);
//            if (b == '\n')
//                lines++;
        }
        return new ReadResult(bb.limit(), lines);
    }

    private ReadResult readAllBytesUsingBuffer(ByteBuffer bb, int bufferSize) {
        long bytes = 0;
        long lines = 0;

        // read by copying to buf array
        byte[] buf = new byte[bufferSize];
        while (bb.hasRemaining()) {
            var length = Math.min(buf.length, bb.remaining());
            bb.get(buf, 0, length);
            bytes += length;
            for (int i = 0; i < length; i++) {
                //lines += consumeByte(buf[i]);
                if (buf[i] == '\n')
                    lines++;
            }
        }
        return new ReadResult(bytes, lines);
    }

    private int consumeByte(byte b) {
        final byte newLine = '\n';
        if (b == newLine)
            return 1;
        else
            return 0;
    }

    private ReadResult wrappedByteBuffer(final int bufferSize) {
//reusing byteBuffer
//wrappedByteBuffer   10Mb       buf:1kb :  11229ms ReadResult[bytes=13795372587, lines=1000000000]
//wrappedByteBuffer   10Mb       buf:1kb :  10392ms ReadResult[bytes=13795372587, lines=1000000000]
//new ByteBuffer for each read
//wrappedByteBuffer   10Mb       buf:1kb :  10302ms ReadResult[bytes=13795372587, lines=1000000000]
        try (FileChannel ch = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            byte[] buf = new byte[bufferSize];
            ByteBuffer bb; //= ByteBuffer.wrap(buf);
            int read;
            ReadResult result = new ReadResult(0, 0);
            while ((read = ch.read(bb = ByteBuffer.wrap(buf))) >= 0) {
                bb.rewind();
                bb.limit(read);
                result = result.add(readAllBytes(bb));
                //bb.rewind();
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void measure(String name, Callable<ReadResult> r) {
        try {
            long started = System.currentTimeMillis();
            ReadResult result = r.call();
            System.out.printf("%-30s buf:%-4s: %6dms %s%n", name,
                    formatBytes(this.bufferReadSize),
                    (System.currentTimeMillis() - started),
                    result);
            System.gc();
            Thread.sleep(100);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String formatBytes(int bytes) {
        if (bytes >= 1073741824)
            return (bytes / 1073741824) + "Gb";
        if (bytes >= 1048576)
            return (bytes / 1048576) + "Mb";
        if (bytes >= 1024)
            return (bytes / 1024) + "kb";
        return bytes + "b ";
    }

    private static record ReadResult(long bytes, long lines) {
        ReadResult add(ReadResult that) {
            return new ReadResult(this.bytes + that.bytes, this.lines + that.lines);
        }
    }

    private static class Nanos {
        List<String> prints = new ArrayList<>();
        long started = System.currentTimeMillis();

        void snap(String name) {
            long done = System.currentTimeMillis();
            prints.add(String.format("%6dms for %s", (done - started), name));
            started = System.currentTimeMillis();
        }

        void printout() {
            prints.forEach(System.out::println);
        }
    }

}
