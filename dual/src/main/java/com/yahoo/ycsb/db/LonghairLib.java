package com.yahoo.ycsb.db;

import com.sun.jna.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Created by ubuntu on 06.01.16.
 */
public class LonghairLib {
    public static int k;
    public static int m;
    public static final int reservedBytes = 8;

    private interface Longhair extends Library {
        Longhair INSTANCE = (Longhair) Native.loadLibrary("longhair", Longhair.class);

        /**
         * Verifies binary compatibility with the API on startup.
         * @return non-zero on success; zero on failure.
         */
        int _cauchy_256_init();

        //int cauchy_256_encode(int k, int m, const unsigned char *data_ptrs[], void *recovery_blocks, int block_bytes);
        /**
         *
         * @param k data blocks
         * @param m recovery blocks
         * @param block_bytes number of bytes per block; multiple of 8
         * @return zero on success, another value indicates failure.
         */
        int cauchy_256_encode(int k, int m, Pointer[] data_ptrs, Pointer recovery_blocks, int block_bytes);

        //int cauchy_256_decode(int k, int m, Block *blocks, int block_bytes);
        /**
         * Recover original data
         * @param k num of original blocks
         * @param m num of recovery blocks
         * @param blocks blocks of data, original or recovery
         * @param blockBytes number of bytes per block; multiple of 8
         * @return 0 on success, otherwise failure
         */
        int cauchy_256_decode(int k, int m, Block[] blocks, int blockBytes);
    }

    public static class Block extends Structure {
        public static class ByReference extends Block implements Structure.ByReference {}

        public Pointer data; // unsigned char *data
        public char row; // unsigned char row

        @Override
        protected List getFieldOrder() {
            return Arrays.asList(new String[] {"data", "row"});
        }
    }

    public static byte[] decode(List<byte[]> blocksBytes) {
        Block.ByReference[] blocks = new Block.ByReference[blocksBytes.size()];
        for (int i = 0; i < blocksBytes.size(); i++) {
            blocks[i] = new Block.ByReference();
        }

        int blockIndex = 0;
        int originalLength = 0;
        int blockSize = 0;
        for (byte[] fullValue : blocksBytes){
            blockSize = fullValue.length - (reservedBytes * 2);

            // divide full value into original length, row number, value
            byte[] lengthBytes = new byte[reservedBytes];
            byte[] rowBytes = new byte[reservedBytes];

            int offset = 0;
            System.arraycopy(fullValue, offset, lengthBytes, 0, reservedBytes);
            offset += reservedBytes;
            System.arraycopy(fullValue, offset, rowBytes, 0, reservedBytes);
            offset += reservedBytes;

            // obtain int
            originalLength = ByteBuffer.wrap(lengthBytes).getInt();
            int row = ByteBuffer.wrap(rowBytes).getInt();

            // add row and value to block
            blocks[blockIndex].row = (char) row;

            Pointer ptr = new Memory(blockSize);
            ptr.write(0, fullValue, offset, blockSize);

            blocks[blockIndex].data = ptr;
            blockIndex++;
        }
        //System.out.println(blocks.length);
        //assert(blocks.length == k);
        assert(originalLength > 0);

        assert(Longhair.INSTANCE.cauchy_256_decode(k, m, blocks, blockSize) == 0);

        Pointer ptrReconstructedData = new Memory(blockSize * k * Native.getNativeSize(Byte.TYPE));
        for (int i = 0; i < k; i++) {
            //System.out.println((int)blocks[i].row);
            ptrReconstructedData.write(i * blockSize, blocks[i].data.getByteArray(0, blockSize), 0, blockSize);
        }
        //System.out.println(Arrays.equals(dataPtr.getByteArray(0,newLen),reconstructed.getByteArray(0,newLen)));
        System.out.println(new String(ptrReconstructedData.getByteArray(0, originalLength), StandardCharsets.UTF_8));
        return ptrReconstructedData.getByteArray(0, originalLength);
    }

    /* helper function for encode */
    private static Map<Integer, byte[]> blocksToBytes(Block.ByReference[] blocks, int blockSize) {
        Map<Integer, byte[]> bBlocks = new HashMap<Integer, byte[]>();
        int blockNum = 0;
        for (Block.ByReference block : blocks) {
            bBlocks.put(blockNum, block.data.getByteArray(0,blockSize));
            blockNum++;
        }
        return bBlocks;
    }

    public static Map<Integer, byte[]> encode(byte[] originalData) {
        // compute length of each block
        int originalLen = originalData.length;

        // reserve 20 bytes for data length and 10 bytes for each block number
        int paddedLen = originalLen;

        while (paddedLen % (8 * k) != 0) {
            paddedLen++;
        }

        // compute block size
        int blockSize = paddedLen / k;

        // construct padded data
        byte[] paddedData = new byte[paddedLen];

        System.arraycopy(originalData, 0, paddedData, 0, originalLen);

        // 1. allocate memory for padded data
        Memory dataPtr = new Memory(paddedLen * Native.getNativeSize(Byte.TYPE));

        // 2. write padded data to that memory
        dataPtr.write(0, paddedData, 0, paddedLen);

        // 3. divide original data into k blocks
        Pointer[] dataPtrs = new Pointer[k];
        //int blockSize = paddedLen / k;
        for (int i = 0; i < k; i++) {
            dataPtrs[i] = dataPtr.share(i * blockSize);
        }
        //System.out.println(Arrays.equals(dataPtr.getByteArray(0,newLen),reconstructed.getByteArray(0,newLen)));

        // reserve memory for the recovery blocks
        Pointer recoveryBlocks = new Memory (blockSize * m * Native.getNativeSize(Byte.TYPE));

        // encode!
        assert(Longhair.INSTANCE.cauchy_256_encode(k,m, dataPtrs, recoveryBlocks, blockSize) == 0);

        // encoded blocks
        Block.ByReference[] blocks = new Block.ByReference[k + m];
        for (int i = 0; i < k + m; i++) {
            blocks[i] = new Block.ByReference();
        }
        //System.out.println("num encoded blocks: " + blocks.length);
        assert(blocks.length == k + m);

        for(int i = 0; i < k; i++) {
            blocks[i].data = dataPtrs[i].share(0);
            blocks[i].row = (char)i;
        }
        for (int i = 0; i < m; i++) {
            blocks[k + i].data = recoveryBlocks.share(i * blockSize);
            blocks[k + i].row = (char)i;
        }

        return blocksToBytes(blocks, blockSize);
    }

}
