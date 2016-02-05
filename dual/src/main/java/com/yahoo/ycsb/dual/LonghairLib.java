package com.yahoo.ycsb.dual;

import com.sun.jna.*;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by ubuntu on 06.01.16.
 */
public class LonghairLib {
    public static final int reservedBytes = 8;
    public static int k;
    public static int m;
    public static boolean initialized = false;
    private static Logger logger = Logger.getLogger(Class.class);

    public static byte[] decode(List<byte[]> blocksBytes) {
        //Block.ByReference[] blocks = new Block.ByReference[blocksBytes.size()];
        Block[] blocks = (Block[]) new Block().toArray(blocksBytes.size());
        /*for (int i = 0; i < blocksBytes.size(); i++) {
            blocks[i] = new Block();  // .ByReference();
        }*/

        int blockIndex = 0;
        int originalLength = -1;
        int blockSize = 0;
        byte[] lengthBytes;
        for (byte[] fullValue : blocksBytes) {
            blockSize = fullValue.length - (reservedBytes * 2);
            int offset = 0;

            // divide full value into original length, row number, value
            if (originalLength < 0) {
                lengthBytes = new byte[reservedBytes];
                System.arraycopy(fullValue, offset, lengthBytes, 0, reservedBytes);
                originalLength = ByteBuffer.wrap(lengthBytes).getInt();
            }
            offset += reservedBytes;

            byte[] rowBytes = new byte[reservedBytes];
            System.arraycopy(fullValue, offset, rowBytes, 0, reservedBytes);
            offset += reservedBytes;
            int row = ByteBuffer.wrap(rowBytes).getInt();

            // add row and value to block
            blocks[blockIndex].row = (char) row;

            Pointer ptr = new Memory(blockSize);
            ptr.write(0, fullValue, offset, blockSize);

            blocks[blockIndex].data = ptr;
            blockIndex++;
        }
        assert (originalLength > 0);

        /*System.out.println("Before calling decode");
        for (int i = 0; i < blocks.length; i++) {
            System.out.println((int)blocks[i].row);
            System.out.println(new String(blocks[i].data.getByteArray(0, blockSize), StandardCharsets.UTF_8));
        }*/

        Longhair.INSTANCE.cauchy_256_decode(k, m, blocks, blockSize);

        Pointer ptrReconstructedData = new Memory(blockSize * k * Native.getNativeSize(Byte.TYPE));
        for (int i = 0; i < blocks.length; i++) {
            //System.out.println((int)blocks[i].row);
            //System.out.println(new String(blocks[i].data.getByteArray(0, blockSize), StandardCharsets.UTF_8));
            if (blocks[i].row < k) {
                ptrReconstructedData.write(blocks[i].row * blockSize, blocks[i].data.getByteArray(0, blockSize), 0, blockSize);
            }
        }
        //System.out.println(Arrays.equals(dataPtr.getByteArray(0,newLen),reconstructed.getByteArray(0,newLen)));
        //System.out.println(new String(ptrReconstructedData.getByteArray(0, originalLength), StandardCharsets.UTF_8));
        return ptrReconstructedData.getByteArray(0, originalLength);
    }

    /* helper function for encode */
    private static List<byte[]> appendLengthAndRow(Block.ByReference[] blocks, int blockSize, int originalLen) {
        List<byte[]> blocksAsBytes = new ArrayList<byte[]>();
        for (Block.ByReference block : blocks) {
            byte[] newBlock = new byte[(LonghairLib.reservedBytes * 2) + blockSize];
            int offset = 0;

            // preceed block by original data length ...
            byte[] lengthBytes = ByteBuffer.allocate(reservedBytes).putInt(originalLen).array();
            System.arraycopy(lengthBytes, 0, newBlock, offset, LonghairLib.reservedBytes);
            offset += LonghairLib.reservedBytes;
            // ... and row number
            byte[] rowBytes = ByteBuffer.allocate(reservedBytes).putInt(block.row).array();
            System.arraycopy(rowBytes, 0, newBlock, offset, LonghairLib.reservedBytes);
            offset += LonghairLib.reservedBytes;
            // now copy value to new block
            System.arraycopy(block.data.getByteArray(0, blockSize), 0, newBlock, offset, blockSize);

            blocksAsBytes.add(newBlock);
        }
        return blocksAsBytes;
    }

    public static List<byte[]> encode(byte[] originalData) {
        // pad data
        int paddedLen = originalData.length;
        while (paddedLen % (8 * k) != 0) {
            paddedLen++;
        }

        // compute block size
        int blockSize = paddedLen / k;

        // allocate memory for padded data
        Memory dataPtr = new Memory(paddedLen * Native.getNativeSize(Byte.TYPE));

        // write padded data to that memory
        dataPtr.write(0, originalData, 0, originalData.length);

        // divide original data into k blocks
        Pointer[] dataPtrs = new Pointer[k];
        for (int i = 0; i < k; i++) {
            dataPtrs[i] = dataPtr.share(i * blockSize);
        }
        //System.out.println(Arrays.equals(dataPtr.getByteArray(0,newLen),reconstructed.getByteArray(0,newLen)));

        // reserve memory for the recovery blocks
        Pointer recoveryBlocks = new Memory(blockSize * m * Native.getNativeSize(Byte.TYPE));

        // encode!
        Longhair.INSTANCE.cauchy_256_encode(k, m, dataPtrs, recoveryBlocks, blockSize);

        // encoded blocks
        Block.ByReference[] blocks = new Block.ByReference[k + m];
        for (int i = 0; i < k + m; i++) {
            blocks[i] = new Block.ByReference();
        }
        //System.out.println("num encoded blocks: " + blocks.length);
        assert (blocks.length == k + m);

        for (int i = 0; i < k; i++) {
            blocks[i].data = dataPtrs[i].share(0);
            blocks[i].row = (char) i;
        }
        for (int i = 0; i < m; i++) {
            blocks[k + i].data = recoveryBlocks.share(i * blockSize);
            blocks[k + i].row = (char) (k + i);
        }

        return appendLengthAndRow(blocks, blockSize, originalData.length);
    }

    public interface Longhair extends Library {
        Longhair INSTANCE = (Longhair) Native.loadLibrary("longhair", Longhair.class);

        /**
         * Verifies binary compatibility with the API on startup.
         *
         * @return non-zero on success; zero on failure.
         */
        int _cauchy_256_init(int version);

        //int cauchy_256_encode(int k, int m, const unsigned char *data_ptrs[], void *recovery_blocks, int block_bytes);

        /**
         * @param k           data blocks
         * @param m           recovery blocks
         * @param block_bytes number of bytes per block; multiple of 8
         * @return zero on success, another value indicates failure.
         */
        int cauchy_256_encode(int k, int m, Pointer[] data_ptrs, Pointer recovery_blocks, int block_bytes);

        //int cauchy_256_decode(int k, int m, Block *blocks, int block_bytes);

        /**
         * Recover original data
         *
         * @param k          num of original blocks
         * @param m          num of recovery blocks
         * @param blocks     blocks of data, original or recovery
         * @param blockBytes number of bytes per block; multiple of 8
         * @return 0 on success, otherwise failure
         */
        int cauchy_256_decode(int k, int m, Block[] blocks, int blockBytes);
    }

    public static class Block extends Structure {
        public Pointer data; // unsigned char *data
        public char row; // unsigned char row

        @Override
        protected List getFieldOrder() {
            return Arrays.asList(new String[]{"data", "row"});
        }

        public static class ByReference extends Block implements Structure.ByReference {}
    }

}
