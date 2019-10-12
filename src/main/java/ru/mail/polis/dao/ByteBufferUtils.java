package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static java.lang.Byte.MIN_VALUE;

public final class ByteBufferUtils {

    private ByteBufferUtils(){}

    /**
     * shiftBytes.
     *
     * @param buffer - buffer
     * @return byte
     */
    public static byte[] shiftBytes(@NotNull final ByteBuffer buffer) {
        final var copy = clone(buffer);
        final var array = new byte[copy.remaining()];
        copy.get(array);
        for (int i = 0; i < array.length; i++) {
            array[i] -= MIN_VALUE;
        }
        return array;
    }

    /**
     * revertShift.
     *
     * @param array - array
     * @return ByteBuffer
     */
    public static ByteBuffer revertShift(@NotNull final byte[] array) {
        final var arrayCopy = Arrays.copyOf(array, array.length);
        for (int i = 0; i < arrayCopy.length; i++) {
            arrayCopy[i] += MIN_VALUE;
        }
        return ByteBuffer.wrap(arrayCopy);
    }


    /**
     * clone.
     *
     * @param original - original
     * @return ByteBuffer
     */
    public static ByteBuffer clone(final ByteBuffer original) {
        final ByteBuffer clone = ByteBuffer.allocate(original.capacity());
        original.rewind();
        clone.put(original);
        original.rewind();
        clone.flip();
        return clone;
    }

    /**
     * toArray.
     *
     * @param buffer - buffer
     * @return byte
     */
    public static byte[] toArray(@NotNull final ByteBuffer buffer) {
        final var bufferCopy = buffer.duplicate();
        final var array = new byte[bufferCopy.remaining()];
        bufferCopy.get(array);
        return array;
    }
}
