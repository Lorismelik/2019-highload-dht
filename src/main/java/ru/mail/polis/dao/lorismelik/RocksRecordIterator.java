package ru.mail.polis.dao.lorismelik;

import org.jetbrains.annotations.NotNull;
import org.rocksdb.RocksIterator;
import ru.mail.polis.Record;
import ru.mail.polis.dao.ByteBufferUtils;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class RocksRecordIterator implements Iterator<Record>, AutoCloseable {

    private final RocksIterator iterator;

    RocksRecordIterator(@NotNull final RocksIterator iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator.isValid();
    }

    @Override
    public Record next() throws IllegalStateException {
        if (!hasNext()) {
            throw new IllegalStateException("Iterator is exhausted");
        }
        final var key = ByteBufferUtils.revertShift(iterator.key());
        final var value = iterator.value();
        iterator.next();
        return Record.of(key, ByteBuffer.wrap(value));
    }

    @Override
    public void close() {
        iterator.close();
    }
}
