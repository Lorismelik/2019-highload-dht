package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class RocksDAO implements DAO {

    private final RocksDB db;

    RocksDAO(final RocksDB db) {
        this.db = db;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final var iterator = db.newIterator();
        iterator.seek(ByteBufferUtils.shiftBytes(from));
        return new RocksRecordIterator(iterator);
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws RockException {
        try {
            final var result = db.get(ByteBufferUtils.shiftBytes(key));
            if (result == null) {
                throw new NoSuchElementExceptionLite("Cant find element with key " + key.toString());
            }
            return ByteBuffer.wrap(result);
        } catch (RocksDBException exception) {
            throw new RockException("Error while get", exception);
        }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws RockException {
        try {
            db.put(ByteBufferUtils.shiftBytes(key), ByteBufferUtils.toArray(value));
        } catch (RocksDBException exception) {
            throw new RockException("Error while upsert", exception);
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws RockException {
        try {
            db.delete(ByteBufferUtils.shiftBytes(key));
        } catch (RocksDBException exception) {
            throw new RockException("Error while remove", exception);
        }
    }

    @Override
    public void compact() throws RockException {
        try {
            db.compactRange();
        } catch (RocksDBException exception) {
            throw new RockException("Error while compact", exception);
        }
    }

    @Override
    public void close() throws RockException {
        try {
            db.syncWal();
            db.closeE();
        } catch (RocksDBException exception) {
            throw new RockException("Error while close", exception);
        }
    }
}
