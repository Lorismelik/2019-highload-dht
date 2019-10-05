package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;
import org.rocksdb.*;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class RocksDAO implements DAO {

    private final RocksDB db;

    RocksDAO(RocksDB db) {
        this.db = db;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) {
        final var iterator = db.newIterator();
        iterator.seek(from.array());
        return new RocksRecordIterator(iterator);
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull ByteBuffer key) throws RockException {
        try {
            final var result = db.get(key.array());
            if (result == null) {
                throw new NoSuchElementExceptionLite("Cant find element with key " + key.toString());
            }
            return ByteBuffer.wrap(result);
        } catch (RocksDBException exception) {
            throw new RockException("Error while get", exception);
        }
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws RockException {
        try {
            db.put(key.array(), value.array());
        } catch (RocksDBException exception) {
            throw new RockException("Error while upsert", exception);
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws RockException {
        try {
            db.delete(key.array());
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
