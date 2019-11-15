package ru.mail.polis.dao.lorismelik;

import org.jetbrains.annotations.NotNull;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import ru.mail.polis.Record;
import ru.mail.polis.dao.ByteBufferUtils;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.TimestampRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class RocksDAO implements DAO {

    private final RocksDB db;

    public RocksDAO(final RocksDB db) {
        this.db = db;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final var iterator = db.newIterator();
        iterator.seek(ByteBufferUtils.shiftBytes(from));
        return new RocksRecordIterator(iterator);
    }

    /**
     * Get record from DB.
     *
     * @param keys to define key
     * @return record
     */
    @NotNull
    public TimestampRecord getRecordWithTimestamp(@NotNull final ByteBuffer keys)
            throws IOException, NoSuchElementException {
        try {
            final byte[] packedKey = ByteBufferUtils.shiftBytes(keys);
            final byte[] valueByteArray = db.get(packedKey);
            if (valueByteArray.length == 0) {
                return TimestampRecord.getEmpty();
            }
            return TimestampRecord.fromBytes(valueByteArray);
        } catch (RocksDBException exception) {
            throw new RockException("Error while get", exception);
        }
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

    /**
     * Put record into DB.
     *
     * @param keys   to define key
     * @param values to define value
     */
    public void upsertRecordWithTimestamp(@NotNull final ByteBuffer keys,
                                          @NotNull final ByteBuffer values) throws IOException {
        try {
            final var record = TimestampRecord.fromValue(values, System.currentTimeMillis());
            final byte[] packedKey = ByteBufferUtils.shiftBytes(keys);
            final byte[] arrayValue = record.toBytes();
            db.put(packedKey, arrayValue);
        } catch (RocksDBException e) {
            throw new RockException("Upsert method exception!", e);
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

    /**
     * Delete record from DB.
     *
     * @param key to define key
     */
    public void removeRecordWithTimestamp(@NotNull final ByteBuffer key) throws IOException {
        try {
            final byte[] packedKey = ByteBufferUtils.shiftBytes(key);
            final var record = TimestampRecord.tombstone(System.currentTimeMillis());
            final byte[] arrayValue = record.toBytes();
            db.put(packedKey, arrayValue);
        } catch (RocksDBException e) {
            throw new RockException("Remove method exception!", e);
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
