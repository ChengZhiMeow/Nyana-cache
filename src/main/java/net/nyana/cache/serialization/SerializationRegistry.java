package net.nyana.cache.serialization;

import net.nyana.cache.registry.Registry;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class SerializationRegistry extends Registry<Class<?>, CacheSerializer<?>> {
    public static final byte[] NULL_VALUE = new byte[]{0};

    public SerializationRegistry() {
        super(true);
        this.register(byte[].class, new ByteArraySerializer());
        this.register(String.class, new StringSerializer());
        this.register(Integer.class, new IntegerSerializer());
        this.register(Long.class, new LongSerializer());
        this.register(Boolean.class, new BooleanSerializer());
        this.register(Double.class, new DoubleSerializer());
        this.register(Float.class, new FloatSerializer());
        this.register(Short.class, new ShortSerializer());
        this.register(Byte.class, new ByteSerializer());
        this.register(Character.class, new CharacterSerializer());
    }

    private static final class ByteArraySerializer implements CacheSerializer<byte[]> {
        @Override
        public byte @NotNull [] toBytes(byte @NotNull [] value) {
            return Arrays.copyOf(value, value.length);
        }

        @Override
        public byte @NotNull [] byBytes(byte @NotNull [] bytes) {
            return Arrays.copyOf(bytes, bytes.length);
        }
    }

    private static final class StringSerializer implements CacheSerializer<String> {
        @Override
        public byte @NotNull [] toBytes(@NotNull String value) {
            return value.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public @NotNull String byBytes(byte @NotNull [] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    private static final class IntegerSerializer implements CacheSerializer<Integer> {
        @Override
        public byte @NotNull [] toBytes(@NotNull Integer value) {
            return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
        }

        @Override
        public @NotNull Integer byBytes(byte @NotNull [] bytes) {
            return ByteBuffer.wrap(bytes).getInt();
        }
    }

    private static final class LongSerializer implements CacheSerializer<Long> {
        @Override
        public byte @NotNull [] toBytes(@NotNull Long value) {
            return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
        }

        @Override
        public @NotNull Long byBytes(byte @NotNull [] bytes) {
            return ByteBuffer.wrap(bytes).getLong();
        }
    }

    private static final class BooleanSerializer implements CacheSerializer<Boolean> {
        @Override
        public byte @NotNull [] toBytes(@NotNull Boolean value) {
            return new byte[]{(byte) (value ? 1 : 0)};
        }

        @Override
        public @NotNull Boolean byBytes(byte @NotNull [] bytes) {
            return bytes.length > 0 && bytes[0] != 0;
        }
    }

    private static final class DoubleSerializer implements CacheSerializer<Double> {
        @Override
        public byte @NotNull [] toBytes(@NotNull Double value) {
            return ByteBuffer.allocate(Double.BYTES).putDouble(value).array();
        }

        @Override
        public @NotNull Double byBytes(byte @NotNull [] bytes) {
            return ByteBuffer.wrap(bytes).getDouble();
        }
    }

    private static final class FloatSerializer implements CacheSerializer<Float> {
        @Override
        public byte @NotNull [] toBytes(@NotNull Float value) {
            return ByteBuffer.allocate(Float.BYTES).putFloat(value).array();
        }

        @Override
        public @NotNull Float byBytes(byte @NotNull [] bytes) {
            return ByteBuffer.wrap(bytes).getFloat();
        }
    }

    private static final class ShortSerializer implements CacheSerializer<Short> {
        @Override
        public byte @NotNull [] toBytes(@NotNull Short value) {
            return ByteBuffer.allocate(Short.BYTES).putShort(value).array();
        }

        @Override
        public @NotNull Short byBytes(byte @NotNull [] bytes) {
            return ByteBuffer.wrap(bytes).getShort();
        }
    }

    private static final class ByteSerializer implements CacheSerializer<Byte> {
        @Override
        public byte @NotNull [] toBytes(@NotNull Byte value) {
            return new byte[]{value};
        }

        @Override
        public @NotNull Byte byBytes(byte @NotNull [] bytes) {
            return bytes[0];
        }
    }

    private static final class CharacterSerializer implements CacheSerializer<Character> {
        @Override
        public byte @NotNull [] toBytes(@NotNull Character value) {
            return ByteBuffer.allocate(Character.BYTES).putChar(value).array();
        }

        @Override
        public @NotNull Character byBytes(byte @NotNull [] bytes) {
            return ByteBuffer.wrap(bytes).getChar();
        }
    }

}
