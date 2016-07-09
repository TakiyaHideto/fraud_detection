package com.mr.utils;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.List;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

/**
 * pb序列化为CTRL+A CTRL+B...样式的text格式，
 * 或者从text还原回来
 * @author cangyingzhijia
 *
 */
public class TextMessageCodec<T extends Message> implements ProtobufMessageCodec<T> {
	
	static String FIELD_SEPERATOR_ARRAY[] = {
		"\001", "\002", "\003", "\004",
		"\005", "\006", "\007", "\008",
		"\021", "\022", "\023", "\024",
		"\025", "\026", "\027", "\030",
	};
	
	@Override
	public String serializeToString(Message message) throws Exception {
		StringBuilder sb = new StringBuilder();
		serializeMessageToString(message, sb, 0);
		return sb.toString();
	}

	public boolean serializeMessageToString(Message message, StringBuilder sb, int level) throws UnsupportedEncodingException {
		Descriptor descriptor = message.getDescriptorForType();
		List<FieldDescriptor> fieldList = descriptor.getFields();
		int i = 0;
		for (FieldDescriptor field : fieldList) {
			Object value = message.getField(field);
			if (!serializeFieldToString(message, field, value, sb, level + 1)) {
				return false;
			}
			if (++i != fieldList.size()) {
				sb.append(FIELD_SEPERATOR_ARRAY[level]);
			}
		}
		return true;
	}

	private boolean serializeFieldToString(Message message, FieldDescriptor field, Object value,
			StringBuilder sb, int level) throws UnsupportedEncodingException {
		if (field.isRepeated()) {
			int i = 0;
			List<?> elementList = (List<?>) value;
			for (Object element : elementList) {
				serializeFieldValueToString(field, element, sb, level + 1);
				if (++i != elementList.size()) {
					sb.append(FIELD_SEPERATOR_ARRAY[level]);
				}
			}
			return true;
		}
		if (message.hasField(field)) {
			return serializeFieldValueToString(field, value, sb, level);
		}
		return true;
	}

	private boolean serializeFieldValueToString(FieldDescriptor field, Object value,
			StringBuilder sb, int level) throws UnsupportedEncodingException {
		switch (field.getType()) {
		case INT32:
		case SINT32:
		case SFIXED32:
			sb.append(((Integer) value).toString());
			break;

		case INT64:
		case SINT64:
		case SFIXED64:
			sb.append(((Long) value).toString());
			break;

		case BOOL:
			sb.append(((Boolean) value).toString());
			break;

		case FLOAT:
			sb.append(((Float) value).toString());
			break;

		case DOUBLE:
			sb.append(((Double) value).toString());
			break;

		case UINT32:
		case FIXED32:
			sb.append(unsignedToString((Integer) value));
			break;

		case UINT64:
		case FIXED64:
			sb.append(unsignedToString((Long) value));
			break;

		case STRING:
			sb.append(escapeText((String) value));
			break;

		case BYTES:
			sb.append(escapeText((String) value));
			break;

		case ENUM:
			sb.append(((EnumValueDescriptor) value).getNumber());
			break;

		case MESSAGE:
		case GROUP:
			return serializeMessageToString((Message) value, sb, level);
		}
		return true;
	}

	static String escapeText(final String input) throws UnsupportedEncodingException {
		return escapeBytes(input);
	}

	/** Convert an unsigned 32-bit integer to a string. */
	private static String unsignedToString(final int value) {
		if (value >= 0) {
			return Integer.toString(value);
		} else {
			return Long.toString(((long) value) & 0x00000000FFFFFFFFL);
		}
	}

	/** Convert an unsigned 64-bit integer to a string. */
	private static String unsignedToString(final long value) {
		if (value >= 0) {
			return Long.toString(value);
		} else {
			// Pull off the most-significant bit so that BigInteger doesn't
			// think
			// the number is negative, then set it again using setBit().
			return BigInteger.valueOf(value & 0x7FFFFFFFFFFFFFFFL).setBit(63).toString();
		}
	}

	static String escapeBytes(final String input) throws UnsupportedEncodingException {
		final StringBuilder builder = new StringBuilder(input.length());
		for (int i = 0; i < input.length(); i++) {
			final char b = input.charAt(i);
			switch (b) {
			case '\001':
				builder.append("\\^A");
				break;
			case '\002':
				builder.append("\\^B");
				break;
			case '\003':
				builder.append("\\^C");
				break;
			case '\004':
				builder.append("\\^D");
				break;
			case '\005':
				builder.append("\\^E");
				break;
			case '\006':
				builder.append("\\^F");
				break;
			case '\007':
				builder.append("\\^G");
				break;
			case '\010':
				builder.append("\\^H");
				break;
			case '\021':
				builder.append("\\^Q");
				break;
			case '\022':
				builder.append("\\^R");
				break;
			case '\023':
				builder.append("\\^S");
				break;
			case '\024':
				builder.append("\\^T");
				break;
			case '\025':
				builder.append("\\^U");
				break;
			case '\026':
				builder.append("\\^V");
				break;
			case '\027':
				builder.append("\\^W");
				break;
			case '\030':
				builder.append("\\^X");
				break;
			case '\n':
				builder.append("\\n");
				break;
			case '\\':
				builder.append("\\\\");
				break;
			default:
				builder.append(b);
				break;
			}
		}
		return builder.toString();
	}

	@Override
	public T parseFromString(String input, Builder builder) {
		parseMessageFromString(input, builder, 0);
		return (T)builder.build();
	}

	private void parseMessageFromString(String input, Builder builder, int level) {
		String[] tokenArr = input.split(FIELD_SEPERATOR_ARRAY[level], -1);
		Descriptor descriptor = builder.getDescriptorForType();
		List<FieldDescriptor> fdList = descriptor.getFields();
		int count = tokenArr.length < fdList.size() ? tokenArr.length : fdList.size();
		for (int i = 0; i < count; ++i) {
			parseMessageFieldFromString(tokenArr[i], fdList.get(i), builder, level + 1);
		}
	}

	private void parseMessageFieldFromString(String token,
			FieldDescriptor fieldDescriptor, Builder builder, int level) {
		if (token.equals("")) {
			return;
		}
		if (fieldDescriptor.isRepeated()) {
			String[] elementTokenArr = token.split(FIELD_SEPERATOR_ARRAY[level], -1);
			for (String elementToken : elementTokenArr) {
				Object value = parseMessageFieldFieldValueFromString(elementToken, fieldDescriptor,
						builder, level + 1);
				builder.addRepeatedField(fieldDescriptor, value);
			}
		} else {
			Object value = parseMessageFieldFieldValueFromString(token, fieldDescriptor, builder, level);
			builder.setField(fieldDescriptor, value);
		}
	}

	private Object parseMessageFieldFieldValueFromString(String elementToken,
			FieldDescriptor fieldDescriptor, Builder builder, int level) {
		Object value = null;
		if (fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
			final Message.Builder subBuilder = builder
					.newBuilderForField(fieldDescriptor);
			parseMessageFromString(elementToken, subBuilder, level);
			value = subBuilder.build();
		} else {
			switch (fieldDescriptor.getType()) {
			case INT32:
			case SINT32:
			case SFIXED32:
				value = parseInt32(elementToken);
				break;

			case INT64:
			case SINT64:
			case SFIXED64:
				value = parseInt64(elementToken);
				break;

			case UINT32:
			case FIXED32:
				value = parseUInt32(elementToken);
				break;

			case UINT64:
			case FIXED64:
				value = parseUInt64(elementToken);
				break;

			case FLOAT:
				value = Float.parseFloat(elementToken);
				break;

			case DOUBLE:
				value = Double.parseDouble(elementToken);
				break;

			case BOOL:
				value = parseBoolean(elementToken);
				break;

			case STRING:
				value = unescapeText(elementToken);
				break;

			case BYTES:
				value = unescapeText(elementToken);
				break;

			case ENUM:
				final EnumDescriptor enumType = fieldDescriptor.getEnumType();
				value = enumType.findValueByName(elementToken);
				if (value == null) {
					final int number = Integer.parseInt(elementToken);
					value = enumType.findValueByNumber(number);
				}
				if (value == null) {
					throw new RuntimeException(String.format("parse enum error#enumType=%s, token=%s", enumType, elementToken));
				}
				break;
			case MESSAGE:
			case GROUP:
				throw new RuntimeException("Can't get here.");
			}
		}
		return value;
	}

	static private Object unescapeBytes(String input) {
		final StringBuilder builder = new StringBuilder(input.length());
		for (int i = 0; i < input.length(); i++) {
			char first = input.charAt(i);
			if (first != '\\') {
				builder.append(first);
				continue;
			}
			// match ^ \
			if (++i >= input.length()) {
				builder.append(first);
				break;
			}
			char second = input.charAt(i);
			if (second == 'n') {
				builder.append('\n');
				continue;
			} else if (second == '\\') {
				builder.append('\\');
				continue;
			}
			// match ctrl A - H
			if (++i >= input.length()) {
				builder.append(first);
				builder.append(second);				
				break;
			}
			char third = input.charAt(i);
			switch (third) {
			case 'A':
				builder.append('\001');
				break;
			case 'B':
				builder.append('\002');
				break;
			case 'C':
				builder.append('\003');
				break;
			case 'D':
				builder.append('\004');
				break;
			case 'E':
				builder.append('\005');
				break;
			case 'F':
				builder.append('\006');
				break;
			case 'G':
				builder.append('\007');
				break;
			case 'H':
				builder.append('\010');
				break;
			case 'Q':
				builder.append('\021');
				break;
			case 'R':
				builder.append('\022');
				break;
			case 'S':
				builder.append('\023');
				break;
			case 'T':
				builder.append('\024');
				break;
			case 'U':
				builder.append('\025');
				break;
			case 'V':
				builder.append('\026');
				break;
			case 'W':
				builder.append('\027');
				break;
			case 'X':
				builder.append('\030');
				break;
			default:
				builder.append(first);
				builder.append(second);
				builder.append(third);
			}
		}
		return builder.toString();
	}

	static private Object unescapeText(String input) {
		return unescapeBytes(input);
	}

	static private Object parseBoolean(String elementToken) {
		if (elementToken.equalsIgnoreCase("true") || elementToken.equals("1")) {
			return true;
		}
		return false;
	}

	/**
	 * Parse a 32-bit signed integer from the text. Unlike the Java standard
	 * {@code Integer.parseInt()}, this function recognizes the prefixes "0x"
	 * and "0" to signify hexidecimal and octal numbers, respectively.
	 */
	static int parseInt32(final String text) throws NumberFormatException {
		return (int) parseInteger(text, true, false);
	}

	/**
	 * Parse a 32-bit unsigned integer from the text. Unlike the Java standard
	 * {@code Integer.parseInt()}, this function recognizes the prefixes "0x"
	 * and "0" to signify hexidecimal and octal numbers, respectively. The
	 * result is coerced to a (signed) {@code int} when returned since Java has
	 * no unsigned integer type.
	 */
	static int parseUInt32(final String text) throws NumberFormatException {
		return (int) parseInteger(text, false, false);
	}

	/**
	 * Parse a 64-bit signed integer from the text. Unlike the Java standard
	 * {@code Integer.parseInt()}, this function recognizes the prefixes "0x"
	 * and "0" to signify hexidecimal and octal numbers, respectively.
	 */
	static long parseInt64(final String text) throws NumberFormatException {
		return parseInteger(text, true, true);
	}

	/**
	 * Parse a 64-bit unsigned integer from the text. Unlike the Java standard
	 * {@code Integer.parseInt()}, this function recognizes the prefixes "0x"
	 * and "0" to signify hexidecimal and octal numbers, respectively. The
	 * result is coerced to a (signed) {@code long} when returned since Java has
	 * no unsigned long type.
	 */
	static long parseUInt64(final String text) throws NumberFormatException {
		return parseInteger(text, false, true);
	}

	private static long parseInteger(final String text, final boolean isSigned,
			final boolean isLong) throws NumberFormatException {
		int pos = 0;

		boolean negative = false;
		if (text.startsWith("-", pos)) {
			if (!isSigned) {
				throw new NumberFormatException("Number must be positive: " + text);
			}
			++pos;
			negative = true;
		}

		int radix = 10;
		if (text.startsWith("0x", pos)) {
			pos += 2;
			radix = 16;
		} else if (text.startsWith("0", pos)) {
			radix = 8;
		}

		final String numberText = text.substring(pos);

		long result = 0;
		if (numberText.length() < 16) {
			// Can safely assume no overflow.
			result = Long.parseLong(numberText, radix);
			if (negative) {
				result = -result;
			}

			// Check bounds.
			// No need to check for 64-bit numbers since they'd have to be 16
			// chars
			// or longer to overflow.
			if (!isLong) {
				if (isSigned) {
					if (result > Integer.MAX_VALUE
							|| result < Integer.MIN_VALUE) {
						throw new NumberFormatException("Number out of range for 32-bit signed integer: " + text);
					}
				} else {
					if (result >= (1L << 32) || result < 0) {
						throw new NumberFormatException("Number out of range for 32-bit unsigned integer: " + text);
					}
				}
			}
		} else {
			BigInteger bigValue = new BigInteger(numberText, radix);
			if (negative) {
				bigValue = bigValue.negate();
			}

			// Check bounds.
			if (!isLong) {
				if (isSigned) {
					if (bigValue.bitLength() > 31) {
						throw new NumberFormatException("Number out of range for 32-bit signed integer: " + text);
					}
				} else {
					if (bigValue.bitLength() > 32) {
						throw new NumberFormatException("Number out of range for 32-bit unsigned integer: " + text);
					}
				}
			} else {
				if (isSigned) {
					if (bigValue.bitLength() > 63) {
						throw new NumberFormatException("Number out of range for 64-bit signed integer: " + text);
					}
				} else {
					if (bigValue.bitLength() > 64) {
						throw new NumberFormatException("Number out of range for 64-bit unsigned integer: " + text);
					}
				}
			}

			result = bigValue.longValue();
		}
		
		return result;
	}

}