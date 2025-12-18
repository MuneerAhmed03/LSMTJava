package org.lsmtdb.common;

/**
 * Utility class for formatting keys with numeric components to support
 * lexicographic sorting. Numbers are zero-padded to ensure correct ordering.
 */
public class KeyFormatter {

    private KeyFormatter() {}

    /**
     * Format a key with a numeric suffix, padding the number to ensure
     * lexicographic sorting works correctly.
     * 
     * @param prefix The string prefix (e.g., "key")
     * @param number The numeric value to append
     * @param maxNumber The maximum expected number (used to determine padding width)
     * @return Formatted key with zero-padded number (e.g., "key0000001")
     */
    public static String formatKey(String prefix, int number, int maxNumber) {
        int width = String.valueOf(maxNumber).length();
        return String.format("%s%0" + width + "d", prefix, number);
    }

    /**
     * Format a key with a numeric suffix using default padding width.
     * 
     * @param prefix The string prefix (e.g., "key")
     * @param number The numeric value to append
     * @param paddingWidth The width to pad the number to
     * @return Formatted key with zero-padded number
     */
    public static String formatKeyWithWidth(String prefix, int number, int paddingWidth) {
        return String.format("%s%0" + paddingWidth + "d", prefix, number);
    }

    /**
     * Format a number with zero-padding to specified width.
     * 
     * @param number The number to format
     * @param width The desired width (number of digits)
     * @return Zero-padded number string
     */
    public static String padNumber(int number, int width) {
        return String.format("%0" + width + "d", number);
    }
}

