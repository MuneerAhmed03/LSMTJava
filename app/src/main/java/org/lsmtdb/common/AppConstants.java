package org.lsmtdb.common;

public class AppConstants {

    private AppConstants(){}

    private static final String DEFAULT_BASE_DIR = System.getProperty("user.dir");
    public static String BASE_DIR = DEFAULT_BASE_DIR;
    public static String BASE_DB_PATH = BASE_DIR + "/test_db";
    public static final String MANIFEST_PATH = "manifest.json";

    public static synchronized void setBaseDirForTests(String baseDir) {
        if (baseDir == null || baseDir.isBlank()) {
            throw new IllegalArgumentException("baseDir cannot be null or blank");
        }
        BASE_DIR = baseDir;
        BASE_DB_PATH = BASE_DIR + "/test_db";
    }

    public static synchronized void resetBaseDirForTests() {
        BASE_DIR = DEFAULT_BASE_DIR;
        BASE_DB_PATH = BASE_DIR + "/test_db";
    }

}
