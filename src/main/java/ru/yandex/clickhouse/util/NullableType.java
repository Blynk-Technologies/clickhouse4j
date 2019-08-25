package ru.yandex.clickhouse.util;

/**
 * The Blynk Project.
 * Created by Dmitriy Dumanskiy.
 * Created on 25.08.2019.
 */
public enum NullableType {

    YES,
    NO;

    public static boolean isNullable(String type) {
        return type.startsWith("Nullable(") && type.endsWith(")");
    }

    public static NullableType resolve(String type) {
        if (isNullable(type)) {
            return YES;
        }
        return NO;
    }

    static String unwrapNullable(String clickshouseType) {
        return clickshouseType.substring("Nullable(".length(), clickshouseType.length() - 1);
    }

    public static String unwrapNullableIfApplicable(String clickhouseType) {
        return isNullable(clickhouseType)
                ? unwrapNullable(clickhouseType)
                : clickhouseType;
    }
}
