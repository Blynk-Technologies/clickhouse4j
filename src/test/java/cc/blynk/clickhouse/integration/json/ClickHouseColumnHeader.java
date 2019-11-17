package cc.blynk.clickhouse.integration.json;

import cc.blynk.clickhouse.domain.ClickHouseDataType;

/**
 * Example:
 *
 * {
 *   "name": "created",
 *    type": "DateTime"
 * }
 *
 * The Blynk Project.
 * Created by Dmitriy Dumanskiy.
 * Created on 16.11.2019.
 */
public final class ClickHouseColumnHeader {

    private String name;

    private ClickHouseDataType type;

    public ClickHouseColumnHeader() {
    }

    public String getName() {
        return name;
    }

    public ClickHouseDataType getType() {
        return type;
    }
}
