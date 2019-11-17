package cc.blynk.clickhouse.integration.json;

/**
 * Wrapper for the JSON response from the clickhouse client.
 * Example :
 *
 * {
 * 	"meta":
 * 	[
 *        {
 * 			"name": "created",
 * 			"type": "DateTime"
 *        },
 *        {
 * 			"name": "value",
 * 			"type": "Int32"
 *        }
 * 	],
 *
 * 	"data":
 * 	[
 *        {
 * 			"created": "2019-11-16 14:01:29",
 * 			"value": 1
 *        },
 *        {
 * 			"created": "2019-11-16 14:01:30",
 * 			"value": 2
 *        },
 *
 * 	],
 *
 * 	"rows": 8,
 *
 * 	"statistics": 	{
 * 		"elapsed": 0.000350996,
 * 		"rows_read": 2,
 * 		"bytes_read": 16
 * 	}
 * }
 *
 * The Blynk Project.
 * Created by Dmitriy Dumanskiy.
 * Created on 16.11.2019.
 */
public final class ClickHouseJsonResponse<T> {

    private ClickHouseColumnHeader[] meta;

    private T[] data;

    private long rows;

    private ClickHouseJsonResponseStatistics statistics;

    public ClickHouseJsonResponse() {
    }

    public ClickHouseColumnHeader[] getMeta() {
        return meta;
    }

    public T[] getData() {
        return data;
    }

    public long getRows() {
        return rows;
    }

    public ClickHouseJsonResponseStatistics getStatistics() {
        return statistics;
    }

}
