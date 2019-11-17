package cc.blynk.clickhouse.domain;

import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class ClickHouseFormatTest {

    @Test
    public void testNull() {
        ClickHouseFormat format = ClickHouseFormat.detectFormat(null);
        assertNull(format);
    }

    @Test
    public void testEmpty() {
        ClickHouseFormat format = ClickHouseFormat.detectFormat(" \t \r\n");
        assertNull(format);
    }

    @Test
    public void testTrailingWhitespace() {
        ClickHouseFormat format2 = ClickHouseFormat.detectFormat("FORMAT Phantasy  ");
        assertNull(format2);
        ClickHouseFormat format1 = ClickHouseFormat.detectFormat("FORMAT TabSeparatedWithNamesAndTypes ");
        assertNotNull(format1);
        ClickHouseFormat format = ClickHouseFormat.detectFormat("FORMAT TabSeparatedWithNamesAndTypes \t \n");
        assertNotNull(format);
    }

    @Test
    public void testTrailingSemicolon() {
        ClickHouseFormat format3 = ClickHouseFormat.detectFormat("FORMAT Phantasy  ;");
        assertNull(format3);
        ClickHouseFormat format2 = ClickHouseFormat.detectFormat("FORMAT TabSeparatedWithNamesAndTypes ; ");
        assertNotNull(format2);
        ClickHouseFormat format1 = ClickHouseFormat.detectFormat("FORMAT TabSeparatedWithNamesAndTypes ;");
        assertNotNull(format1);
        ClickHouseFormat format = ClickHouseFormat.detectFormat("FORMAT TabSeparatedWithNamesAndTypes \t ; \n");
        assertNotNull(format);
    }

    @Test
    public void testAllFormats() {
        for (ClickHouseFormat format : ClickHouseFormat.values()) {
            ClickHouseFormat format1 = ClickHouseFormat.detectFormat("FORMAT " + format);
            assertNotNull(format1);
        }
    }

}
