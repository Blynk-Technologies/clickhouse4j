package cc.blynk.clickhouse.domain;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ClickHouseFormatTest {

    @Test
    public void testNull() {
        assertFalse(ClickHouseFormat.containsFormat(null));
    }

    @Test
    public void testEmpty() {
        assertFalse(ClickHouseFormat.containsFormat(" \t \r\n"));
    }

    @Test
    public void testTrailingWhitespace() {
        assertFalse(ClickHouseFormat.containsFormat("FORMAT Phantasy  "));
        assertTrue(ClickHouseFormat.containsFormat("FORMAT TabSeparatedWithNamesAndTypes "));
        assertTrue(ClickHouseFormat.containsFormat("FORMAT TabSeparatedWithNamesAndTypes \t \n"));
    }

    @Test
    public void testTrailingSemicolon() {
        assertFalse(ClickHouseFormat.containsFormat("FORMAT Phantasy  ;"));
        assertTrue(ClickHouseFormat.containsFormat("FORMAT TabSeparatedWithNamesAndTypes ; "));
        assertTrue(ClickHouseFormat.containsFormat("FORMAT TabSeparatedWithNamesAndTypes ;"));
        assertTrue(ClickHouseFormat.containsFormat("FORMAT TabSeparatedWithNamesAndTypes \t ; \n"));
    }

    @Test
    public void testAllFormats() {
        for (ClickHouseFormat format : ClickHouseFormat.values()) {
            assertTrue(ClickHouseFormat.containsFormat("FORMAT " + format));
        }
    }

}
