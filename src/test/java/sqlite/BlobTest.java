package sqlite;

import com.craftmend.storm.Storm;
import com.craftmend.storm.connection.sqlite.SqliteFileDriver;
import lombok.SneakyThrows;
import models.SqlMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class BlobTest {

    // TODO make better tests
    @Test
    @SneakyThrows
    public void testSqlite() {
        File dataFile = new File("test-data/database.db");
        dataFile.mkdirs();
        if (dataFile.exists()) dataFile.delete();

        Storm storm = new Storm(new SqliteFileDriver(dataFile));
        storm.registerModel(new SqlMap());

        storm.runMigrations();

        // create a new user
        SqlMap m = new SqlMap();
        m.setMapName("Sample map");
        m.getKeyValue().put("a", "AAA");
        m.getKeyValue().put("b", "BBB");
        m.getKeyValue().put("c", "CCC");
        storm.save(m).block();

        SqlMap l = storm.buildQuery(SqlMap.class).execute().blockFirst();
        Assert.assertNotNull(l);
        Assert.assertEquals("Sample map", l.getMapName());

        Assert.assertEquals("AAA", l.getKeyValue().get("a"));
        Assert.assertEquals("BBB", l.getKeyValue().get("b"));
        Assert.assertEquals("CCC", l.getKeyValue().get("c"));
    }

}
