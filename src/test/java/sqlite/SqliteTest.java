package sqlite;

import com.craftmend.storm.Storm;
import com.craftmend.storm.api.enums.Where;
import com.craftmend.storm.connection.sqlite.SqliteFileDriver;
import lombok.SneakyThrows;
import models.SocialPost;
import models.User;
import org.junit.Assert;
import org.junit.Test;
import reactor.core.publisher.Flux;

import java.io.File;
import java.util.Collection;
import java.util.UUID;

public class SqliteTest {

    // TODO make better tests
    @Test
    @SneakyThrows
    public void testSqlite() {
        File dataFile = new File("test-data/database.db");
        dataFile.mkdirs();
        if (dataFile.exists()) dataFile.delete();

        Storm storm = new Storm(new SqliteFileDriver(dataFile));
        storm.registerModel(new User());
        storm.registerModel(new SocialPost());

        storm.runMigrations();

        // create a new user
        User mindgamesnl = new User();
        mindgamesnl.setUserName("Mindgamesnl");
        mindgamesnl.setEmailAddress("mats@toetmats.nl");
        mindgamesnl.setScore(9009);
        storm.save(mindgamesnl).block();

        User niceFriend = new User();
        niceFriend.setUserName("Some Friend");
        niceFriend.setEmailAddress("friend@test.com");
        niceFriend.setScore(50);
        storm.save(niceFriend).block();

        User randomBloke = new User();
        randomBloke.setUserName("Random Bloke");
        randomBloke.setEmailAddress("whatever@sheeesh.com");
        randomBloke.setScore(394);
        storm.save(randomBloke).block();

        // try to find all users
        Collection<User> allUsers = storm.findAll(User.class).collectList().block();
        Assert.assertNotNull(allUsers);
        Assert.assertEquals(3, allUsers.size());

        // check if all UUID's are loaded propery
        for (User allUser : allUsers) {
            Assert.assertNotNull(allUser.getMinecraftUserId());
            Assert.assertEquals(UUID.class, allUser.getMinecraftUserId().getClass());
        }

        // test queries
        Flux<User> justMindgamesnl =
                storm.buildQuery(User.class)
                        .where("user_name", Where.EQUAL, "Mindgamesnl")
                        .limit(1)
                        .execute();

        Assert.assertNotNull(justMindgamesnl);
        Assert.assertEquals("Mindgamesnl", justMindgamesnl.blockFirst().getUserName());

        Collection<User> two = storm.buildQuery(User.class).limit(2).execute().collectList().block();
        Assert.assertNotNull(two);
        Assert.assertEquals(2, two.size());
    }

}
