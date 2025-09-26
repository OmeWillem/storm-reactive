<p align="center">
    <img src="https://github.com/user-attachments/assets/51521066-f973-472d-b66e-dd4fa4df67f5" />
</p>
<b>Storm Reactive</b> is a fast, easy-to-use, no-bullshit Java ORM inspired by Doctrine. Its main goal is to let future-mats (and others!) quickly implement SQL-based storage and relation solutions in projects, and prototype concepts without harming production usability.  

> **Note:** Storm Reactive is **NOT truly reactive**. It still uses JDBC under the hood, but comes with all the perks of [Project Reactor](https://projectreactor.io/).  

## Features
 - Automatic schema creation and updates based on models
 - Built in support for java types with an API to add your own
 - Support for OneToMany mappings with arraylist columns
 - Out-of-the-box adapters for Sqlite (flat file), Sqlite (Memory) and HiariCP-MariaDB
 - Dynamic SQL Dialects depending on the target platform
 - Incredibly easy to use API
 - Native mappings and support for:
   - Boolean
   - Double
   - Float
   - Integer
   - Long
   - String
   - UUID
   - Instant
   - Any java object as Blob (HashMap, etc)

# Performance
Tests ran on my main workstation, targeting a Sqlite flatfile and memory database.
![image](https://user-images.githubusercontent.com/10709682/156046029-537cf0dd-fd3b-4a6e-ab6b-17bf832046d4.png)

# Examples
```java
// create a model
@Data
@Table(name = "user")
class User extends StormModel {

    @Column
    private String userName;

    @Column
    private Integer score;

    @Column(
            type = ColumnType.ONE_TO_MANY,
            references = {SocialPost.class},
            matchTo = "poster"
    )
    private List<SocialPost> posts;

    @Column
    private UUID minecraftUserId = UUID.randomUUID();

    @Column(
            name = "email",
            defaultValue = "default@craftmend.com"
    )
    private String emailAddress;

}

public class SocialPost extends StormModel {

    @Column(
            notNull = true
    )
    private String content;

    @Column(
            keyType = KeyType.FOREIGN,
            references = {User.class}
    )
    private Integer poster;

}


// create an instance
Storm storm = new Storm(new SqliteDriver(dataFile));
// register one table
storm.migrate(new User());
storm.migrate(new SocialPost());
storm.runMigrations();

// create a new user
User mindgamesnl = new User();
mindgamesnl.setUserName("Mindgamesnl");
mindgamesnl.setEmailAddress("mats@toetmats.nl");
mindgamesnl.setScore(9009);

// You should always subscribe, see the methods as "blueprints"! To be fair, with saving you can keep the subscription empty.
storm.save(mindgamesnl).subscribe(id -> {
        // Let's find the user again by building a query!
        storm.buildQuery(User.class)
                    .where("user_name", Where.EQUAL, "Mindgamesnl")
                    .limit(1)
                    .execute().subscribe(user -> {
        System.out.println("We found the user! " + user.getId());
        });
});
```

# Usage with HikariCP
```java
// hikari
HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:mysql://localhost:3306/simpsons");
config.setUsername("bart");
config.setPassword("51mp50n");
config.addDataSourceProperty("cachePrepStmts", "true");
config.addDataSourceProperty("prepStmtCacheSize", "250");
config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

Storm storm = new Storm(new HikariDriver(config));
```
