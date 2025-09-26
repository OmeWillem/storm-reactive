package com.craftmend.storm;

import com.craftmend.storm.api.StormModel;
import com.craftmend.storm.api.builders.QueryBuilder;
import com.craftmend.storm.connection.StormDriver;
import com.craftmend.storm.gson.InstantTypeAdapter;
import com.craftmend.storm.logger.StormLogger;
import com.craftmend.storm.parser.ModelParser;
import com.craftmend.storm.parser.objects.ParsedField;
import com.craftmend.storm.utils.ColumnDefinition;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.Getter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class Storm {

    private StormLogger logger;
    private final Map<Class<? extends StormModel>, ModelParser<? extends StormModel>> registeredModels = new HashMap<>();
    @Getter private final StormDriver driver;
    private boolean createdTables = false;
    @Getter private Gson gson;
    private final Scheduler blockingScheduler = Schedulers.boundedElastic();

    /**
     * Initialize a new STORM instance with a given database driver
     * @param options Sotmr options
     * @param driver Database driver
     */
    public Storm(StormOptions options, StormDriver driver) {
        this.logger = options.getLogger();
        this.driver = driver;
        GsonBuilder gb = new GsonBuilder();
        for (Map.Entry<Class<?>, Object> entry : options.getTypeAdapters().entrySet()) {
            Class<?> t = entry.getKey();
            Object a = entry.getValue();
            gb.registerTypeAdapter(t, a);
        }
        gb.registerTypeAdapter(Instant.class, new InstantTypeAdapter());
        gson = gb.create();
    }

    /**
     * Initialize a new STORM instance with a given database driver
     * @param driver Database driver
     */
    public Storm(StormDriver driver) {
        this(new StormOptions(), driver);
    }

    /**
     * Storm must register/migrate models before they can be used internally.
     * It registers the parsed class definition locally, but also plays a vital role in schema management.
     * <p>
     * It checks the remote if a table exists with the annotated name, and creates it if it doesn't (along with the
     *          field schema for all annotated columns)
     * <p>
     * It also checks the local schema class against the database, and alters the database table to add/remove
     * values based on dynamic code changes.
     *
     * @param model Model to register
     * @throws SQLException Something went boom
     */
    public void registerModel(StormModel model) throws SQLException {
        if (registeredModels.containsKey(model.getClass())) return;
        ModelParser<?> parsed = new ModelParser(model.getClass(), this, model);
        logger.info("Registering class <-> table (" + parsed.getTableName() +"<->" + model.getClass().getSimpleName() + ".java)");
        registeredModels.put(model.getClass(), parsed);
    }

    public void runMigrations() throws SQLException {
        for (Map.Entry<Class<? extends StormModel>, ModelParser<? extends StormModel>> entry : registeredModels.entrySet()) {
            ModelParser<? extends StormModel> parsed = entry.getValue();
            StormModel model = parsed.getEmptyInstance();
            if (parsed.isMigrated()) continue;

            try (ResultSet tables = driver.getMeta().getTables(null, null, parsed.getTableName(), null)) {
                if (!tables.next()) {
                    // table doesn't exist.. creating
                    logger.info("Creating table " + parsed.getTableName() + "...");
                    driver.execute(model.statements().buildSqlTableCreateStatement(driver.getDialect(), this));
                }
            }

            // find fields in table
            Map<String, String> columnsInDatabase = new HashMap<>();
            try (ResultSet tables = driver.getMeta().getColumns(null, null, parsed.getTableName(), null)) {
                while(tables.next()) {
                    String type = tables.getString("TYPE_NAME");
                    String name = tables.getString("COLUMN_NAME");
                    columnsInDatabase.put(name, type);
                }
            }

            // compare local tables to the ones in the database, we might need to add, remove or update some
            List<String> missingInDatabase = new ArrayList<>();
            for (ParsedField parsedField : parsed.getParsedFields()) {
                missingInDatabase.add(parsedField.getColumnName());
            }
            missingInDatabase.removeAll(columnsInDatabase.keySet());

            // compare local
            List<String> missingInLocal = new ArrayList<>();
            missingInLocal.addAll(columnsInDatabase.keySet());
            for (ParsedField parsedField : parsed.getParsedFields()) {
                missingInLocal.remove(parsedField.getColumnName());
            }

            // drop remote fields
            for (String columnName : missingInLocal) {
                logger.warning("Dropping column '" + columnName + "' because it's not present in the local class");
                driver.executeUpdate("ALTER TABLE %table DROP COLUMN %column;"
                        .replace("%table", parsed.getTableName())
                        .replace("%column", columnName));
            }

            // add remote fields
            for (String columnName : missingInDatabase) {
                // find type
                for (ParsedField parsedField : parsed.getParsedFields()) {
                    if (parsedField.getColumnName().equals(columnName)) {
                        logger.warning("Column '" + columnName + "' is not present in the remote table schema. Altering table and adding type " + driver.getDialect().compileColumn(parsedField));

                        ColumnDefinition cd = driver.getDialect().compileColumn(parsedField);
                        String sql = cd.getColumnSql();
                        if (cd.getConfigurationSql() != null) {
                            sql += ", " + cd.getConfigurationSql();
                        }

                        String statement = "ALTER TABLE %table ADD COLUMN %columnData;"
                                .replace("%table", parsed.getTableName())
                                .replace("%columnData", sql);

                        driver.executeUpdate(statement);
                    }
                }
            }

            parsed.setMigrated(true);
        }
        this.createdTables = true;
    }

    /**
     * @param model Start a new query
     * @return Query for you to play with
     */
    public <T extends StormModel> QueryBuilder<T> buildQuery(Class<T> model) {
        catchState();
        ModelParser<T> parser = (ModelParser<T>) registeredModels.get(model);
        if (parser == null) throw new IllegalArgumentException("The model " + model.getName() + " isn't loaded. Please call storm.migrate() with an empty instance");
        return new QueryBuilder<>(model, parser, this);
    }

    /**
     * Counts the total number of rows for a given model.
     * @param model The model class to count.
     * @return A {@link Mono} emitting the total count as an Integer.
     */
    public <T extends StormModel> Mono<Integer> count(Class<T> model) {
        catchState();
        return Mono.fromFuture(() -> {
            CompletableFuture<Integer> future = new CompletableFuture<>();
            String query = "SELECT COUNT(*) FROM " + getParsedModel(model, true).getTableName() + ";";
            try {
                driver.executeQuery(query, rows -> {
                    boolean found = false;
                    while (rows.next()) {
                        future.complete(rows.getInt(1));
                        found = true;
                    }

                    if (!found) {
                        future.complete(0);
                    }
                });
            } catch (Exception e) {
                future.completeExceptionally(e);
            }

            return future;
        }).subscribeOn(blockingScheduler);
    }

    /**
     * Execute query and streams the results.
     *
     * @param query Query to execute
     * @param <T> The type of the StormModel.
     * @return A {@link Flux} emitting each model instance found by the query.
     */
    public <T extends StormModel> Flux<T> executeQuery(QueryBuilder<T> query) {
        catchState();
        return Mono.fromFuture(() -> {
            CompletableFuture<Collection<T>> future = new CompletableFuture<>();
            List<T> results = new ArrayList<>();
            ModelParser<T> parser = (ModelParser<T>) registeredModels.get(query.getModel());
            if (parser == null) {
                future.completeExceptionally(new IllegalArgumentException("The model " + query.getModel().getName() + " isn't loaded. Please call storm.migrate() with an empty instance"));
                return future;
            }
            QueryBuilder.PreparedQuery pq = query.build();
            try {
                driver.executeQuery(pq.getQuery(), rows -> {
                    while (rows.next()) {
                        results.add(parser.fromResultSet(rows, parser.getRelationFields()));
                    }
                    future.complete(results);
                }, pq.getValues());
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
            return future;
        }).flatMapMany(Flux::fromIterable).subscribeOn(blockingScheduler);
    }

    /**
     * Get *ALL* models that are stored for a defined type.
     * This might be really memory intensive for large data sets.
     *
     * @param model Model to check
     * @param <T> The type of the StormModel.
     * @return A {@link Flux} emitting all found model instances.
     */
    public <T extends StormModel> Flux<T> findAll(Class<T> model) {
        catchState();
        return Mono.fromFuture(() -> {
            CompletableFuture<Collection<T>> future = new CompletableFuture<>();
            List<T> results = new ArrayList<>();
            ModelParser<T> parser = (ModelParser<T>) registeredModels.get(model);
            if (parser == null) {
                future.completeExceptionally(new IllegalArgumentException("The model " + model.getName() + " isn't loaded. Please call storm.migrate() with an empty instance"));
                return future;
            }

            try {
                driver.executeQuery("select * from " + parser.getTableName(), rows -> {
                    while (rows.next()) {
                        results.add(parser.fromResultSet(rows, parser.getRelationFields()));
                    }
                    future.complete(results);
                });
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
            return future;
        }).flatMapMany(Flux::fromIterable).subscribeOn(blockingScheduler);
    }

    /**
     * Deletes a model from the database.
     * @param model Delete a row
     * @return A {@link Mono} that completes when the deletion is finished.
     */
    public Mono<Void> delete(StormModel model) {
        return Mono.fromRunnable(() -> {
            catchState();
            model.preDelete();
            ModelParser parser = registeredModels.get(model.getClass());
            if (parser == null) throw new IllegalArgumentException("The model " + model.getClass().getName() + " isn't loaded. Please call storm.migrate() with an empty instance");
            if (model.getId() == null) throw new IllegalArgumentException("This model doesn't have an ID");
            try {
                driver.executeUpdate("DELETE FROM " + parser.getTableName() + " WHERE id=" + model.getId());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            model.postDelete();
        }).subscribeOn(blockingScheduler).then();
    }

    public <T extends StormModel> ModelParser<T> getParsedModel(Class<T> m, boolean loadIfNotFound) {
        ModelParser<T> parser = (ModelParser<T>) registeredModels.get(m);
        if (parser == null) {
            if (loadIfNotFound) {
                try {
                    StormModel sm = m.getConstructor().newInstance();
                    registerModel(sm);
                    return getParsedModel(m, false);
                } catch (SQLException | InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
                    // well... we tried!
                }
            }
            throw new IllegalArgumentException("The model " + m.getName() + " isn't loaded. Please call storm.migrate() with an empty instance");
        }
        return parser;
    }

    /**
     * Save the model in the database.
     * This either inserts it as a new row, or updates an existing row if there already is a row with this ID
     * @param model Target to save
     * @param <T> The type of the StormModel.
     * @return A {@link Mono} emitting the saved model instance, now including any generated ID.
     */
    public <T extends StormModel> Mono<T> save(T model) {
        return Mono.fromCallable(() -> {
            catchState();
            model.preSave();
            String updateOrInsert = "update %tableName set %psUpdateValues where id=%id";
            String insertStatement = "insert into %tableName(%insertVars) values(%insertValues);";

            // ps update value things
            StringBuilder updateValues = new StringBuilder();
            StringBuilder insertRow = new StringBuilder();
            String insertPointers = "";
            int nonAutoFields = model.parsed(this).getParsedFields().length;
            for (ParsedField parsedField : model.parsed(this).getParsedFields()) {
                if (parsedField.isAutoIncrement()) {
                    nonAutoFields--;
                }
            }
            Object[] preparedValues = new Object[nonAutoFields];
            int pvi = 0;
            for (int i = 0; i < model.parsed(this).getParsedFields().length; i++) {
                boolean notLast = (i+1) != nonAutoFields;
                ParsedField mf = model.parsed(this).getParsedFields()[i];
                if (mf.isAutoIncrement()) {
                    // skip auto fields
                    continue;
                }
                preparedValues[pvi] = mf.valueOn(model);
                pvi++;
                updateValues.append(mf.getColumnName() + " = ?");
                if (notLast) {
                    updateValues.append(", ");
                }

                insertRow.append(mf.getColumnName());
                insertPointers += "?";
                if (notLast) {
                    insertRow.append(", ");
                    insertPointers += ", ";
                }
            }

            insertStatement = insertStatement
                    .replace("%insertVars", insertRow.toString())
                    .replace("%insertValues", insertPointers)
                    .replaceAll("%tableName", model.parsed(this).getTableName());

            updateOrInsert = updateOrInsert
                    .replace("%psUpdateValues", updateValues.toString())
                    .replaceAll("%tableName", model.parsed(this).getTableName())
                    .replace("%id", model.getId() + "");


            if (model.getId() == null) {
                int o = driver.executeUpdate(insertStatement, preparedValues);
                model.setId(o);
                model.postSave();
            } else {
                driver.executeUpdate(updateOrInsert, preparedValues);
                model.postSave();
            }
            return model;
        }).subscribeOn(blockingScheduler);
    }

    private void catchState() {
        if (!createdTables) {
            throw new IllegalStateException("You must call runMigrations() to seed Storm before you can use any api methods");
        }
    }

}