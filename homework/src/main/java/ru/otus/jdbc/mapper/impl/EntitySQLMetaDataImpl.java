package ru.otus.jdbc.mapper.impl;

import ru.otus.jdbc.mapper.EntityClassMetaData;
import ru.otus.jdbc.mapper.EntitySQLMetaData;

import java.lang.reflect.Field;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class EntitySQLMetaDataImpl<T> implements EntitySQLMetaData {

    private final EntityClassMetaData<T> entityClassMetaData;

    public EntitySQLMetaDataImpl(EntityClassMetaData<T> entityClassMetaData) {
        this.entityClassMetaData = entityClassMetaData;
    }

    @Override
    public String getSelectAllSql() {
        return format("select * from %s", entityClassMetaData.getName());
    }

    @Override
    public String getSelectByIdSql() {
        return format("select * from %s where %s  = ?",
                entityClassMetaData.getName(), entityClassMetaData.getIdField().getName());
    }

    @Override
    public String getInsertSql() {
        var fields = entityClassMetaData.getFieldsWithoutId().stream()
                .map(Field::getName)
                .collect(joining(", "));

        var mask = entityClassMetaData.getFieldsWithoutId().stream()
                .map(field -> "?")
                .collect(joining(", "));

        return format("insert into %s (%s) values (%s)", entityClassMetaData.getName(), fields, mask);
    }

    @Override
    public String getUpdateSql() {
        var fields = entityClassMetaData.getFieldsWithoutId().stream()
                .map(field -> format("%s = ?", field.getName()))
                .collect(joining(", "));

        return format("update %s set %s where %s = ?", entityClassMetaData.getName(), fields, entityClassMetaData.getIdField().getName());
    }
}
