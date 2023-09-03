package ru.otus.jdbc.mapper;

import ru.otus.core.repository.DataTemplate;
import ru.otus.core.repository.DataTemplateException;
import ru.otus.core.repository.executor.DbExecutor;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Сохратяет объект в базу, читает объект из базы
 */
public class DataTemplateJdbc<T> implements DataTemplate<T> {

    private final DbExecutor dbExecutor;
    private final EntitySQLMetaData entitySQLMetaData;
    private final EntityClassMetaData<T> entityClassMetaData;

    public DataTemplateJdbc(DbExecutor dbExecutor, EntitySQLMetaData entitySQLMetaData, EntityClassMetaData<T> entityClassMetaData
    ) {
        this.dbExecutor = dbExecutor;
        this.entitySQLMetaData = entitySQLMetaData;
        this.entityClassMetaData = entityClassMetaData;
    }

    @Override
    public Optional<T> findById(Connection connection, long id) {
        return dbExecutor.executeSelect(connection, entitySQLMetaData.getSelectByIdSql(), List.of(id), rs -> {
            try {
                if (rs.next()) {
                    return mapToEntity(rs);
                }
                return null;
            } catch (SQLException e) {
                throw new DataTemplateException(e);
            }
        });
    }

    @Override
    public List<T> findAll(Connection connection) {
        return dbExecutor.executeSelect(connection, entitySQLMetaData.getSelectAllSql(), Collections.emptyList(), rs -> {
            var entities = new ArrayList<T>();
            try {
                while (rs.next()) {
                    entities.add(mapToEntity(rs));
                }
                return entities;
            } catch (SQLException e) {
                throw new DataTemplateException(e);
            }
        }).orElseThrow(() -> new RuntimeException("Unexpected error"));
    }

    @Override
    public long insert(Connection connection, T entity) {
        try {
            return dbExecutor.executeStatement(connection, entitySQLMetaData.getInsertSql(),
                    getFieldValues(entity, entityClassMetaData.getFieldsWithoutId()));
        } catch (Exception e) {
            throw new DataTemplateException(e);
        }
    }

    @Override
    public void update(Connection connection, T entity) {
        try {
            dbExecutor.executeStatement(connection, entitySQLMetaData.getInsertSql(),
                    getFieldValues(entity, entityClassMetaData.getAllFields()));
        } catch (Exception e) {
            throw new DataTemplateException(e);
        }
    }

    private T mapToEntity(ResultSet rs) throws SQLException {
        try {
            var entity = entityClassMetaData.getConstructor().newInstance();

            entityClassMetaData.getAllFields().forEach(field -> {
                var accessible = field.isAccessible();
                field.setAccessible(true);
                try {
                    field.set(entity, rs.getObject(field.getName()));
                } catch (IllegalAccessException | SQLException e) {
                    throw new DataTemplateException(e);
                }
                finally {
                    field.setAccessible(accessible);
                }
            });
            return entity;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private List<Object> getFieldValues(T entity, List<Field> fields) {
        return fields.stream()
                .map(field -> {
                    var accessible = field.isAccessible();
                    field.setAccessible(true);
                    try {
                        return field.get(entity);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e.getMessage());
                    }
                    finally {
                        field.setAccessible(accessible);
                    }
                })
                .toList();
    }
}
