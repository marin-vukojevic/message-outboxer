package com.decodetamination.messageoutboxer;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.List;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class MessageRepository {

    private static final String INSERT = """
            insert into message_outbox (class, topic, serialized) values (:class, :topic, :serialized)
            """;
    private static final String SELECT_ALL = "select * from message_outbox order by id asc";
    private static final String DELETE_BY_ID = "delete from message_outbox where id = :id";

    private static final String ID_COLUMN = "id";
    private static final String CLASS_COLUMN = "class";
    private static final String TOPIC_COLUMN = "topic";
    private static final String SERIALIZED_COLUMN = "serialized";

    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    @PostConstruct
    @SneakyThrows
    private void initDb() {
        InputStream inputStream = new ClassPathResource("outboxer-schema.sql").getInputStream();
        String sql = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining(System.lineSeparator()));

        namedParameterJdbcTemplate.getJdbcTemplate().execute(sql);
    }

    public void save(Message<?> message) {
        MapSqlParameterSource mapSqlParameterSource = getMapSqlParameterSource(message);
        namedParameterJdbcTemplate.update(INSERT, mapSqlParameterSource);
    }

    public List<Message<?>> getAll() {
        return namedParameterJdbcTemplate.query(SELECT_ALL, this::map);
    }

    public void delete(Message<?> message) {
        MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue(ID_COLUMN, message.getId());

        namedParameterJdbcTemplate.update(DELETE_BY_ID, mapSqlParameterSource);
    }

    private MapSqlParameterSource getMapSqlParameterSource(Message<?> message) {
        MapSqlParameterSource mapSqlParameterSource = new MapSqlParameterSource();
        mapSqlParameterSource.addValue(CLASS_COLUMN, message.getClazz().getName());
        mapSqlParameterSource.addValue(TOPIC_COLUMN, message.getTopic());
        mapSqlParameterSource.addValue(SERIALIZED_COLUMN, message.getSerialized());
        return mapSqlParameterSource;
    }

    @SneakyThrows
    private Message<?> map(ResultSet resultSet, int i) {
        return new Message(
                resultSet.getLong(ID_COLUMN),
                Class.forName(resultSet.getString(CLASS_COLUMN)),
                resultSet.getString(TOPIC_COLUMN),
                resultSet.getBytes(SERIALIZED_COLUMN));
    }
}
