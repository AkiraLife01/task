package com.example.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class TaskApplication {

    private static final String url = "jdbc:postgresql://localhost:5432/postgres";
    private static final String user = "postgres";
    private static final String password = "13975+*Akira";

    public static void main(String[] args) throws URISyntaxException, IOException {
        SpringApplication.run(TaskApplication.class, args);

        File f = new File("C:\\Users\\Akira_Life\\IdeaProjects\\task\\src\\main\\resources\\test_source\\rule.json");
        byte[] bytes = Files.readAllBytes(f.toPath());
        String json = new String(bytes);
        ObjectMapper objectMapper = new ObjectMapper();

        Rule rule = objectMapper.readValue(json, Rule.class);


        StringBuilder query = new StringBuilder("SELECT * FROM ");
        //TODO сделать проверку на совпадающую таблицу, и указать НА ошибку ЯВНО
        //TODO если таблица повторяется в каждом Join из Joins то, выполняю 1
        query.append(rule.joins().get(0).table_left())
                .append(" ")
                .append(rule.joins().get(0).type().toUpperCase())
                .append(" JOIN").append(" ")
                .append(rule.joins().get(1)
                        .table_right());

        query.append(" ON ");
        for (Join join : rule.joins()) {
            query
                    .append(join.table_left())
                    .append(".")
                    .append(join.entity_left())
                    .append(" = ")
                    .append(join.table_right())
                    .append(".")
                    .append(join.entity_right())
                    .append(" ")
                    .append("AND")
                    .append(" ");
        }
        query.deleteCharAt(query.length() - 1);
        query.deleteCharAt(query.length() - 1);
        query.deleteCharAt(query.length() - 1);
        query.deleteCharAt(query.length() - 1);
        query.deleteCharAt(query.length() - 1);
        query.append(";");

        //TODO Выполнить Query +
        //TODO Превратить полученные из запроса данные в Объекты

        List<SelectedRow> selectedRows = new ArrayList<>();

        try{

            Class.forName("org.postgresql.Driver").getDeclaredConstructor().newInstance();

            try (Connection conn = connect()) {

                Statement statement = conn.createStatement();
                ResultSet resultSet = statement.executeQuery(query.toString());

                // TODO Пройтись по variables, и получить список
                // TODO Пройтись по списку и вытащить те данные которые указанны в variables
                // TODO Парсить VALUE из PARAMETERS
                //
                while (resultSet.next()) {
//                    BigDecimal fid = resultSet.getBigDecimal("fid");
//                    BigDecimal year = resultSet.getBigDecimal("year");
//                    BigDecimal quarter = resultSet.getBigDecimal("quarter");
//                    BigDecimal s40 = resultSet.getBigDecimal("s40");
//                    var dateReceipt = resultSet.getDate("date_receipt");
//                    var dateCreation = resultSet.getDate("date_creation");
//                    BigDecimal s120_3 = resultSet.getBigDecimal("s120_3");
//                    selectedRows.add(new SelectedRow(fid, year, quarter, s40, dateReceipt, dateCreation, s120_3));
                }

                for (SelectedRow selectedRow : selectedRows) {
                    System.out.println(selectedRow);
                }
            }
        } catch(Exception ex) {
            System.out.println("Connection failed...");

            System.out.println(ex);
        }

        System.out.println(query);
    }
    public static Connection connect() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }



}
