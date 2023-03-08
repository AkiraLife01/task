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
import java.util.HashMap;
import java.util.List;

@SpringBootApplication
public class TaskApplication {

    private static final String url = "jdbc:postgresql://localhost:5432/postgres";
    private static final String user = "postgres";
    private static final String password = "13975+*Akira";

    public static void main(String[] args) throws URISyntaxException, IOException {
//        SpringApplication.run(TaskApplication.class, args);
//
//        File f = new File("C:\\Users\\Akira_Life\\IdeaProjects\\task\\src\\main\\resources\\test_source\\rule.json");
//        byte[] bytes = Files.readAllBytes(f.toPath());
//        String json = new String(bytes);
//        ObjectMapper objectMapper = new ObjectMapper();
//
//        Rule rule = objectMapper.readValue(json, Rule.class);
//        StringBuilder columns = new StringBuilder();
//
//        for (Join join : rule.joins()) {
//            columns.append(join.table_left()).append(".").append(join.entity_left()).append(", ");
//        }
//        columns.deleteCharAt(columns.length() - 1);
//        columns.deleteCharAt(columns.length() - 1);
//
//        StringBuilder query = new StringBuilder();
//        query.append("SELECT ")
//                        .append(columns)
//                                .append(" ")
//                                        .append("FROM ");
//        //TODO сделать проверку на совпадающую таблицу, и указать НА ошибку ЯВНО
//        //TODO если таблица повторяется в каждом Join из Joins то, выполняю 1
//
////        query.append(rule.joins().get(0).table_left())
////                .append(" ")
////                .append(rule.joins().get(0).type().toUpperCase())
////                .append(" ")
////                .append("JOIN")
////                .append(" ")
////                .append(rule.joins().get(0).table_right())
////                .append(" ")
////                .append("ON ")
////                .append(rule.joins().get(0).table_left())
////                .append(".")
////                .append(rule.joins().get(0).entity_left())
////                .append(" ")
////                .append("=")
////                .append(" ")
////                .append(rule.joins().get(0).table_right())
////                .append(".")
////                .append(rule.joins().get(0).entity_right())
////                .append(" ");
////
////
////
////        Join joinTemp = rule.joins().get(0);
////        int index = 0;
////
////
////        for (Join join : rule.joins()) {
////
////            if (join == joinTemp) {
////                continue;
////            }
////
////            String tableLeft = join.table_left();
////            String tableRight = join.table_right();
////            String entityLeft = join.entity_left();
////            String entityRight = join.entity_right();
////            String aliasOfTableRight = Character.toString(tableRight.charAt(0)) + index;
////            String aliasOfTableLeft = Character.toString(tableLeft.charAt(0)) + index;
////            String joinType = join.type().toUpperCase();
////
////
////            query.append(joinType)
////                    .append(" ")
////                    .append("JOIN")
////                    .append(" ")
////                    .append(tableLeft)
////                    .append(" ")
////                    .append(aliasOfTableLeft)
////                    .append(" ")
////                    .append("ON ")
////                    .append(aliasOfTableLeft)
////                    .append(".")
////                    .append(entityLeft)
////                    .append(" = ")
////                    .append(tableRight)
////                    .append(".")
////                    .append(entityRight)
////                    .append(" ");
////            index++;
////
////        }
////        query.deleteCharAt(query.length() - 1);
//////        query.deleteCharAt(query.length() - 1);
//////        query.deleteCharAt(query.length() - 1);
//////        query.deleteCharAt(query.length() - 1);
//////        query.deleteCharAt(query.length() - 1);
//        query.append(";");
//        System.out.println(query);
//        //TODO Выполнить Query +
//        //TODO Превратить полученные из запроса данные в Объекты
//
//
////        try {
////
////            Class.forName("org.postgresql.Driver").getDeclaredConstructor().newInstance();
////
////            try (Connection conn = connect()) {
////
////                Statement statement = conn.createStatement();
////                ResultSet resultSet = statement.executeQuery(query.toString());
////
////                // TODO Пройтись по variables, и получить список
////                // TODO Пройтись по списку и вытащить те данные которые указанны в variables
////                // TODO Парсить VALUE из PARAMETERS
////                //
//////                while (resultSet.next()) {
//////
//////                }
////
////
////            }
////        } catch (Exception ex) {
////            System.out.println("Connection failed...");
////
////            System.out.println(ex);
////        }


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
                .append(" JOIN ")
                .append(rule.joins().get(1).table_right())
                .append(" ON ");

        for (int i = 0; i < rule.joins().size(); i++) {
            query
                    .append(rule.joins().get(i).table_left())
                    .append(".")
                    .append(rule.joins().get(i).entity_left())
                    .append(" = ")
                    .append(rule.joins().get(i).table_right())
                    .append(".")
                    .append(rule.joins().get(i).entity_right());

            if (i != rule.joins().size() - 1) {
                query.append(" AND ");
            } else {
                query.append(";");
            }
        }

        //TODO Выполнить Query
        //TODO Превратить полученные из запроса данные в Объекты

        List<SelectedRow> selectedRows = new ArrayList<>();

        try {

            Class.forName("org.postgresql.Driver").getDeclaredConstructor().newInstance();

            try (Connection conn = connect()) {

                Statement statement = conn.createStatement();
                ResultSet resultSet = statement.executeQuery(query.toString());

                // TODO Пройтись по variables, и получить список
                // TODO Пройтись по списку и вытащить те данные которые указанны в variables
                // TODO Парсить VALUE из PARAMETERS

                List<Variable> variables = rule.variables();

                List<Row1> row1List = new ArrayList<>();

//                while (resultSet.next()) {
//
//                    List<Object> objects = new ArrayList<>();
//
//                    for (Variable variable : variables) {
//                        Object valueOfColumn = resultSet.getObject(variable.field());
//                        objects.add(valueOfColumn);
//                    }
//
//
//                    row1List.add(new Row1(objects));
//                }

                List<Variable2> variable2List = new ArrayList<>();

                HashMap<String, VariableData> variablesMap = new HashMap<>();

                while (resultSet.next()) {

                    for (Variable variable : variables) {
                        String nameOfCurrentVariable = variable.name();
                        var value = resultSet.getObject(variable.field());
                        if (variablesMap.containsKey(nameOfCurrentVariable)) {
                            variablesMap.get(nameOfCurrentVariable).values().add(value);
                        } else {
                            ArrayList<Object> values = new ArrayList<>(resultSet.getFetchSize());
                            values.add(value);
                            variablesMap.put(nameOfCurrentVariable, new VariableData(variable.type(), values));
                        }

                    }
                }

            }

        } catch (Exception ex) {
            System.out.println("Connection failed...");
        }


    }

    public static Connection connect() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }


}
