package com.example.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

@SpringBootApplication
public class TaskApplication {

    private static final String url = "jdbc:postgresql://localhost:5432/postgres";
    private static final String user = "postgres";
    private static final String password = "13975+*Akira";

    public static void main(String[] args) throws IOException {
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

        System.out.println(query);

        int a = 0;

        try {


            try (Connection conn = connect()) {

                Statement statement = conn.createStatement();
                ResultSet resultSet = statement.executeQuery(query.toString());

                List<Variable> variables = rule.variables();

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
                System.out.println();

                assert variables.size() > 0;


                var listOfParameters = new ArrayList<Parameter2>();

                for (Parameter parameter : rule.parameters()) {
                    var listOfField = new ArrayList<String>();
                    var listOfOperator = new ArrayList<String>();
                    StringBuilder fieldResult = new StringBuilder();

                    for (int i = 0; i < parameter.value().length(); i++) {

                        char currentSymbol = parameter.value().charAt(i);

                        if (currentSymbol == '+' || currentSymbol == '-' ||
                                currentSymbol == '*' || currentSymbol == '/') {
                            listOfOperator.add(Character.toString(currentSymbol));
                            listOfField.add(fieldResult.toString());
                            fieldResult = new StringBuilder();
                        } else {
                            fieldResult.append(currentSymbol);
                        }

                        if (i == parameter.value().length() - 1) {
                            listOfField.add(fieldResult.toString());
                        }
                    }
                    listOfParameters.add(new Parameter2(parameter.name(), parameter.type(),
                            listOfField, listOfOperator));
                }

                System.out.println();

                var parameterResult = new ArrayList<ParameterResult>();
                final int setSize = variablesMap.get(variables.get(0).name()).values().size();

                BigDecimal parameterValue1;
                BigDecimal parameterValue2;
                BigDecimal resultValue = null;

                for (Parameter2 parameter2 : listOfParameters) {

                    for (int i = 0; i < setSize; i++) {
                        a = i;

                        switch (parameter2.listOfOperators().get(0)) {
                            case "+" -> {
                                parameterValue1 = (BigDecimal) variablesMap.get(parameter2.listOfFields().get(0)).values().get(i);
                                parameterValue2 = (BigDecimal) variablesMap.get(parameter2.listOfFields().get(1)).values().get(i);
                                resultValue = parameterValue1.add(parameterValue2);
                                System.out.println(resultValue);
                            }
                            case "-" -> {
                                parameterValue1 = (BigDecimal) variablesMap.get(parameter2.listOfFields().get(0)).values().get(i);
                                parameterValue2 = (BigDecimal) variablesMap.get(parameter2.listOfFields().get(1)).values().get(i);
                                resultValue = parameterValue1.subtract(parameterValue2);
                                System.out.println(resultValue);
                            }
                            case "/" -> {
                                parameterValue1 = (BigDecimal) variablesMap.get(parameter2.listOfFields().get(0)).values().get(i);
                                parameterValue2 = (BigDecimal) variablesMap.get(parameter2.listOfFields().get(1)).values().get(i);
                                try {
                                    resultValue = parameterValue1.divide(parameterValue2, 3, RoundingMode.DOWN);

                                } catch (ArithmeticException arithmeticException) {
                                    System.out.println("Деление на 0");
                                }
                                System.out.println(resultValue);

                            }
                            case "*" -> {
                                parameterValue1 = (BigDecimal) variablesMap.get(parameter2.name()).values().get(i);
                                parameterValue2 = (BigDecimal) variablesMap.get(parameter2.name()).values().get(i);
                                resultValue = parameterValue1.multiply(parameterValue2);
                                System.out.println(resultValue);
                            }
                        }
                        parameterResult.add(new ParameterResult(parameter2.name(), resultValue));
                    }

                }
                System.out.println("sssssssss");



            } catch (SQLException ex) {
                System.out.println("Connection failed...");

            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println(a);
        }


    }

    public static Connection connect() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
}


// пока не использую параметры, только столбцы и целые числа