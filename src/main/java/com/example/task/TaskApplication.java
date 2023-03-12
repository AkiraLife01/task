package com.example.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
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

                assert variables.size() > 0;


                var listOfParameters = new ArrayList<Parameter2>();

                // assume there exists no other parameter references in a parameter value
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


                var parameterMap = new HashMap<String, VariableData>();
                final int setSize = variablesMap.get(variables.get(0).name()).values().size();

                BigDecimal parameterValue1;
                BigDecimal parameterValue2;
                


                // assume there are only one operator per parameter value
                for (Parameter2 parameter2 : listOfParameters) {
                    var type = parameter2.type();
                    List<Object> objects = new ArrayList<>();
                    
                    for (int i = 0; i < setSize; i++) {
                        a = i;
                        
                        Object resultValue = null;
                        
                        switch (parameter2.listOfOperators().get(0)) {
                            case "+" -> {
                                //TODO добавить переключатель для типов сравнения Date и BigDecimal и TimeStamp
                                parameterValue1 = (BigDecimal) variablesMap.get(parameter2.listOfFields().get(0)).values().get(i);
                                parameterValue2 = (BigDecimal) variablesMap.get(parameter2.listOfFields().get(1)).values().get(i);
                                resultValue = parameterValue1.add(parameterValue2);
                                
                            }
                            case "-" -> {
                                parameterValue1 = (BigDecimal) variablesMap.get(parameter2.listOfFields().get(0)).values().get(i);
                                parameterValue2 = (BigDecimal) variablesMap.get(parameter2.listOfFields().get(1)).values().get(i);
                                resultValue = parameterValue1.subtract(parameterValue2);
                            }
                            case "/" -> {
                                parameterValue1 = (BigDecimal) variablesMap.get(parameter2.listOfFields().get(0)).values().get(i);
                                parameterValue2 = (BigDecimal) variablesMap.get(parameter2.listOfFields().get(1)).values().get(i);
                                try {
                                    resultValue = parameterValue1.divide(parameterValue2, 3, RoundingMode.DOWN);

                                } catch (ArithmeticException arithmeticException) {
                                    System.out.println("Деление на 0");
                                }

                            }
                            case "*" -> {
                                parameterValue1 = (BigDecimal) variablesMap.get(parameter2.name()).values().get(i);
                                parameterValue2 = (BigDecimal) variablesMap.get(parameter2.name()).values().get(i);
                                resultValue = parameterValue1.multiply(parameterValue2);
                            }

                        }
                        objects.add(resultValue);

                    }
                    parameterMap.put(parameter2.name(), new VariableData(type, objects));
                }


                var criterias = new HashMap<Long, List<Boolean>>();

                var valueDataExtractor = new ValueDataExtractor(variablesMap, setSize);


                for (Criteria criteria : rule.criterias()) {

                    var parameterName = criteria.parameter();
                    List<Object> parameters;
                    var type = "";

                    if (variablesMap.containsKey(parameterName)) {
                        parameters = variablesMap.get(parameterName).values();
                        type = variablesMap.get(parameterName).type();

                    } else {
                        parameters = parameterMap.get(parameterName).values();
                        type = parameterMap.get(parameterName).type();
                    }

                    List<Object> values = new ArrayList<>(setSize);

                    if (isConstValue(criteria)) {

                        Object value = convertToType(type, criteria.value());
                        for (int i = 0; i < setSize; i++) {
                            values.add(value);
                        }
                    }

                    if (isReferenceValue(criteria)) {
                        String referenceValue = ValueDataExtractor.getReference(criteria.value());

                        if (variablesMap.containsKey(referenceValue)) {
                            values = variablesMap.get(referenceValue).values();

                        } else {
                            values = parameterMap.get(referenceValue).values();
                        }
                    }


                    var operator = criteria.operator();

                    var criteriaResults = new ArrayList<Boolean>();

                    for (int i = 0; i < setSize; i++) {

                        var parameter  =  parameters.get(i);
                        var value =  values.get(i);

                        if (parameter instanceof BigDecimal parameterBigDec && value instanceof BigDecimal valueBigDec) {

                            switch (operator) {
                                case "lt" -> {
                                    boolean b = parameterBigDec.compareTo(valueBigDec) < 0;
                                    criteriaResults.add(b);
                                }
                                case "gt" -> {
                                    boolean b = parameterBigDec.compareTo(valueBigDec) > 0;
                                    criteriaResults.add(b);
                                }
                                case "eq" -> {
                                    boolean b = parameterBigDec.compareTo(valueBigDec) == 0;
                                    criteriaResults.add(b);
                                }
                            }
                        } else if (parameter instanceof Date parameterDate && value instanceof Date valueDate) {
                            switch (operator) {
                                case "lt" -> {
                                    boolean b = parameterDate.compareTo(valueDate) < 0;
                                    criteriaResults.add(b);
                                }
                                case "gt" -> {
                                    boolean b = parameterDate.compareTo(valueDate) > 0;
                                    criteriaResults.add(b);
                                }
                                case "eq" -> {
                                    boolean b = parameterDate.compareTo(valueDate) == 0;
                                    criteriaResults.add(b);
                                }
                            }
                        }
                        // TODO надо сделать операторы для остальных типов данных (как минимум equal)

                    }
                    criterias.put(criteria.id(), criteriaResults);
                }

                // TODO получить корневой Node
                // TODO получить нужные критерии на вычисления
                // TODO сравнить их по указанному оператору и сохранить результаты в белев список список
                // TODO пройтись по списку и в качестве ключа вытащить id к нужному Node (loss 1000.0 or 0.0)
                // TODO вытащить из мапы те у кого значение loss 1000

                System.out.println(criterias);
            } catch (SQLException ex) {
                System.out.println("Connection failed...");

            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.out.println(a);
        }


    }

    private static Object convertToType(String type, String value) throws Exception {
        Object result = null;

        switch (type) {
            case "BigDecimal" -> {
                result = new BigDecimal(value);
            }
            case "Date" -> {
                result = new SimpleDateFormat("yyyy-MM-dd").parse(value);
            }
            case "Timestamp" -> {
                // I am not sure which format does the timestamp has
                throw new Exception();
            }
            case "Boolean" -> {
                result = value.equalsIgnoreCase("true");
            }
            case "String" -> {
                result = value;
            }
        }
        
        return result;
    }

    private static boolean isReferenceValue(Criteria criteria) {
        String trim = criteria.value().trim();
        return trim.charAt(0) == '{';
    }

    private static boolean isConstValue(Criteria criteria) {
        String trim = criteria.value().trim();
        return trim.charAt(0) != '{';
    }


    public static Connection connect() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }
}

