package com.example.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.text.SimpleDateFormat;
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

        Rule rule = obtainRuleFromJson();

        String query = createQueryForDB(rule);

        try {

            try (Connection conn = connect()) {

                Statement statement = conn.createStatement();
                ResultSet resultSet = statement.executeQuery(query);
                List<Variable> variables = rule.variables();

                HashMap<String, VariableData> variablesMap = fillMapValues(resultSet, variables);

                assert variables.size() > 0;

                List<Parameter2> parameterList = obtainParameters(rule.parameters());

                final int amountLineDB = variablesMap.get(variables.get(0).name()).values().size();

                HashMap<String, VariableData> parameterMap = obtainParameterValue(variablesMap, parameterList, amountLineDB);


                HashMap<Long, List<Boolean>> criterias = computeCriterias(variablesMap, rule.criterias(), parameterMap, amountLineDB);



                List<List<Boolean>> listSelectedCriterias = new ArrayList<>();

                if (rule.tree().get(0).criterias() != null && !rule.tree().get(0).criterias().isEmpty()) {
                    for (Long criteria : rule.tree().get(0).criterias()) {
                        List<Boolean> currentCriteria = criterias.get(criteria);
                        listSelectedCriterias.add(currentCriteria);
                    }
                }

                Boolean[] countedResult = new Boolean[amountLineDB];

                if (rule.tree().get(0).operator().equals("AND")) {

                    for (int i = 0; i < listSelectedCriterias.size(); i++) {
                        for (int j = 0; j < amountLineDB; j++) {
                            Boolean aBoolean = listSelectedCriterias.get(i).get(j);

                            if (i == 0) {
                                countedResult[j] = aBoolean;
                            } else {
                                countedResult[j] &= aBoolean;
                            }
                        }
                    }

                } else if (rule.tree().get(0).operator().equals("OR")) {

                    for (int i = 0; i < listSelectedCriterias.size(); i++) {
                        for (int j = 0; j < amountLineDB; j++) {
                            Boolean aBoolean = listSelectedCriterias.get(i).get(j);

                            if (i == 0) {
                                countedResult[j] = aBoolean;
                            } else {
                                countedResult[j] |= aBoolean;
                            }
                        }
                    }

                }

                List<String> fields = new ArrayList<>();
                for (Join join : rule.joins()) {
                    String field = join.entity_left();
                    fields.add(field);
                }

                List<HashMap<String, Object>> resultMaps = new ArrayList<>();

                for (int i = 0; i < amountLineDB; i++) {
                    if (countedResult[i]) {
                        HashMap<String, Object> map = new HashMap<>();

                        for (String field : fields) {
                            map.put(field, variablesMap.get(field).values().get(i));
                        }
                        resultMaps.add(map);
                    }
                }


                ObjectMapper objectMapper = new ObjectMapper();
                String resultJson = objectMapper.writeValueAsString(resultMaps);
                System.out.println(resultJson);


                String directory = "C:\\Users\\Akira_Life\\IdeaProjects\\task\\src\\main\\resources\\";
                String file = "response.json";
                Path path = Paths.get(directory + file);

                Files.write(path, resultJson.getBytes());


                System.out.println(countedResult.length);

            } catch (SQLException ex) {
                System.out.println("Connection failed...");

            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }


    }

    private static HashMap<Long, List<Boolean>> computeCriterias(HashMap<String, VariableData> variablesMap, List<Criteria> criterias, HashMap<String, VariableData> parameterMap, int amountLineDB) throws Exception {
        var criteriasTemp = new HashMap<Long, List<Boolean>>();

        for (Criteria criteria : criterias) {

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

            List<Object> values = new ArrayList<>(amountLineDB);

            if (isConstValue(criteria)) {

                Object value = convertToType(type, criteria.value());
                for (int i = 0; i < amountLineDB; i++) {
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

            for (int i = 0; i < amountLineDB; i++) {

                var parameter = parameters.get(i);
                var value = values.get(i);

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

            }
            criteriasTemp.put(criteria.id(), criteriaResults);

        }
        return criteriasTemp;
    }

    private static HashMap<String, VariableData> obtainParameterValue(HashMap<String, VariableData> variablesMap, List<Parameter2> parameterList, int amountLineDB) {
        BigDecimal parameterValue1;
        BigDecimal parameterValue2;
        HashMap<String, VariableData> map = new HashMap<>();

        // assume there are only one operator per parameter value
        for (Parameter2 parameter2 : parameterList) {
            var type = parameter2.type();
            List<Object> objects = new ArrayList<>();

            for (int i = 0; i < amountLineDB; i++) {

                Object resultValue = null;

                switch (parameter2.listOfOperators().get(0)) {
                    case "+" -> {
                        parameterValue1 = (BigDecimal) variablesMap.get(parameter2.fieldList().get(0)).values().get(i);
                        parameterValue2 = (BigDecimal) variablesMap.get(parameter2.fieldList().get(1)).values().get(i);
                        resultValue = parameterValue1.add(parameterValue2);

                    }
                    case "-" -> {
                        parameterValue1 = (BigDecimal) variablesMap.get(parameter2.fieldList().get(0)).values().get(i);
                        parameterValue2 = (BigDecimal) variablesMap.get(parameter2.fieldList().get(1)).values().get(i);
                        resultValue = parameterValue1.subtract(parameterValue2);
                    }
                    case "/" -> {
                        parameterValue1 = (BigDecimal) variablesMap.get(parameter2.fieldList().get(0)).values().get(i);
                        parameterValue2 = (BigDecimal) variablesMap.get(parameter2.fieldList().get(1)).values().get(i);
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
            map.put(parameter2.name(), new VariableData(type, objects));
        }
        return map;
    }

    private static List<Parameter2> obtainParameters(List<Parameter> parameters) {
        var parameterList = new ArrayList<Parameter2>();
        // assume there exists no other parameter references in a parameter value
        for (Parameter parameter : parameters) {

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
            parameterList.add(new Parameter2(parameter.name(), parameter.type(),
                    listOfField, listOfOperator));
        }
        return parameterList;
    }

    private static HashMap<String, VariableData> fillMapValues(ResultSet resultSet, List<Variable> variables) throws SQLException {
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
        return variablesMap;
    }

    private static Rule obtainRuleFromJson() throws IOException {
        File f = new File("C:\\Users\\Akira_Life\\IdeaProjects\\task\\src\\main\\resources\\test_source\\rule.json");
        byte[] bytes = Files.readAllBytes(f.toPath());
        String json = new String(bytes);
        ObjectMapper objectMapper = new ObjectMapper();

        return objectMapper.readValue(json, Rule.class);
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

    private static String createQueryForDB(Rule rule) {
        StringBuilder query = new StringBuilder("SELECT * FROM ");
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
        return query.toString();
    }
}

