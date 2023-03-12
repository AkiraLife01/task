package com.example.task;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

public class ValueDataExtractor {

    private final HashMap<String, VariableData> valuesMap;
    private final int size;
    public ValueDataExtractor(HashMap<String, VariableData> valuesMap, int size) {
        this.valuesMap = valuesMap;
        this.size = size;
    }

    public HashMap<String, VariableData> getValuesMap() {
        return valuesMap;
    }

    public VariableData extract(String valueName) {
        if (valueName.charAt(0) == '{' && valueName.charAt(valueName.length() - 1) == '}') {
            return valuesMap.get(getReference(valueName));
        }
        return generateData(valueName);
    }

    private VariableData generateData(String valueName) {
        BigDecimal bigDecimal = parseBigDecimal(valueName);

        if (bigDecimal != null) {
            List<Object> decimalList = IntStream.rangeClosed(1, Math.toIntExact(size))
                    .mapToObj(i -> (Object) bigDecimal).toList();

            return new VariableData("BigDecimal", decimalList);
        }

        throw new RuntimeException("Unsupported value type in criteria");
    }

    private BigDecimal parseBigDecimal(String line) {
        try {
            return new BigDecimal(line);
        } catch (RuntimeException runtimeException) {
            return null;
        }
    }
    public static String getReference(String line) {
        line = line.trim();
        if (line.charAt(0) == '{' && line.charAt(line.length() - 1) == '}') {
            line = line.substring(1, line.length() - 1);
        }
        return line;
    }
}