package com.example.task;

import java.util.ArrayList;

public record CalculatedParameter(String name, String type, ArrayList<String> fieldList, ArrayList<String> listOfOperators) {
}
