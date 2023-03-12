package com.example.task.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Rule(List<Join> joins, List<Variable> variables, List<Parameter> parameters,
                   List<Criteria> criterias, List<NodeTwo> tree) {



}
