package com.example.task.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record NodeTwo(Long id, List<Long> criterias, String operator,
                      List<Node> nodes, String loss) {
}
//