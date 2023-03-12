package com.example.task;

import java.math.BigDecimal;
import java.util.List;

public record Tree(Long id, List<Integer> criterias, String operator, List<Node> nodes, Loss loss) {

}

record Node2(Long id, List<Criteria> criteriaList, String operator, List<Node> nodes) {

}

record LeafNode(Long id, BigDecimal loss) {

}