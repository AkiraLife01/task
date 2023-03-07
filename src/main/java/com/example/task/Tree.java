package com.example.task;

import java.util.List;

public record Tree(Long id, List<Integer> criterias, String operator, List<Node> nodes, Loss loss) {

}
