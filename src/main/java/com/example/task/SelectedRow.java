package com.example.task;

import java.math.BigDecimal;
import java.sql.Date;

public record SelectedRow(BigDecimal fid, BigDecimal year, BigDecimal quarter,
                          BigDecimal s40, Date dateReceipt, Date dateCreation,
                          BigDecimal s120_3) {

}
