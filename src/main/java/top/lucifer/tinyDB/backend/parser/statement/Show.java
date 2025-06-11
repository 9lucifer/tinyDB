package top.lucifer.tinyDB.backend.parser.statement;

import lombok.Data;

import java.time.LocalDateTime;
@Data
public class Show {
    public String tableName;

    public String[] values;
    public Show(){
        System.out.println("====" + "show" + "====");
        LocalDateTime time = LocalDateTime.now();
        values = new String[1];
        values[0] = time.toString();
    }
}
