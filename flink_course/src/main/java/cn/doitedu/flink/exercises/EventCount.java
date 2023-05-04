package cn.doitedu.flink.exercises;

/**
 * @author zengwang
 * @create 2023-05-03 21:50
 * @desc:
 */
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventCount {
    private int id;
    private String eventId;
    private int cnt;

}