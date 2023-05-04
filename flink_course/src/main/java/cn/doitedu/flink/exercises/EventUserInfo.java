package cn.doitedu.flink.exercises;

/**
 * @author zengwang
 * @create 2023-05-03 21:50
 * @desc:
 */
import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventUserInfo {

    private int id;
    private String eventId;
    private int cnt;
    private String gender;
    private String city;

}